"""
知识库混合检索引擎 (Hybrid Memory Retrieval Engine)
--------------------------------------------------
双轨道检索：
1. 向量轨道 (ChromaDB): 负责语义、情感和长文本片段的模糊匹配。
2. 图谱轨道 (Neo4j): 顺着实体关系链 (Relation) 摸索隐藏事实，支持 3 跳深度推理。

评分系统：基于 HybridScorer 实现时间衰减、影响分、实体权重、语义相似度的四维加权。
"""
import logging
import json
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone

from neo4j import AsyncGraphDatabase
import chromadb

try:
    from rank_bm25 import BM25Okapi
except ImportError:
    BM25Okapi = None

from config import neo4j_config, embed_config, memory_config, identity_config, postgres_config
from .scoring import HybridScorer, ScoredCandidate, UnifiedMemoryResult

logger = logging.getLogger(__name__)

class KnowledgeBaseEngine:
    """混合动力记忆引擎"""
    
    def __init__(self, top_k: int = 5, window_cutoff: int = 6):
        # 初始化图谱驱动
        self._driver = AsyncGraphDatabase.driver(
            uri=neo4j_config.uri,
            auth=(neo4j_config.username, neo4j_config.password)
        )
        self.database = neo4j_config.database or "neo4j"
        
        # 初始化向量数据库客户端
        from .vector.storer import VectorStorer
        self.v_storer = VectorStorer()
        self.chat_collection = self.v_storer.chat_collection
        self.knowledge_collection = self.v_storer.doc_collection
        
        self.top_k = top_k
        self.window_cutoff = window_cutoff
        self.scorer = HybridScorer()
        
        # 性能诊断埋点
        self.last_latency = {"vector": 0, "graph": 0, "bm25": 0, "structured_events": 0, "plans": 0, "total": 0}

    async def close(self):
        await self._driver.close()

    @property
    def is_bm25_enabled(self) -> bool:
        """检查 BM25 开关状态（默认开启；缺少外部库时会走内置 fallback）。"""
        from config import memory_config
        return bool(getattr(memory_config, "enable_bm25", True))

    async def fetch_identities(self, session):
        """Warm-up fetch for canonical user/assistant names from graph."""
        try:
            async with self._driver.session(database=self.database) as db_session:
                res = await db_session.run(
                    "MATCH (e:Entity {owner_id: $uid}) WHERE e.entity_id IN ['assistant_001', 'user_001'] RETURN e.entity_id AS id, COALESCE(e.primary_name, e.name) AS name, COALESCE(e.aliases, []) AS aliases",
                    uid=identity_config.user_id
                )
                data = await res.data()
                for item in data:
                    if item['id'] == 'assistant_001' and item['name']:
                        session.context_canvas["assistant_real_name"] = item['name']
                        session.context_canvas["assistant_aliases"] = list(item.get("aliases") or [])
                    if item['id'] == 'user_001' and item['name']:
                        session.context_canvas["user_real_name"] = item['name']
                        session.context_canvas["user_aliases"] = list(item.get("aliases") or [])
        except Exception as exc:
            logger.warning("[KnowledgeBaseEngine] fetch_identities failed: %s", exc)

    async def query(self, query: str, user_id: Optional[str] = None, session_id: Optional[str] = None) -> List[UnifiedMemoryResult]:
        """
        全量检索接口：召回向量记忆 + 推理图谱关联 (带耗时统计)
        """
        import time
        start_total = time.perf_counter()
        
        # 1. 向量召回 (RAG)
        start_v = time.perf_counter()
        vector_candidates = await self._retrieve_vector_context(query, user_id)
        self.last_latency["vector"] = int((time.perf_counter() - start_v) * 1000)
        
        # 2. 图谱召回 (推理) - 自动识别语义时间窗口
        start_g = time.perf_counter()
        
        tw_start, tw_end, source = self._infer_time_window(query)
        self.last_latency["time_window"] = {
            "enabled": tw_start is not None,
            "start": tw_start,
            "end": tw_end,
            "source": source
        }
        
        graph_candidates = await self._retrieve_graph_events(query, user_id, tw_start, tw_end)
        self.last_latency["graph"] = int((time.perf_counter() - start_g) * 1000)
        
        # 3. BM25 文本关键词召回 (支持开关控制)
        start_b = time.perf_counter()
        bm25_candidates = []
        if getattr(memory_config, "enable_bm25", True):
            bm25_candidates = await self._retrieve_bm25_context(query, user_id)
        self.last_latency["bm25"] = int((time.perf_counter() - start_b) * 1000)

        # 4. SQL CRM 结构化数据召回 (仅当配置了 PostgreSQL 且为 CRM 类查询时触发)
        start_s = time.perf_counter()
        sql_candidates = []
        if postgres_config.is_configured() and postgres_config.tenant_id:
            sql_candidates = await self._retrieve_sql_crm_context(query)
        self.last_latency["sql"] = int((time.perf_counter() - start_s) * 1000)

        # 5. Episode 剧情召回 (M2.3)
        start_e = time.perf_counter()
        episode_candidates = await self._retrieve_episode_context(query, user_id)
        self.last_latency["episodes"] = int((time.perf_counter() - start_e) * 1000)

        # 6. Saga 主线召回 (M2.2)
        start_sa = time.perf_counter()
        saga_candidates = await self._retrieve_graph_sagas(query, user_id)
        self.last_latency["sagas"] = int((time.perf_counter() - start_sa) * 1000)

        # 7. 汇总并全局去重 (统一 deduplication key)
        import re
        start_struc = time.perf_counter()
        struc_candidates = await self._retrieve_structured_events(query, user_id=user_id, time_start=tw_start, time_end=tw_end)
        self.last_latency["structured_events"] = int((time.perf_counter() - start_struc) * 1000)

        start_plan = time.perf_counter()
        plan_candidates = await self._retrieve_plan_items(query, user_id=user_id, time_start=tw_start, time_end=tw_end)
        self.last_latency["plans"] = int((time.perf_counter() - start_plan) * 1000)

        raw_candidates = vector_candidates + graph_candidates + bm25_candidates + sql_candidates + episode_candidates + saga_candidates + struc_candidates + plan_candidates
        all_candidates = []
        seen_keys = set()
        for cand in raw_candidates:
            # 移除标点、空格、将其转为小写作为全局去重键
            v_key = re.sub(r'[^\w\u4e00-\u9fa5]', '', cand.content).lower()
            if not v_key or v_key not in seen_keys:
                if v_key:
                    seen_keys.add(v_key)
                all_candidates.append(cand)

        self.last_latency["total"] = int((time.perf_counter() - start_total) * 1000)
        
        if not all_candidates:
            return []
            
        # 注入图谱交叉验证分数 (Graph Hop Score)
        for cand in all_candidates:
            if not cand.graph_validated:
                cand.graph_hop_score = await self._graph_hop_score(cand.speaker, query)
        
        # 应用混合评分排序 (获取全量加权结果)
        scored_all = self.scorer.score(all_candidates, top_k=99)

        # Inject SQL evidence windows for prompt-adopted items only.
        top_prompt_candidates = [c for i, c in enumerate(scored_all) if i < self.top_k and c.source_msg_id]
        top_msg_ids = [int(c.source_msg_id) for c in top_prompt_candidates if c.source_msg_id]
        evidence_context_map = await self._build_evidence_context_map(top_msg_ids)
        for c in top_prompt_candidates:
            if c.source_msg_id:
                c.evidence_context = evidence_context_map.get(int(c.source_msg_id))

        return [
            UnifiedMemoryResult(
                content=c.content,
                final_score=c.final_score,
                speaker=c.speaker or "unknown",
                source_type=c.source_type or "RAG",
                timestamp=c.timestamp or "",
                graph_validated=c.graph_validated,
                source_name=c.source_name or "unknown",
                semantic_score=c.semantic_score,
                time_decay_score=c.time_decay_score,
                graph_hop_score=c.graph_hop_score,
                impact_score=c.impact_score,
                source_msg_id=c.source_msg_id,
                evidence_context=getattr(c, "evidence_context", None),
                in_prompt=(i < self.top_k),
            )
            for i, c in enumerate(scored_all)
        ]

    async def _graph_hop_score(self, entity_name: str, query_hint: str) -> float:
        """Estimate graph relevance score by checking entity presence."""
        if entity_name in ("[文档]", "AI", "unknown", "", "用户"):
            return 0.5
        try:
            async with self._driver.session(database=self.database) as session:
                result = await session.run(
                    "MATCH (e:Entity {owner_id: $uid}) WHERE e.name = $name OR e.name CONTAINS $name "
                    "RETURN count(e) > 0 AS exists",
                    name=entity_name, uid=identity_config.user_id
                )
                record = await result.single()
                return 1.0 if record and record["exists"] else 0.0
        except Exception as exc:
            logger.debug("[KnowledgeBaseEngine] graph_hop_score failed for '%s': %s", entity_name, exc)
            return 0.0

    def _infer_time_window(self, query: str) -> tuple[Optional[str], Optional[str], str]:
        """Infer a coarse time window from natural-language query text."""
        now = datetime.now(timezone.utc)
        q = query.lower()
        
        if "今天" in q or "今日" in q or "刚刚" in q:
            day = now.date().isoformat()
            return f"{day}T00:00:00Z", f"{day}T23:59:59Z", "nlp_inferred"
        
        if "昨天" in q or "昨日" in q:
            day = (now - timedelta(days=1)).date().isoformat()
            return f"{day}T00:00:00Z", f"{day}T23:59:59Z", "nlp_inferred"
            
        if "前天" in q:
            day = (now - timedelta(days=2)).date().isoformat()
            return f"{day}T00:00:00Z", f"{day}T23:59:59Z", "nlp_inferred"
            
        if "明天" in q:
            day = (now + timedelta(days=1)).date().isoformat()
            return f"{day}T00:00:00Z", f"{day}T23:59:59Z", "nlp_inferred"
            
        if "本周" in q:
            start = (now - timedelta(days=now.weekday())).date().isoformat()
            end = (now + timedelta(days=6-now.weekday())).date().isoformat()
            return f"{start}T00:00:00Z", f"{end}T23:59:59Z", "nlp_inferred"

        if "上周" in q:
            start = (now - timedelta(days=now.weekday()+7)).date().isoformat()
            end = (now - timedelta(days=now.weekday()+1)).date().isoformat()
            return f"{start}T00:00:00Z", f"{end}T23:59:59Z", "nlp_inferred"

        return None, None, "none"

    async def _retrieve_graph_events(self, query: str, user_id: str, time_window_start: str = None, time_window_end: str = None) -> List[ScoredCandidate]:
        """
        增强型图谱检索：支持 3 跳内关联实体的推理
        """
        candidates = []
        try:
            async with self._driver.session(database=self.database) as session:
                # 寻找 query 中提到的关键词，并扩展 3 跳检索事件
                result = await session.run(
                    """
                    MATCH (root:Entity {owner_id: $uid})
                    WHERE $q_text CONTAINS root.name
                    MATCH (root)-[:RELATION*0..3]-(sub:Entity)-[:ACTOR_IN]->(evt:Event)
                    WHERE evt.owner_id = $uid AND evt.status = 'active'
                      AND (
                        ($start IS NULL OR $end IS NULL) OR
                        (evt.event_time IS NOT NULL AND evt.event_time >= $start AND evt.event_time <= $end) OR
                        (evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start) AND (evt.record_time <= $end OR evt.created_at <= $end))
                      )
                    RETURN 
                        sub.name AS subject,
                        evt.predicate AS predicate,
                        evt.context AS context,
                        evt.timestamp_reference AS time_ref,
                        evt.event_time AS event_time,
                        evt.record_time AS record_time,
                        evt.created_at AS created_at,
                        COALESCE(evt.impact_score, 8) AS impact_score,
                        COALESCE(evt.confidence, 1.0) AS confidence,
                        evt.uuid AS event_uuid
                    ORDER BY evt.impact_score DESC, evt.created_at DESC
                    LIMIT $limit
                    """,
                    q_text=query, limit=self.top_k, uid=user_id,
                    start=time_window_start, end=time_window_end
                )
                records = await result.data()
                
                # 兜底：如果关键词没有命中，取全局高影响事件
                if not records:
                    result = await session.run(
                        """
                        MATCH (sub:Entity {owner_id: $uid})-[:ACTOR_IN]->(evt:Event) 
                        WHERE evt.owner_id = $uid AND evt.status = 'active'
                          AND (
                            ($start IS NULL OR $end IS NULL) OR
                            (evt.event_time IS NOT NULL AND evt.event_time >= $start AND evt.event_time <= $end) OR
                            (evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start) AND (evt.record_time <= $end OR evt.created_at <= $end))
                          )
                        RETURN sub.name AS subject, evt.predicate AS predicate, evt.context AS context,
                                evt.timestamp_reference AS time_ref, evt.created_at AS created_at,
                                evt.event_time AS event_time, evt.record_time AS record_time,
                                COALESCE(evt.impact_score, 5) AS impact_score,
                                COALESCE(evt.confidence, 1.0) AS confidence,
                                evt.uuid AS event_uuid
                        ORDER BY evt.impact_score DESC LIMIT $limit
                        """,
                        limit=self.top_k, uid=user_id,
                        start=time_window_start, end=time_window_end
                    )
                    records = await result.data()

                # --- 核心：关系自动联想 (让 AI 知道傻蛋是谁) ---
                async with self._driver.session(database=self.database) as rel_session:
                    rel_result = await rel_session.run(
                        """
                        MATCH (root:Entity {owner_id: $uid})
                        WHERE $q_text CONTAINS root.name
                        MATCH (root)-[r:RELATION]-(related:Entity)
                        WHERE r.owner_id = $uid
                        RETURN root.name AS source, r.type AS rel, related.name AS target, 
                                COALESCE(properties(r)['inferred'], false) AS inferred, 
                                properties(r)['inference_rule'] AS rule
                        LIMIT 10
                        """,
                        q_text=query, uid=user_id
                    )
                    rel_records = await rel_result.data()
                for rr in rel_records:
                    tag = "推演" if rr.get("inferred") else "发现"
                    candidates.append(ScoredCandidate(
                        content=f"关系网[{tag}]：{rr['source']} 的 {rr['rel']} 是 {rr['target']}" + (f" (规则: {rr['rule']})" if rr.get("rule") else ""),
                        speaker="图谱关系",
                        source_type="graph",
                        timestamp="",
                        graph_validated=True,
                        source_name="关系网",
                        semantic_score=1.0,
                        time_decay_score=1.0,
                        graph_hop_score=1.0,
                        impact_score=7.0
                    ))

                # --- 核心改进：语义去重过滤器 ---
                seen_fingerprints = set()
                deduplicated_records = []
                for r in records:
                    # 使用 Subject + Predicate (以及 Object 如果存在) 作为事实指纹
                    fingerprint = f"{r['subject']}_{r['predicate']}"
                    if fingerprint not in seen_fingerprints:
                        seen_fingerprints.add(fingerprint)
                        deduplicated_records.append(r)

                # --- [M1] 批量获取证据链 ID ---
                event_uuids = [r["event_uuid"] for r in deduplicated_records if r.get("event_uuid")]
                evidence_map = await self._fetch_source_message_ids(event_uuids)

                for r in deduplicated_records:
                    content = f"{r['subject']} {r['predicate']}"
                    if r.get("context") and r["context"] != "无":
                        content += f"（{r['context']}）"
                    
                    ts = r.get("created_at")
                    t_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else ""

                    source_msg_id = evidence_map.get(r.get("event_uuid"))

                    candidates.append(ScoredCandidate(
                        content=content,
                        speaker=r["subject"],
                        source_type="graph",
                        timestamp=t_str,
                        graph_validated=True,
                        source_name="知识图谱",
                        semantic_score=1.0,
                        time_decay_score=1.0,
                        graph_hop_score=1.0,
                        impact_score=float(r["impact_score"]),
                        confidence=float(r["confidence"]) if r.get("confidence") is not None else None,
                        source_msg_id=source_msg_id
                    ))
        except Exception as e:
            logger.error(f"Retriever Error: {e}")
        return candidates

    async def _retrieve_episode_context(self, query: str, user_id: str) -> List[ScoredCandidate]:
        """
        [M2.3] 检索与当前 query 语义相关的业务剧情 (Episode)
        """
        candidates = []
        try:
            async with self._driver.session(database=self.database) as session:
                # 增强：分词检索以提高召回率
                keywords = [k.strip() for k in query.split() if k.strip()]
                if not keywords:
                    keywords = [query]
                
                # 用户要求：双重召回（直连 + 关联事件）
                cypher = """
                MATCH (ep:Episode {owner_id: $uid})
                WHERE ALL(k IN $ks WHERE ep.name CONTAINS k OR ep.summary CONTAINS k)
                RETURN ep.episode_id as uuid, ep.name as name, ep.summary as summary,
                       ep.evidence_msg_ids as evidence_msg_ids,
                       ep.created_at as created, ep.impact_score as impact
                ORDER BY ep.created_at DESC
                LIMIT 3
                """
                result = await session.run(cypher, ks=keywords, uid=user_id)
                records = await result.data()

                # 如果没有直连命中，尝试关联事件召回
                if not records:
                    cypher_fallback = """
                    MATCH (ep:Episode {owner_id: $uid})-[:CONTAINS_EVENT]->(ev:Event {owner_id: $uid})
                    WHERE ALL(k IN $ks WHERE ev.predicate CONTAINS k OR ev.object CONTAINS k)
                    RETURN ep.episode_id as uuid, ep.name as name, ep.summary as summary,
                           ep.evidence_msg_ids as evidence_msg_ids,
                           ep.created_at as created, ep.impact_score as impact
                    LIMIT 2
                    """
                    result = await session.run(cypher_fallback, ks=keywords, uid=user_id)
                    records = await result.data()
                
                for r in records:
                    content = f"剧情摘要({r['name']})：{r['summary']}"
                    ts = r.get("created")
                    t_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else ""
                    
                    # 剧情证据链：用户要求优先取第一个
                    msg_id = None
                    if r.get("evidence_msg_ids"):
                        msg_ids = r["evidence_msg_ids"]
                        if isinstance(msg_ids, list) and len(msg_ids) > 0:
                            msg_id = msg_ids[0]
                        
                    candidates.append(ScoredCandidate(
                        content=content,
                        speaker="剧情引擎",
                        source_type="episode",
                        timestamp=t_str,
                        graph_validated=True,
                        source_name=f"Episode:{r['name']}",
                        semantic_score=1.0, 
                        time_decay_score=1.0,
                        graph_hop_score=1.0,
                        impact_score=float(r.get("impact") or 7.0),
                        source_msg_id=msg_id
                    ))
        except Exception as e:
            logger.error(f"[EpisodeRetrieval] Error: {e}")
        return candidates

    async def _retrieve_graph_sagas(self, query: str, user_id: str) -> List[ScoredCandidate]:
        """
        [M2.2] 检索与当前 query 相关的长程主线 (Saga)
        """
        candidates = []
        try:
            async with self._driver.session(database=self.database) as session:
                # 语义命中 Saga 标题或描述
                cypher = """
                MATCH (sg:Saga {owner_id: $uid})
                WHERE ALL(k IN $ks WHERE sg.title CONTAINS k OR sg.description CONTAINS k)
                RETURN sg.saga_id as id, sg.title as title, sg.description as desc, sg.last_active as last
                ORDER BY sg.last_active DESC
                LIMIT 1
                """
                keywords = [k.strip() for k in query.split() if k.strip()]
                if not keywords: return []
                
                result = await session.run(cypher, ks=keywords, uid=user_id)
                r = await result.single()
                if r:
                    content = f"长期主线({r['title']})：{r['desc']}"
                    candidates.append(ScoredCandidate(
                        content=content,
                        speaker="主线引擎",
                        source_type="saga",
                        timestamp=str(r['last']) if r['last'] else "",
                        graph_validated=True,
                        source_name=f"Saga:{r['title']}",
                        semantic_score=1.0,
                        time_decay_score=1.0, # 主线通常不衰减
                        graph_hop_score=1.0,
                        impact_score=9.0, # 主线优先级最高
                    ))
        except Exception as e:
            logger.error(f"[SagaRetrieval] Error: {e}")
        return candidates

    async def _retrieve_vector_context(self, query: str, user_id: Optional[str]) -> List[ScoredCandidate]:
        """Retrieve vector candidates from chat and knowledge collections."""
        try:
            candidates = []
            
            # 1. 检索对话记忆 (仅查找当前 user_id)
            chat_results = self.chat_collection.query(
                query_texts=[query],
                n_results=self.top_k,
                where={"user_id": user_id} if user_id else None
            )
            
            # 2. 检索全量知识库
            kb_results = self.knowledge_collection.query(
                query_texts=[query],
                n_results=self.top_k
            )
            
            # 合并结果
            payloads = [
                (chat_results, "对话记忆", 5.0),
                (kb_results, "外部知识", 8.0)
            ]
            
            for results, source_tag, base_impact in payloads:
                if not results or not results['documents'] or not results['documents'][0]:
                    continue
                
                for i in range(len(results['documents'][0])):
                    doc = results['documents'][0][i]
                    meta = results['metadatas'][0][i]
                    dist = results['distances'][0][i]
                    sim = max(0.0, 1.0 - dist)
                    
                    candidates.append(ScoredCandidate(
                        content=doc,
                        speaker=meta.get("speaker", source_tag),
                        source_type="vector",
                        timestamp=meta.get("timestamp", ""),
                        source_name=meta.get("source", source_tag),
                        semantic_score=sim,
                        time_decay_score=1.0, 
                        impact_score=base_impact,
                        confidence=float(meta["confidence"]) if meta.get("confidence") is not None else None,
                        source_msg_id=int(meta["source_msg_id"]) if "source_msg_id" in meta else None
                    ))
            
            return candidates
        except Exception as e:
            logger.error(f"Vector Retrieval Error: {e}")
            return []

    async def _retrieve_bm25_context(self, query: str, user_id: Optional[str]) -> List[ScoredCandidate]:
        """时序关键词召回：基于 BM25 的精确匹配通道 (P0)"""
        try:
            # 1. 取最近 100 条作为语料库
            results = self.chat_collection.get(
                where={"user_id": user_id} if user_id else None,
                limit=100
            )
            
            docs = results.get('documents', [])
            metas = results.get('metadatas', [])
            
            if not docs: return []
            
            # 2. 分词准备
            tokenized_corpus = [list(doc) for doc in docs]
            tokenized_query = list(query)

            if BM25Okapi is not None:
                bm25 = BM25Okapi(tokenized_corpus)
                doc_scores = bm25.get_scores(tokenized_query)
            else:
                # Fallback: 简易关键词重合评分（无第三方依赖）
                qset = set(tokenized_query)
                doc_scores = []
                for toks in tokenized_corpus:
                    if not toks:
                        doc_scores.append(0.0)
                        continue
                    overlap = len(qset.intersection(set(toks)))
                    score = overlap / max(1, len(qset))
                    doc_scores.append(score)
            
            # 3. 构造候选
            candidates = []
            max_score = max(doc_scores) if len(doc_scores) > 0 else 1.0
            
            for i, score in enumerate(doc_scores):
                if score <= 0: continue
                
                # 归一化语义分
                sim = score / max_score if max_score > 0 else 0.0
                if sim < 0.1: continue # 过滤噪声
                
                meta = metas[i]
                candidates.append(ScoredCandidate(
                    content=docs[i],
                    speaker=meta.get("speaker", "用户"),
                    source_type="bm25",
                    timestamp=meta.get("timestamp", ""),
                    source_name=meta.get("source", "BM25通道"),
                    semantic_score=sim,
                    time_decay_score=1.0,
                    impact_score=6.0, # BM25 命中的通常是硬事实
                    confidence=float(meta["confidence"]) if meta.get("confidence") is not None else None
                ))
            
            # 只取前 5 条送入融合
            candidates.sort(key=lambda x: x.semantic_score, reverse=True)
            return candidates[:5]
            
        except Exception as e:
            logger.error(f"BM25 Retrieval Error: {e}")
            return []

    async def _retrieve_sql_crm_context(self, query: str) -> List[ScoredCandidate]:
        """
        第四通道：拉取外部共享 PostgreSQL 中的结构化业务数据。
        仅在查询命中 CRM 语义关键词时执行，避免污染普通对话检索。
        """
        from .sql.retriever import is_crm_query, query_crm_context, format_crm_rows

        if not is_crm_query(query):
            return []

        try:
            rows = await query_crm_context(
                query=query,
                tenant_id=postgres_config.tenant_id,
                limit=8,
            )
            if not rows:
                return []

            text = format_crm_rows(rows)
            return [
                ScoredCandidate(
                    content=text,
                    speaker="CRM数据库",
                    source_type="sql",
                    timestamp="",
                    graph_validated=False,
                    source_name="Business CRM",
                    semantic_score=0.9,
                    time_decay_score=1.0,
                    graph_hop_score=0.0,
                    impact_score=9.0,  # 结构化事实，高优先级
                    confidence=1.0,
                )
            ]
        except Exception as exc:
            logger.warning("[SQL CRM] retrieval failed: %s", exc)
            return []


    async def _retrieve_structured_events(self, query: str, user_id: str = None, time_start: str = None, time_end: str = None) -> List[ScoredCandidate]:
        """SQL memory events retrieval track."""
        from memory.sql.event_repository import EventRepository
        from memory.event.slots import MainEventType
        from .scoring import ScoredCandidate
        q, candidates, repo = query.lower(), [], EventRepository()
        triggers = {
            MainEventType.FINANCE: ["花", "买", "钱", "工资", "支出", "账单", "spending", "cost", "money", "buy"],
            MainEventType.HEALTH: ["体重", "身高", "心率", "血压", "运动", "健康", "weight", "height", "health"],
        }
        target_types = [etype for etype, kws in triggers.items() if any(k in q for k in kws)]
        if not target_types: return []
        try:
            for etype in target_types:
                if any(k in q for k in ["多少", "总共", "合计", "total", "sum", "how much"]):
                    results = await repo.aggregate_events(owner_id=user_id, main_type=etype, time_start=time_start, time_end=time_end)
                    for res in results:
                        candidates.append(ScoredCandidate(
                            content=f"财务统计：总支出 {res['total_amount']} {res['currency']} (共 {res['count']} 笔交易)",
                            speaker="财务管家", source_type="structured", source_name="SQL:Aggregate", timestamp="",
                            semantic_score=1.0, impact_score=9.0))
                recent = await repo.list_events(owner_id=user_id, main_type=etype, time_start=time_start, time_end=time_end, limit=5)
                for ev in recent:
                    detail = f"[{ev['main_type']}] {ev['subject']} {ev['predicate']} {ev['object'] or ''}"
                    val = f" ({ev['amount']} {ev['currency'] or 'CNY'})" if ev['amount'] else f" ({ev['quantity']} {ev['quantity_unit'] or ''})" if ev['quantity'] else ""
                    candidates.append(ScoredCandidate(
                        content=detail + val, speaker="结构化记忆", source_type="structured",
                        timestamp=str(ev.get('event_time') or ev.get('created_at') or ''), source_name="SQL:Events",
                        semantic_score=0.9, impact_score=8.5, source_msg_id=ev.get('source_msg_id')))
            return candidates
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"[KnowledgeEngine] Structured retrieval error: {e}")
            return []

    async def _retrieve_plan_items(self, query: str, user_id: str = None, time_start: str = None, time_end: str = None) -> List[ScoredCandidate]:
        """Retrieve actionable plan/todo/task items for [PLAN] prompt injection."""
        from memory.sql.event_repository import EventRepository
        from memory.event.slots import MainEventType
        from .scoring import ScoredCandidate

        q = (query or "").lower()
        plan_triggers = [
            "todo", "to-do", "task", "tasks", "plan", "plans", "remind", "reminder",
            "待办", "任务", "计划", "提醒", "安排", "日程", "截止", "deadline",
            "what should i do", "next step", "next steps"
        ]
        if not any(k in q for k in plan_triggers):
            return []
        if not user_id:
            return []

        repo = EventRepository()
        candidates: List[ScoredCandidate] = []
        seen_ids = set()
        main_types = [
            MainEventType.PLAN,
            MainEventType.TASK,
            MainEventType.SCHEDULE,
            MainEventType.GOAL,
        ]
        closed_status = {"done", "completed", "cancelled", "canceled", "resolved"}

        try:
            for mtype in main_types:
                rows = await repo.list_events(
                    owner_id=user_id,
                    main_type=mtype,
                    time_start=time_start,
                    time_end=time_end,
                    limit=20,
                )
                for ev in rows:
                    ev_id = str(ev.get("event_id") or "")
                    if ev_id and ev_id in seen_ids:
                        continue
                    if ev_id:
                        seen_ids.add(ev_id)

                    metadata = ev.get("metadata") or {}
                    if isinstance(metadata, str):
                        try:
                            metadata = json.loads(metadata)
                        except Exception:
                            metadata = {}

                    status = str(
                        metadata.get("status")
                        or metadata.get("task_status")
                        or metadata.get("state")
                        or ""
                    ).strip().lower()
                    if status in closed_status:
                        continue

                    subtype = str(ev.get("subtype") or "").strip()
                    subject = str(ev.get("subject") or "user").strip()
                    predicate = str(ev.get("predicate") or "").strip()
                    obj = str(ev.get("object") or "").strip()
                    due_time = (
                        metadata.get("due_time")
                        or metadata.get("deadline")
                        or metadata.get("due")
                        or ""
                    )
                    priority = metadata.get("priority") or ""

                    line = f"[{ev.get('main_type')}/{subtype or 'item'}] {subject} {predicate} {obj}".strip()
                    attrs = []
                    if status:
                        attrs.append(f"status={status}")
                    if due_time:
                        attrs.append(f"due={due_time}")
                    if priority:
                        attrs.append(f"priority={priority}")
                    if attrs:
                        line += " (" + ", ".join(attrs) + ")"

                    candidates.append(
                        ScoredCandidate(
                            content=line,
                            speaker="计划助手",
                            source_type="plan",
                            source_name="SQL:Plan",
                            timestamp=str(ev.get("event_time") or ev.get("created_at") or ""),
                            semantic_score=0.95,
                            impact_score=9.2,
                            source_msg_id=ev.get("source_msg_id"),
                        )
                    )
            return candidates[:20]
        except Exception as exc:
            logger.error("[KnowledgeEngine] Plan retrieval error: %s", exc)
            return []
    async def _fetch_source_message_ids(self, event_uuids: List[str]) -> Dict[str, int]:
        """Batch map Event UUIDs to SQL message ids."""
        if not event_uuids:
            return {}
        from memory.sql.pool import get_db
        try:
            async with get_db() as conn:
                is_sqlite = "sqlite" in str(type(conn)).lower()
                placeholders = ",".join(["?" if is_sqlite else f"${i+1}" for i in range(len(event_uuids))])
                query = f"SELECT event_uuid, message_id FROM ef_event_evidence_links WHERE event_uuid IN ({placeholders})"

                if is_sqlite:
                    cursor = await conn.execute(query, event_uuids)
                    rows = await cursor.fetchall()
                else:
                    rows = await conn.fetch(query, *event_uuids)

                out = {}
                for row in rows or []:
                    try:
                        if isinstance(row, dict):
                            out[row.get("event_uuid")] = row.get("message_id")
                        else:
                            out[row[0]] = row[1]
                    except Exception:
                        continue
                return out
        except Exception as e:
            logger.error(f"[KnowledgeEngine] Failed to fetch source_msg_ids: {e}")
            return {}

    async def _build_evidence_context_map(
        self,
        msg_ids: List[int],
        prev_rounds: int = 1,
        next_rounds: int = 0,
        max_chars_per_msg: int = 180,
        max_chars_total: int = 1800,
    ) -> Dict[int, str]:
        """Build SQL evidence windows by dialogue rounds (prev1 + current)."""
        if not msg_ids:
            return {}

        from memory.sql.pool import get_db
        target_ids = sorted({int(x) for x in msg_ids if x is not None})
        if not target_ids:
            return {}

        def _row_get(row, key, default=None):
            try:
                if isinstance(row, dict):
                    return row.get(key, default)
                return row[key]
            except Exception:
                return default

        def _clean_text(text: str) -> str:
            raw = str(text or "").replace("\r", " ").replace("\n", " ").strip()
            if len(raw) > max_chars_per_msg:
                return raw[: max_chars_per_msg - 3] + "..."
            return raw

        def _build_rounds(messages: List[dict]) -> tuple[List[List[dict]], Dict[int, int]]:
            rounds: List[List[dict]] = []
            msg_to_round: Dict[int, int] = {}
            for m in messages:
                mid = int(m["id"])
                role = str(m["role"] or "").lower()
                if role == "user" or not rounds:
                    rounds.append([m])
                else:
                    rounds[-1].append(m)
                msg_to_round[mid] = len(rounds) - 1
            return rounds, msg_to_round

        try:
            async with get_db() as conn:
                is_sqlite = "sqlite" in str(type(conn)).lower()
                placeholders = ",".join(["?" if is_sqlite else f"${i+1}" for i in range(len(target_ids))])
                target_query = f"SELECT id, session_id FROM ef_chat_messages WHERE id IN ({placeholders})"

                if is_sqlite:
                    cursor = await conn.execute(target_query, target_ids)
                    target_rows = await cursor.fetchall()
                else:
                    target_rows = await conn.fetch(target_query, *target_ids)

                session_targets: Dict[str, List[int]] = {}
                for row in target_rows or []:
                    mid = _row_get(row, "id")
                    sid = _row_get(row, "session_id")
                    if mid is None or not sid:
                        continue
                    session_targets.setdefault(str(sid), []).append(int(mid))

                context_map: Dict[int, str] = {}
                for sid, mids in session_targets.items():
                    if is_sqlite:
                        cursor = await conn.execute(
                            "SELECT id, role, content FROM ef_chat_messages WHERE session_id = ? ORDER BY id ASC",
                            (sid,),
                        )
                        rows = await cursor.fetchall()
                    else:
                        rows = await conn.fetch(
                            "SELECT id, role, content FROM ef_chat_messages WHERE session_id = $1 ORDER BY id ASC",
                            sid,
                        )

                    messages = [
                        {
                            "id": int(_row_get(r, "id")),
                            "role": str(_row_get(r, "role") or ""),
                            "content": _clean_text(_row_get(r, "content") or ""),
                        }
                        for r in (rows or [])
                        if _row_get(r, "id") is not None
                    ]
                    if not messages:
                        continue

                    rounds, msg_to_round = _build_rounds(messages)
                    for mid in mids:
                        if mid not in msg_to_round:
                            continue
                        center = msg_to_round[mid]
                        start = max(0, center - prev_rounds)
                        end = min(len(rounds) - 1, center + next_rounds)
                        selected = rounds[start : end + 1]
                        parts: List[str] = []
                        for ridx, rd in enumerate(selected, start=start + 1):
                            parts.append(f"ROUND-{ridx}")
                            for msg in rd:
                                role_tag = "U" if str(msg["role"]).lower() == "user" else "A"
                                parts.append(f"{role_tag}#{msg['id']}: {msg['content']}")
                        excerpt = "\n".join(parts).strip()
                        if len(excerpt) > max_chars_total:
                            excerpt = excerpt[: max_chars_total - 3] + "..."
                        context_map[mid] = excerpt

                return context_map
        except Exception as e:
            logger.error("[KnowledgeEngine] Failed to build evidence windows: %s", e)
            return {}

    def format_for_prompt(self, results: List[UnifiedMemoryResult]) -> str:
        """Format retrieved memory for model prompt."""
        if not results:
            return "(no related memory)"

        lines = []
        for i, res in enumerate(results):
            if res.source_type == "saga":
                tag = "[SAGA]"
            elif res.source_type == "episode":
                tag = "[EPISODE]"
            elif res.graph_validated:
                tag = "[GRAPH_CORE]"
            else:
                tag = "[MEMORY]"

            line = f"{i+1}. {tag} {res.content} (source: {res.source_name}"
            if res.timestamp:
                line += f", time: {res.timestamp}"
            line += ")"
            if getattr(res, "in_prompt", False) and getattr(res, "source_msg_id", None) and getattr(res, "evidence_context", None):
                line += f"\n   [SQL_EVIDENCE msg_id={res.source_msg_id}]\n{res.evidence_context}"
            lines.append(line)
        return "\n".join(lines)
