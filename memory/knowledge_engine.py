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
import calendar
import re
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone

from neo4j import AsyncGraphDatabase
import chromadb

try:
    from rank_bm25 import BM25Okapi
except ImportError:
    BM25Okapi = None

from config import neo4j_config, embed_config, memory_config, identity_config
from .scoring import HybridScorer, ScoredCandidate, UnifiedMemoryResult

logger = logging.getLogger(__name__)
_SHANGHAI_TZ = timezone(timedelta(hours=8))

def _utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

    @staticmethod
    def infer_query_intent(query: str) -> str:
        """Classify retrieval intent so evidence and narrative memories do not compete blindly."""
        import re

        q = str(query or "").strip().lower()
        if not q:
            return "semantic"

        long_term_markers = [
            "长期", "主线", "整体", "全局", "一直", "以来", "长期目标", "长期关系",
            "项目进展", "阶段", "脉络", "演变", "over time", "long-term", "roadmap",
        ]
        summary_markers = [
            "最近", "这几轮", "这段时间", "总结", "概括", "回顾", "我们聊了什么",
            "发生了什么", "进展如何", "summary", "recap", "what happened",
        ]
        fact_markers = [
            "多少", "哪里", "什么时候", "哪天", "谁", "是什么", "有没有", "是否",
            "上次说", "原文", "证据", "预算", "价格", "金额",
            "电话", "地址", "日期", "时间", "关系", "名字", "生日", "how much",
            "when", "where", "who", "what is", "evidence",
        ]
        numeric_fact_pattern = (
            r"\d+\s*(元|块|万|亿|%|岁|年|月|日|号|点|分钟|小时|天|个)"
            r"|\d+\s*(kg|g|cm|m|usd|rmb|cny|dollars?)\b"
        )

        has_fact_signal = bool(any(marker in q for marker in fact_markers) or re.search(numeric_fact_pattern, q))
        if has_fact_signal:
            return "fact"
        if any(marker in q for marker in long_term_markers):
            return "long_term"
        if any(marker in q for marker in summary_markers):
            return "summary"
        return "semantic"

    async def fetch_identities(self, session):
        """Warm-up fetch for canonical user/assistant names from graph."""
        try:
            uid = getattr(session, "user_id", None) or identity_config.user_id
            async with self._driver.session(database=self.database) as db_session:
                res = await db_session.run(
                    "MATCH (e:Entity {owner_id: $uid}) WHERE e.entity_id IN ['assistant_001', 'user_001'] RETURN e.entity_id AS id, COALESCE(e.primary_name, e.name) AS name, COALESCE(e.aliases, []) AS aliases",
                    uid=uid
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
        query_intent = self.infer_query_intent(query)
        self.last_latency["query_intent"] = query_intent
        
        # 1. 向量召回 (RAG)
        start_v = time.perf_counter()
        vector_candidates = await self._retrieve_vector_context(query, user_id)
        self.last_latency["vector"] = int((time.perf_counter() - start_v) * 1000)
        
        # 2. 图谱召回 (推理) - 自动识别语义时间窗口
        start_g = time.perf_counter()
        
        tw_start, tw_end, source = self._infer_time_window(query)
        narrative_day = self._infer_narrative_day(query)
        self.last_latency["time_window"] = {
            "enabled": tw_start is not None,
            "start": tw_start,
            "end": tw_end,
            "source": source,
            "narrative_day": narrative_day,
        }

        graph_candidates = await self._retrieve_graph_events(
            query, user_id, tw_start, tw_end, narrative_day=narrative_day
        )
        self.last_latency["graph"] = int((time.perf_counter() - start_g) * 1000)
        
        # 3. BM25 文本关键词召回 (支持开关控制)
        start_b = time.perf_counter()
        bm25_candidates = []
        if getattr(memory_config, "enable_bm25", True):
            bm25_candidates = await self._retrieve_bm25_context(query, user_id)
        self.last_latency["bm25"] = int((time.perf_counter() - start_b) * 1000)

        start_sql = time.perf_counter()
        sql_candidates = await self._retrieve_sql_keyword_context(query, user_id)
        self.last_latency["sql_keyword"] = int((time.perf_counter() - start_sql) * 1000)

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

        raw_candidates = (
            vector_candidates
            + graph_candidates
            + bm25_candidates
            + sql_candidates
            + episode_candidates
            + saga_candidates
            + struc_candidates
            + plan_candidates
        )
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
                cand.graph_hop_score = await self._graph_hop_score(cand.speaker, query, user_id)
        
        # 应用混合评分排序 (获取全量加权结果)
        scored_all = self.scorer.score(all_candidates, top_k=99, query_intent=query_intent)
        scored_all = self._promote_fact_evidence(scored_all, query_intent)

        # Inject SQL evidence windows for prompt-adopted items only.
        top_prompt_candidates = [c for i, c in enumerate(scored_all) if i < self.top_k and c.source_msg_id]
        top_msg_ids = [int(c.source_msg_id) for c in top_prompt_candidates if c.source_msg_id]
        evidence_context_map = await self._build_evidence_context_map(top_msg_ids, user_id=user_id)
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

    async def _graph_hop_score(self, entity_name: str, query_hint: str, user_id: Optional[str]) -> float:
        """Estimate graph relevance score by checking entity presence."""
        if entity_name in ("[文档]", "AI", "unknown", "", "用户"):
            return 0.5
        try:
            async with self._driver.session(database=self.database) as session:
                result = await session.run(
                    "MATCH (e:Entity {owner_id: $uid}) WHERE e.name = $name OR e.name CONTAINS $name "
                    "RETURN count(e) > 0 AS exists",
                    name=entity_name, uid=user_id or identity_config.user_id
                )
                record = await result.single()
                return 1.0 if record and record["exists"] else 0.0
        except Exception as exc:
            logger.debug("[KnowledgeBaseEngine] graph_hop_score failed for '%s': %s", entity_name, exc)
            return 0.0

    def _promote_fact_evidence(self, candidates: List[ScoredCandidate], query_intent: str) -> List[ScoredCandidate]:
        """Ensure fact questions carry at least one raw SQL evidence hit."""
        if query_intent != "fact" or not candidates:
            return candidates

        prompt_limit = max(1, self.top_k)
        head = list(candidates[:prompt_limit])
        tail = list(candidates[prompt_limit:])
        if any(c.source_type == "sql" and c.source_msg_id for c in head):
            return candidates

        evidence = next((c for c in tail if c.source_type == "sql" and c.source_msg_id), None)
        if not evidence:
            return candidates

        tail.remove(evidence)
        replace_idx = len(head) - 1
        for idx in range(len(head) - 1, -1, -1):
            if head[idx].source_type in {"episode", "saga", "vector"}:
                replace_idx = idx
                break

        displaced = head[replace_idx]
        head[replace_idx] = evidence
        return head + [displaced] + tail

    async def _graph_hop_score(self, entity_name: str, query_hint: str, user_id: Optional[str]) -> float:
        """Estimate graph relevance score by checking entity presence."""
        if entity_name in ("[文档]", "AI", "unknown", "", "用户"):
            return 0.5
        try:
            async with self._driver.session(database=self.database) as session:
                result = await session.run(
                    "MATCH (e:Entity {owner_id: $uid}) WHERE e.name = $name OR e.name CONTAINS $name "
                    "RETURN count(e) > 0 AS exists",
                    name=entity_name, uid=user_id or identity_config.user_id
                )
                record = await result.single()
                return 1.0 if record and record["exists"] else 0.0
        except Exception as exc:
            logger.debug("[KnowledgeBaseEngine] graph_hop_score failed for '%s': %s", entity_name, exc)
            return 0.0

    def _infer_narrative_day(self, query: str) -> Optional[int]:
        """Parse RP-style narrative day numbering (e.g. '第 61 天', 'day 23').

        Returns the integer day index when the query mentions one, else None.
        This is a separate axis from wall-clock time windows — narrative day
        only makes sense when events were tagged with metadata.narrative_day
        at extraction time.
        """
        if not query:
            return None
        import re
        # Chinese: 第61天 / 第 61 天 / 第 6 1 天 (allow whitespace)
        m = re.search(r"第\s*([0-9]{1,4})\s*天", query)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        # English: day 12 / day-12 / day#12 (word-boundary)
        m = re.search(r"\bday[\s\-#:]*([0-9]{1,4})\b", query, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        return None

    def _infer_time_window(self, query: str) -> tuple[Optional[str], Optional[str], str]:
        """Infer a coarse time window from natural-language query text."""
        now = datetime.now(timezone.utc).astimezone(_SHANGHAI_TZ)
        q = str(query or "").lower()

        def _day_window(day):
            start = datetime.combine(day, datetime.min.time(), tzinfo=_SHANGHAI_TZ)
            end = start + timedelta(days=1) - timedelta(seconds=1)
            return _utc_z(start), _utc_z(end), "nlp_inferred"

        def _range_window(start_day, end_day):
            start = datetime.combine(start_day, datetime.min.time(), tzinfo=_SHANGHAI_TZ)
            end = datetime.combine(end_day, datetime.min.time(), tzinfo=_SHANGHAI_TZ) + timedelta(days=1) - timedelta(seconds=1)
            return _utc_z(start), _utc_z(end), "nlp_inferred"

        if "今天" in q or "今日" in q or "刚刚" in q:
            return _day_window(now.date())
        
        if "昨天" in q or "昨日" in q:
            return _day_window((now - timedelta(days=1)).date())
            
        if "前天" in q:
            return _day_window((now - timedelta(days=2)).date())
            
        if "明天" in q:
            return _day_window((now + timedelta(days=1)).date())
            
        if "本周" in q:
            start = (now - timedelta(days=now.weekday())).date()
            end = (now + timedelta(days=6-now.weekday())).date()
            return _range_window(start, end)

        if "上周" in q:
            start = (now - timedelta(days=now.weekday()+7)).date()
            end = (now - timedelta(days=now.weekday()+1)).date()
            return _range_window(start, end)

        m = re.search(r"(?:过去|最近|前)\s*([0-9]{1,3})\s*天", q)
        if m:
            days = max(1, min(90, int(m.group(1))))
            return _range_window((now - timedelta(days=days)).date(), now.date())

        if any(marker in q for marker in ("前几天", "这几天", "最近几天", "过去几天")):
            return _range_window((now - timedelta(days=3)).date(), now.date())

        m = re.search(r"(?:(20[0-9]{2})\s*年\s*)?([0-9]{1,2})\s*月\s*([0-9]{1,2})\s*(?:日|号)?", q)
        if m:
            year = int(m.group(1) or now.year)
            month = int(m.group(2))
            day_num = int(m.group(3))
            try:
                target = datetime(year, month, day_num, tzinfo=timezone.utc).date()
            except ValueError:
                return None, None, "none"
            if any(marker in q for marker in ("那几天", "那几日", "前后几天", "附近几天")):
                last_day = calendar.monthrange(year, month)[1]
                start = target.replace(day=max(1, day_num - 2))
                end = target.replace(day=min(last_day, day_num + 2))
                return _range_window(start, end)
            return _day_window(target)

        m = re.search(r"(?:(20[0-9]{2})\s*年\s*)?([0-9]{1,2})\s*月\s*(上旬|中旬|下旬|月初|月底|月末)", q)
        if m:
            year = int(m.group(1) or now.year)
            month = int(m.group(2))
            try:
                last_day = calendar.monthrange(year, month)[1]
            except calendar.IllegalMonthError:
                return None, None, "none"
            phrase = m.group(3)
            if phrase in {"上旬", "月初"}:
                start_day, end_day = 1, 10
            elif phrase == "中旬":
                start_day, end_day = 11, 20
            else:
                start_day, end_day = 21, last_day
            start = datetime(year, month, start_day, tzinfo=timezone.utc).date()
            end = datetime(year, month, min(end_day, last_day), tzinfo=timezone.utc).date()
            return _range_window(start, end)

        return None, None, "none"

    async def _retrieve_graph_events(
        self,
        query: str,
        user_id: str,
        time_window_start: str = None,
        time_window_end: str = None,
        narrative_day: Optional[int] = None,
    ) -> List[ScoredCandidate]:
        """
        增强型图谱检索：支持 3 跳内关联实体的推理。

        Filter precedence:
          1) keyword pass — query text contains an Entity name (3-hop expand)
          2) narrative_day pass — RP-style 第 N 天 / day N filter
          3) time-window pass — only when wall-clock window provided
        We deliberately do NOT fall back to "global highest-impact events"
        when none of the above filter, because that path floods the prompt
        with semantically-unrelated origin/genesis nodes that mislead the
        LLM (the answer comes back confidently wrong with creation-day
        events injected as Day-N evidence).
        """
        candidates = []
        try:
            async with self._driver.session(database=self.database) as session:
                # 寻找 query 中提到的关键词，并扩展 3 跳检索事件
                result = await session.run(
                    """
                    MATCH (root:Entity {owner_id: $uid})
                    WHERE $q_text CONTAINS root.name
                    MATCH p=(root)-[:RELATION*0..3]-(sub:Entity {owner_id: $uid})-[:ACTOR_IN]->(evt:Event {owner_id: $uid})
                    WHERE evt.owner_id = $uid AND evt.status = 'active'
                      AND ALL(rel IN relationships(p) WHERE rel.owner_id = $uid)
                      AND (
                        ($start IS NULL OR $end IS NULL) OR
                        (evt.event_time IS NOT NULL AND evt.event_time >= $start AND evt.event_time <= $end) OR
                        (evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start) AND (evt.record_time <= $end OR evt.created_at <= $end))
                      )
                      AND ($narrative_day IS NULL OR evt.narrative_day = $narrative_day)
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
                    start=time_window_start, end=time_window_end,
                    narrative_day=narrative_day,
                )
                records = await result.data()

                # narrative_day fallback: pull all events for that exact day,
                # even when the query mentions no entity by name.
                if not records and narrative_day is not None:
                    result = await session.run(
                        """
                        MATCH (sub:Entity {owner_id: $uid})-[:ACTOR_IN]->(evt:Event)
                        WHERE evt.owner_id = $uid AND evt.status = 'active'
                          AND evt.narrative_day = $narrative_day
                        RETURN sub.name AS subject, evt.predicate AS predicate, evt.context AS context,
                                evt.timestamp_reference AS time_ref, evt.created_at AS created_at,
                                evt.event_time AS event_time, evt.record_time AS record_time,
                                COALESCE(evt.impact_score, 5) AS impact_score,
                                COALESCE(evt.confidence, 1.0) AS confidence,
                                evt.uuid AS event_uuid
                        ORDER BY evt.created_at ASC
                        LIMIT $limit
                        """,
                        limit=self.top_k, uid=user_id, narrative_day=narrative_day,
                    )
                    records = await result.data()

                # Wall-clock-window fallback: only run when an explicit time
                # window was provided. Without ANY filter, returning global
                # highest-impact events pollutes the prompt — better to be
                # silent than misleading.
                if not records and time_window_start and time_window_end:
                    result = await session.run(
                        """
                        MATCH (sub:Entity {owner_id: $uid})-[:ACTOR_IN]->(evt:Event)
                        WHERE evt.owner_id = $uid AND evt.status = 'active'
                          AND (
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
                        start=time_window_start, end=time_window_end,
                    )
                    records = await result.data()

                # --- 核心：关系自动联想 (让 AI 知道傻蛋是谁) ---
                async with self._driver.session(database=self.database) as rel_session:
                    rel_result = await rel_session.run(
                        """
                        MATCH (root:Entity {owner_id: $uid})
                        WHERE $q_text CONTAINS root.name
                        MATCH (root)-[r:RELATION]-(related:Entity {owner_id: $uid})
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
            chat_results = self.v_storer.query(
                collection_name="chat_memory",
                query_texts=[query],
                n_results=self.top_k,
                where={"user_id": user_id} if user_id else None
            )
            
            # 2. 检索全量知识库
            kb_results = self.v_storer.query(
                collection_name="knowledge_base",
                query_texts=[query],
                n_results=self.top_k,
                where={"user_id": user_id} if user_id else None
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

    def _extract_sql_keywords(self, query: str) -> List[str]:
        """Extract compact keywords for exact SQL dialogue fallback."""
        import re

        raw = str(query or "").strip()
        if not raw:
            return []

        keywords: List[str] = []
        for word in re.findall(r"[A-Za-z0-9_]{2,}", raw):
            keywords.append(word.lower())

        cjk = "".join(re.findall(r"[\u4e00-\u9fff]", raw))
        stop_chars = set("的是了有多少吗呢啊吧呀我你他她它们这那哪什么怎么如何请帮告诉一下一个以及和与或在把被给要想问第天时候")
        compact = "".join(ch for ch in cjk if ch not in stop_chars)

        for size in (4, 3, 2):
            for i in range(0, max(0, len(compact) - size + 1)):
                term = compact[i : i + size]
                if term and term not in keywords:
                    keywords.append(term)

        for ch in compact:
            if ch not in keywords and ch in {"厚", "宽", "高", "长", "米", "钱", "日", "天"}:
                keywords.append(ch)

        return keywords[:16]

    async def _retrieve_sql_keyword_context(self, query: str, user_id: Optional[str]) -> List[ScoredCandidate]:
        """Direct keyword fallback over raw SQL chat messages."""
        if not user_id:
            return []
        keywords = self._extract_sql_keywords(query)
        if not keywords:
            return []

        from memory.sql.pool import get_db

        def _row_get(row, key, default=None):
            try:
                if isinstance(row, dict):
                    return row.get(key, default)
                return row[key]
            except Exception:
                return default

        try:
            async with get_db() as conn:
                is_sqlite = "sqlite" in str(type(conn)).lower()
                clauses = []
                params = []
                for idx, kw in enumerate(keywords):
                    like = f"%{kw}%"
                    if is_sqlite:
                        clauses.append("LOWER(m.content) LIKE ?")
                    else:
                        clauses.append(f"LOWER(m.content) LIKE ${idx + 2}")
                    params.append(like.lower())

                if is_sqlite:
                    sql = (
                        "SELECT m.id, m.role, m.speaker, m.content, m.timestamp, m.session_id "
                        "FROM ef_chat_messages m "
                        "JOIN ef_chat_sessions s ON s.session_id = m.session_id "
                        f"WHERE s.user_id = ? AND ({' OR '.join(clauses)}) "
                        "ORDER BY m.id DESC LIMIT 200"
                    )
                    cursor = await conn.execute(sql, (user_id, *params))
                    rows = await cursor.fetchall()
                else:
                    sql = (
                        "SELECT m.id, m.role, m.speaker, m.content, m.timestamp, m.session_id "
                        "FROM ef_chat_messages m "
                        "JOIN ef_chat_sessions s ON s.session_id = m.session_id "
                        f"WHERE s.user_id = $1 AND ({' OR '.join(clauses)}) "
                        "ORDER BY m.id DESC LIMIT 200"
                    )
                    rows = await conn.fetch(sql, user_id, *params)

            import re

            def _norm_text(value: str) -> str:
                return re.sub(r"[^\w\u4e00-\u9fff]", "", str(value or "")).lower()

            query_norm = _norm_text(query)
            failure_markers = (
                "没有记录到",
                "未记录",
                "没有这件事",
                "不在记忆中",
                "no record",
                "not recorded",
            )
            numeric_or_day_pattern = (
                r"(\d+|[\u4e00-\u9fff]+)\s*"
                r"(\u7c73|\u4e08|\u5c3a|\u5929|\u65e5|\u5e74|\u6b65|\u91cc|\u516c\u91cc|cm|m|km)"
            )

            scored = []
            for row in rows or []:
                content = str(_row_get(row, "content") or "")
                content_l = content.lower()
                content_norm = _norm_text(content)
                role = str(_row_get(row, "role") or "")

                if any(marker in content_l for marker in failure_markers):
                    continue
                if query_norm and content_norm == query_norm:
                    continue
                if role.lower() == "user" and ("?" in content or "？" in content) and not re.search(numeric_or_day_pattern, content, re.I):
                    continue

                matched = [kw for kw in keywords if kw.lower() in content_l]
                if not matched:
                    continue
                score = len(matched) / max(1, min(len(keywords), 8))
                if role.lower() == "user":
                    score += 0.15
                if re.search(numeric_or_day_pattern, content, re.I):
                    score += 0.35
                if "第" in content and ("天" in content or "日" in content):
                    score += 0.25
                scored.append((score, row, matched))

            scored.sort(key=lambda item: (item[0], int(_row_get(item[1], "id") or 0)), reverse=True)

            candidates = []
            for score, row, matched in scored[:5]:
                ts = _row_get(row, "timestamp")
                t_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts or "")
                mid = _row_get(row, "id")
                content = str(_row_get(row, "content") or "").strip()
                if len(content) > 360:
                    content = content[:357] + "..."
                candidates.append(
                    ScoredCandidate(
                        content=f"原始对话命中：{content}",
                        speaker=str(_row_get(row, "speaker") or _row_get(row, "role") or "user"),
                        source_type="sql",
                        timestamp=t_str,
                        source_name=f"SQL原始对话(match={','.join(matched[:5])})",
                        semantic_score=min(1.0, score),
                        time_decay_score=1.0,
                        graph_hop_score=0.8,
                        impact_score=9.5,
                        confidence=1.0,
                        source_msg_id=int(mid) if mid is not None else None,
                    )
                )
            return candidates
        except Exception as e:
            logger.error("[KnowledgeEngine] SQL keyword retrieval error: %s", e)
            return []

    async def _retrieve_structured_events(self, query: str, user_id: str = None, time_start: str = None, time_end: str = None) -> List[ScoredCandidate]:
        """SQL memory events retrieval track.

        Two passes:
          1) Typed: keyword triggers map a query to one or more MainEventType,
             then call aggregate_events / aggregate_quantities / list_events.
          2) Fallback: when the query carries aggregation/inventory intent
             but typed pass returned nothing, surface every event with a
             non-null quantity for this owner so the LLM can compose a
             total. This catches resource events that older extractor
             passes failed to tag with the correct main_type.
        """
        from memory.sql.event_repository import EventRepository
        from memory.event.slots import MainEventType
        from .scoring import ScoredCandidate
        q, candidates, repo = query.lower(), [], EventRepository()

        triggers = {
            MainEventType.FINANCE: ["花", "买", "钱", "工资", "支出", "账单", "spending", "cost", "money", "buy"],
            MainEventType.HEALTH: ["体重", "身高", "心率", "血压", "运动", "健康", "weight", "height", "health"],
            MainEventType.RESOURCE: [
                "物资", "军需", "粮草", "粮食", "补给", "装备", "军械", "武器", "兵器",
                "马匹", "战马", "库存", "缴获", "战利品", "辎重", "军备", "弹药",
                "inventory", "supply", "supplies", "logistics", "stock", "materiel",
            ],
            MainEventType.PROPERTY: ["资产", "财产", "房产", "持有", "所有", "property", "asset"],
            MainEventType.CONSUMPTION: ["消耗", "用掉", "损耗", "消费", "consume", "consumption"],
        }
        target_types = [etype for etype, kws in triggers.items() if any(k in q for k in kws)]
        supplemental_triggers = {
            MainEventType.OPINION: [
                "喜欢", "不喜欢", "讨厌", "避雷", "推荐", "不推荐", "好喝", "难喝", "好吃", "难吃",
                "偏好", "体验", "性价比", "下次", "以后不", "preference", "like", "dislike", "avoid",
            ],
            MainEventType.TASK: ["待办", "任务", "提醒", "记得", "别忘", "todo", "task", "reminder"],
            MainEventType.SCHEDULE: ["计划", "安排", "日程", "预约", "开会", "明天", "后天", "schedule", "plan"],
            MainEventType.PLAN: ["计划", "打算", "准备", "安排", "plan"],
        }
        for etype, kws in supplemental_triggers.items():
            if etype not in target_types and any(k in q for k in kws):
                target_types.append(etype)
        agg_intent = any(k in q for k in [
            "多少", "总共", "合计", "总量", "总数", "总计", "明细", "列出", "列表", "清单", "盘点", "现在有",
            "total", "sum", "how much", "list", "inventory",
        ])

        if not agg_intent:
            agg_intent = any(k in q for k in [
                "多少", "总共", "合计", "总量", "总数", "总计", "明细", "列出", "列表", "清单", "盘点", "现在有",
                "花了多少钱", "花多少钱", "买了什么", "消费多少",
            ])

        if not target_types and not agg_intent:
            return []

        try:
            for etype in target_types:
                if etype == MainEventType.FINANCE and agg_intent:
                    # Currency-based aggregator (legacy financial path).
                    results = await repo.aggregate_events(owner_id=user_id, main_type=etype, time_start=time_start, time_end=time_end)
                    for res in results:
                        candidates.append(ScoredCandidate(
                            content=f"财务统计：总支出 {res['total_amount']} {res['currency']} (共 {res['count']} 笔交易)",
                            speaker="财务管家", source_type="structured", source_name="SQL:Aggregate", timestamp="",
                            semantic_score=1.0, impact_score=9.0))
                elif agg_intent:
                    # Quantity-based aggregator (resources / inventory / supplies).
                    rows = await repo.aggregate_quantities(owner_id=user_id, main_type=etype, time_start=time_start, time_end=time_end)
                    for r in rows:
                        unit = r.get("quantity_unit") or ""
                        subj = r.get("subject") or ""
                        obj = r.get("object") or ""
                        sub = r.get("subtype") or ""
                        label = obj or subj or sub or "(unknown)"
                        candidates.append(ScoredCandidate(
                            content=f"[{etype.value}] 累计 {label}: {r['total_quantity']} {unit} (共 {r['count']} 条记录)",
                            speaker="物资台账", source_type="structured", source_name="SQL:Aggregate",
                            timestamp="", semantic_score=1.0, impact_score=9.0))

                recent = await repo.list_events(owner_id=user_id, main_type=etype, time_start=time_start, time_end=time_end, limit=10)
                for ev in recent:
                    detail = f"[{ev['main_type']}] {ev['subject']} {ev['predicate']} {ev['object'] or ''}"
                    val = f" ({ev['amount']} {ev['currency'] or 'CNY'})" if ev['amount'] else f" ({ev['quantity']} {ev['quantity_unit'] or ''})" if ev['quantity'] else ""
                    candidates.append(ScoredCandidate(
                        content=detail + val, speaker="结构化记忆", source_type="structured",
                        timestamp=str(ev.get('event_time') or ev.get('created_at') or ''), source_name="SQL:Events",
                        semantic_score=0.9, impact_score=8.5, source_msg_id=ev.get('source_msg_id')))

            # Fallback: aggregation intent without strong type match, OR
            # typed pass returned no candidates. Pull every quantitative event.
            if agg_intent and not candidates:
                rows = await repo.aggregate_quantities(owner_id=user_id, time_start=time_start, time_end=time_end)
                for r in rows:
                    unit = r.get("quantity_unit") or ""
                    subj = r.get("subject") or ""
                    obj = r.get("object") or ""
                    label = obj or subj or "(unknown)"
                    candidates.append(ScoredCandidate(
                        content=f"[QTY] 累计 {label}: {r['total_quantity']} {unit} (共 {r['count']} 条记录)",
                        speaker="物资台账", source_type="structured", source_name="SQL:Aggregate",
                        timestamp="", semantic_score=0.95, impact_score=8.8))
                # Also surface the raw rows so the LLM can attribute by date/source.
                recent_any = await repo.list_quantitative_events(
                    owner_id=user_id, time_start=time_start, time_end=time_end, limit=20
                )
                for ev in recent_any:
                    val = f" ({ev['quantity']} {ev['quantity_unit'] or ''})"
                    detail = f"[{ev['main_type']}] {ev['subject']} {ev['predicate']} {ev['object'] or ''}"
                    candidates.append(ScoredCandidate(
                        content=detail + val,
                        speaker="结构化记忆", source_type="structured",
                        timestamp=str(ev.get('event_time') or ev.get('created_at') or ''),
                        source_name="SQL:QtyFallback",
                        semantic_score=0.8, impact_score=8.0,
                        source_msg_id=ev.get('source_msg_id'),
                    ))

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
        user_id: Optional[str] = None,
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
                if user_id:
                    target_query = (
                        f"SELECT m.id, m.session_id FROM ef_chat_messages m "
                        f"JOIN ef_chat_sessions s ON s.session_id = m.session_id "
                        f"WHERE m.id IN ({placeholders}) AND s.user_id = {'?' if is_sqlite else f'${len(target_ids) + 1}'}"
                    )
                    target_params = [*target_ids, user_id]
                else:
                    target_query = f"SELECT id, session_id FROM ef_chat_messages WHERE id IN ({placeholders})"
                    target_params = target_ids

                if is_sqlite:
                    cursor = await conn.execute(target_query, target_params)
                    target_rows = await cursor.fetchall()
                else:
                    target_rows = await conn.fetch(target_query, *target_params)

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
                        if user_id:
                            cursor = await conn.execute(
                                """
                                SELECT m.id, m.role, m.content
                                FROM ef_chat_messages m
                                JOIN ef_chat_sessions s ON s.session_id = m.session_id
                                WHERE m.session_id = ? AND s.user_id = ?
                                ORDER BY m.id ASC
                                """,
                                (sid, user_id),
                            )
                        else:
                            cursor = await conn.execute(
                                "SELECT id, role, content FROM ef_chat_messages WHERE session_id = ? ORDER BY id ASC",
                                (sid,),
                            )
                        rows = await cursor.fetchall()
                    else:
                        if user_id:
                            rows = await conn.fetch(
                                """
                                SELECT m.id, m.role, m.content
                                FROM ef_chat_messages m
                                JOIN ef_chat_sessions s ON s.session_id = m.session_id
                                WHERE m.session_id = $1 AND s.user_id = $2
                                ORDER BY m.id ASC
                                """,
                                sid,
                                user_id,
                            )
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
            elif res.source_type == "sql":
                tag = "[SQL_EVIDENCE]"
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
