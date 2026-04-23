"""
身份进化引擎 (Identity Evolution Manager)
监控用户的互动，一旦发现称呼/名字的变更（状态变更），
则在 Neo4j 中更新实体节点属性，并建立历史别名拓扑 (IS_ALIAS_OF)。
"""
import json
import re
import logging
from typing import List

from datetime import datetime
from config import llm_config, neo4j_config, identity_config
from bridge.llm import LLMBridge
from neo4j import AsyncGraphDatabase
from memory.event.slots import MemoryEvent
from core.session import ChatSession
from memory.identity.resolver import Actor
from memory.identity.state_reducer import reduce_identity_state
from memory.integration.cdc_outbox import outbox
import uuid
logger = logging.getLogger(__name__)

FORBIDDEN_NAMES = {"女的", "男的", "女仆", "管家", "助手", "AI", "Andrew", "Owner", "系统", "机主", "管理员"}

C_GREEN = "\033[32m"
C_YELLOW = "\033[33m"
C_RESET = "\033[0m"

class IdentityEvolutionManager:
    def __init__(self):
        self.bridge = LLMBridge(llm_config, category="memory")
        self.db_driver = AsyncGraphDatabase.driver(
            neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password)
        )

    async def close(self):
        try:
            await self.db_driver.close()
        except Exception as exc:
            logger.debug("[IdentityEvolution] close failed: %s", exc)

    async def detect_and_evolve(self, events: List[MemoryEvent], session: ChatSession, raw_text: str = None):
        # 0. 尝试正则极速路径 (Zero-LLM)
        if raw_text:
            fast_res = self._fast_detect_rename(raw_text)
            if fast_res:
                if await self._execute_fast_rename(fast_res, session):
                    return # 极速路径命中，直接返回

        state_events = [e for e in events if e.action_type == "STATE_CHANGE"]
        if not state_events: return
            
        current_actor: Actor = getattr(session, "current_actor", None)
        if not current_actor: return

        for evt in state_events:
            # --- [DIRECT_PATH] 优先尝试直达更新 (规避 LLM 延迟与幻觉) ---
            if await self._direct_execute_asst_rename(evt, current_actor, session):
                continue
            if await self._direct_execute_user_rename(evt, current_actor, session):
                continue

            # --- [DEEP_PATH] 直达不命中则进入 LLM 意图分析 ---
            evolve_result = await self._analyze_evolution(evt, current_actor)
            if evolve_result.get("renames"):
                print(f"\n{C_YELLOW}[身份进化引擎] 察觉到身份或别名变更！正在重构图谱认知拓扑...{C_RESET}")
                await self._execute_evolution(evolve_result, current_actor, session)

    async def _apply_assistant_persona_direct(self, updates: dict, user_id: str):
        """物理更新助手根节点字段 (P0 持久化 + P2 隔离)"""
        now = datetime.now().isoformat()
        aid = identity_config.assistant_id
        
        async with self.db_driver.session(database=neo4j_config.database) as session:
            set_clauses = []
            for k in updates.keys():
                set_clauses.append(f"a.{k} = ${k}")
            
            # --- [P2 加固] 强制 owner_id 约束 ---
            cypher = f"""
            MATCH (a:Entity {{entity_id: $aid, owner_id: $uid}})
            SET {', '.join(set_clauses)}, a.persona_updated_at = $now
            """
            await session.run(cypher, {**updates, "aid": aid, "uid": user_id, "now": now})

    async def _analyze_evolution(self, event: MemoryEvent, actor: Actor) -> dict:
        schema = {
            "type": "object",
            "properties": {
                "renames": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "old_name": {"type": "string"},
                            "new_name": {"type": "string"}
                        },
                        "required": ["old_name", "new_name"]
                    }
                }
            },
            "required": ["renames"]
        }
        prompt = (
            f"你是一个严谨的身份拓扑分析师。当前对话里，说话人是 '{actor.speaker_name}'，AI 是 '{actor.target_name}'。\n"
            f"分析下面这个状态变更事件，判断这是否意味着某个人物或者实体被起了一个新名字、新绰号或改变了称呼？\n"
            f"事件内容: {event.predicate} | 语境: {event.context}\n"
        )
        json_str = await self.bridge.chat_completion(
            messages=[
                {"role": "system", "content": "Extract rename intent into JSON: " + json.dumps(schema)},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        if json_str:
            try:
                return json.loads(json_str)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.debug("[IdentityEvolution] invalid evolution payload ignored: %s", exc)
                return {}
        return {}

    def _is_user_alias(self, name: str, actor: Actor) -> bool:
        """判断是否为当前用户的别名"""
        aliases = {actor.speaker_name, "用户", "主人", "我", identity_config.user_id}
        return (name or "").strip() in aliases

    def _is_asst_alias(self, name: str, actor: Actor) -> bool:
        """判断是否为当前助手的别名"""
        candidate = (name or "").strip()
        if not candidate:
            return False

        aliases = [actor.target_name, "AI", "AI助手", "助手", "Andrew", identity_config.assistant_id, "你"]
        if candidate in aliases:
            return True

        candidate_lower = candidate.lower()
        for alias in aliases:
            if not alias:
                continue
            alias_text = str(alias).strip()
            if not alias_text:
                continue
            if alias_text in candidate or alias_text.lower() in candidate_lower:
                return True
        return False

    async def _direct_execute_asst_rename(self, event: MemoryEvent, actor: Actor, session: ChatSession) -> bool:
        semantic_keywords = ["改名", "更改", "修改", "命名", "改成", "叫做", "起名", "取名", "名字", "rename", "name"]
        semantic_text = " ".join(filter(None, [event.subject, event.predicate, event.object, event.context]))
        is_rename_semantic = any(k in semantic_text for k in semantic_keywords) if semantic_text else False
        assistant_markers = ["AI助手", "助手", "assistant", "Andrew", identity_config.assistant_id]
        semantic_lower = semantic_text.lower() if semantic_text else ""
        target_is_asst = (
            self._is_asst_alias(event.object, actor)
            or self._is_asst_alias(event.subject, actor)
            or any(m.lower() in semantic_lower for m in assistant_markers if m)
        )
        
        meta = event.event_metadata or {}
        new_name = meta.get("new_name") or meta.get("name") or meta.get("姓名")
        if not new_name and event.object:
            obj_text = event.object.strip()
            if obj_text and not self._is_asst_alias(obj_text, actor):
                new_name = obj_text
        
        if is_rename_semantic and target_is_asst and new_name:
            uid = identity_config.user_id
            aid = identity_config.assistant_id
            old_name = actor.target_name
            if new_name == old_name:
                return False
            now = datetime.now().isoformat()
            
            async with self.db_driver.session(database=neo4j_config.database) as db_session:
                await db_session.run("""
                    MERGE (e:Entity {entity_id: $aid, owner_id: $uid})
                    WITH e, COALESCE(e.aliases, []) AS old_aliases
                    SET e.name = $new,
                        e.aliases = CASE WHEN $old IN old_aliases THEN old_aliases ELSE old_aliases + $old END,
                        e.persona_updated_at = $now
                    WITH e
                    MERGE (old_node:Entity {name: $old, owner_id: $uid})
                    MERGE (old_node)-[r:IS_ALIAS_OF {owner_id: $uid}]->(e)
                    SET r.valid_at = $now, r.status = 'active'
                """, aid=aid, uid=uid, new=new_name, old=old_name, now=now)
                
                confirm_meta = {
                    "old_value": old_name, "new_value": new_name,
                    "evidence": "Direct user instruction", "source": "direct_evolution",
                    "time": now
                }
                await db_session.run("""
                    MERGE (evt:Event {
                        owner_id: $uid,
                        action_type: 'STATE_CHANGE',
                        subject: 'System_Kernel',
                        predicate: 'IDENTITY_EVOLVED',
                        object: $new
                    })
                    ON CREATE SET 
                        evt.event_id = $eid,
                        evt.context = 'Identity Sovereign Transfer',
                        evt.created_at = $now,
                        evt.event_metadata = $meta_json
                """, eid=str(uuid.uuid4()), uid=uid, new=new_name, now=now, meta_json=json.dumps(confirm_meta))
            
            # --- [P1] CDC Outbox 增量埋点 (Identity) ---
            outbox.append_change(
                owner_id=uid,
                op="upsert",
                entity_type="identity",
                entity_id=aid,
                payload={"name": new_name, "type": "assistant"}
            )

            actor.target_name = new_name
            session.current_actor = actor
            session.identity_state = reduce_identity_state(session.identity_state, {"asst_name": new_name, "source": "explicit"})
            session.context_canvas["evolve_path"] = "direct_asst"
            session.context_canvas["assistant_real_name"] = new_name
            session.context_canvas["Actor_Identity_Context"] = actor.to_context_string()
            print(f"{C_GREEN}[直达进化] 已完成助手命名更新: {old_name} -> {new_name}{C_RESET}")
            return True
        return False

    def _extract_user_name_from_event(self, event: MemoryEvent, actor: Actor) -> str | None:
        semantic_text = " ".join(filter(None, [event.subject, event.predicate, event.object, event.context]))
        if not semantic_text: return None
        assistant_markers = ["AI助手", "助手", "assistant", "Andrew", identity_config.assistant_id]
        semantic_lower = semantic_text.lower()
        if any(m.lower() in semantic_lower for m in assistant_markers if m): return None
        user_semantic_keywords = ["我叫", "我是", "my name is", "i am"]
        if not any(k in semantic_text for k in user_semantic_keywords): return None
        meta = event.event_metadata or {}
        candidate = meta.get("user_name") or meta.get("name") or meta.get("姓名")
        if not candidate:
            for raw in [event.subject, event.object]:
                text = (raw or "").strip()
                if not text or self._is_user_alias(text, actor) or self._is_asst_alias(text, actor): continue
                if text in {"用户", "主人", "我", "自己", "系统"}: continue
                candidate = text; break
        if not candidate: return None
        candidate = candidate.strip()
        if not candidate or candidate == actor.speaker_name or len(candidate) > 20: return None
        if any(bad in candidate for bad in FORBIDDEN_NAMES) or any(bad in candidate for bad in ["名字", "改名", "设置", "标识"]): return None
        return candidate

    async def _direct_execute_user_rename(self, event: MemoryEvent, actor: Actor, session: ChatSession) -> bool:
        new_name = self._extract_user_name_from_event(event, actor)
        if not new_name: return False
        uid = identity_config.user_id
        old_name = actor.speaker_name
        now = datetime.now().isoformat()
        async with self.db_driver.session(database=neo4j_config.database) as db_session:
            await db_session.run("""
                MERGE (e:Entity {entity_id: $uid, owner_id: $uid})
                WITH e, COALESCE(e.aliases, []) AS old_aliases
                SET e.name = $new,
                    e.aliases = CASE WHEN $old IN old_aliases THEN old_aliases ELSE old_aliases + $old END,
                    e.persona_updated_at = $now
                WITH e
                MERGE (old_node:Entity {name: $old, owner_id: $uid})
                MERGE (old_node)-[r:IS_ALIAS_OF {owner_id: $uid}]->(e)
                SET r.valid_at = $now, r.status = 'active'
            """, uid=uid, new=new_name, old=old_name, now=now)
            
            import uuid; import json
            confirm_meta = {
                "old_value": old_name, "new_value": new_name,
                "evidence": "Direct user instruction", "source": "direct_evolution",
                "time": now
            }
            await db_session.run("""
                MERGE (evt:Event {
                    owner_id: $uid,
                    action_type: 'STATE_CHANGE',
                    subject: 'System_Kernel',
                    predicate: 'IDENTITY_EVOLVED',
                    object: $new
                })
                ON CREATE SET 
                    evt.event_id = $eid,
                    evt.context = 'Identity Sovereign Transfer',
                    evt.created_at = $now,
                    evt.event_metadata = $meta_json
            """, eid=str(uuid.uuid4()), uid=uid, new=new_name, now=now, meta_json=json.dumps(confirm_meta))
            
            # --- [P1] CDC Outbox 增量埋点 (Identity) ---
            outbox.append_change(
                owner_id=uid,
                op="upsert",
                entity_type="identity",
                entity_id=uid,
                payload={"name": new_name, "type": "user"}
            )
            
        actor.speaker_name = new_name
        session.current_actor = actor
        session.identity_state = reduce_identity_state(session.identity_state, {"user_name_logic": new_name, "source": "explicit"})
        session.context_canvas["user_real_name"] = new_name
        session.context_canvas["Actor_Identity_Context"] = actor.to_context_string()
        session.context_canvas["evolve_path"] = "direct_user"
        print(f"{C_GREEN}[直达进化] 已完成用户命名更新: {old_name} -> {new_name}{C_RESET}")
        return True

    async def _execute_evolution(self, result: dict, actor: Actor, session: ChatSession):
        from memory.identity.conflict_resolver import ConflictResolver, ConflictCandidate
        
        renames = result.get("renames", [])
        if not renames: return

        # 1. 冲突预选
        groups = {"user": [], "assistant": []}
        for rename in renames:
            old_name = rename.get("old_name"); new_name = rename.get("new_name")
            if not old_name or not new_name or old_name == new_name: continue
            if self._is_user_alias(old_name, actor):
                groups["user"].append(ConflictCandidate(value=new_name, source="user", confidence=0.85, record_time=datetime.now().isoformat()))
            elif self._is_asst_alias(old_name, actor):
                groups["assistant"].append(ConflictCandidate(value=new_name, source="user", confidence=0.85, record_time=datetime.now().isoformat()))

        user_changed = ai_changed = False
        user_id = identity_config.user_id; asst_id = identity_config.assistant_id
        now = datetime.now().isoformat()
        
        async with self.db_driver.session(database=neo4j_config.database) as db_session:
            for role, candidates in groups.items():
                if not candidates: continue
                arbitration = ConflictResolver.resolve_conflict(role, candidates)
                new_name = arbitration.winner
                old_name = actor.speaker_name if role == "user" else actor.target_name
                target_id = user_id if role == "user" else asst_id
                
                await db_session.run("""
                    MERGE (e:Entity {entity_id: $tid, owner_id: $uid})
                    WITH e, COALESCE(e.aliases, []) AS old_aliases
                    SET e.name = $new,
                        e.aliases = CASE WHEN $old IN old_aliases THEN old_aliases ELSE old_aliases + $old END,
                        e.persona_updated_at = $now
                    WITH e
                    MERGE (old_node:Entity {name: $old, owner_id: $uid})
                    MERGE (old_node)-[r:IS_ALIAS_OF {owner_id: $uid}]->(e)
                    SET r.valid_at = $now, r.status = 'active'
                """, tid=target_id, uid=user_id, new=new_name, old=old_name, now=now)
                
                confirm_meta = {
                    "old_value": old_name, "new_value": new_name, 
                    "evidence": arbitration.winner_reason, "source": "deep_evolution",
                    "time": now
                }
                await db_session.run("""
                    MERGE (evt:Event {
                        owner_id: $uid, action_type: 'STATE_CHANGE', subject: 'System_Kernel', predicate: 'IDENTITY_EVOLVED', object: $new
                    })
                    ON CREATE SET 
                        evt.event_id = $eid, evt.context = 'Coherent Identity Arbitration', evt.created_at = $now, evt.event_metadata = $meta_json
                """, eid=str(uuid.uuid4()), uid=user_id, new=new_name, now=now, meta_json=json.dumps(confirm_meta))

                # --- [P1] CDC Outbox 增量埋点 (Identity Deep) ---
                outbox.append_change(
                    owner_id=user_id,
                    op="upsert",
                    entity_type="identity",
                    entity_id=target_id,
                    payload={"name": new_name, "reason": arbitration.winner_reason}
                )

                if role == "user":
                    actor.speaker_name = new_name; user_changed = True
                else:
                    actor.target_name = new_name; ai_changed = True

        if user_changed or ai_changed:
            session.current_actor = actor
            if user_changed: session.context_canvas["user_real_name"] = actor.speaker_name
            if ai_changed: session.context_canvas["assistant_real_name"] = actor.target_name
            session.context_canvas["Actor_Identity_Context"] = actor.to_context_string()
            session.context_canvas["evolve_path"] = "deep_analysis"
            print(f"{C_GREEN}[进化] 身份主权已移交图谱: User=({actor.speaker_name}) | AI=({actor.target_name}){C_RESET}")

    def _fast_detect_rename(self, text: str) -> dict | None:
        asst_patterns = [r"(?:你叫|以后你叫|给你起名)[:：\s]*([A-Za-z\u4e00-\u9fff·]{1,10})"]
        for p in asst_patterns:
            m = re.search(p, text, re.I)
            if m:
                name = m.group(1).strip()
                if 1 <= len(name) <= 12: return {"type": "assistant", "new_name": name}
        user_patterns = [r"(?:我叫|我是)[:：\s]*([A-Za-z\u4e00-\u9fff·]{1,10})"]
        for p in user_patterns:
            m = re.search(p, text, re.I)
            if m:
                name = m.group(1).strip()
                if name in {"你", "我", "他", "她", "它", "谁", "大家"} or name in FORBIDDEN_NAMES: continue
                if 1 <= len(name) <= 12: return {"type": "user", "new_name": name}
        return None

    async def _execute_fast_rename(self, fast_res: dict, session: ChatSession) -> bool:
        actor = getattr(session, "current_actor", None)
        if not actor: return False
        target_type = fast_res["type"]; new_name = fast_res["new_name"]
        uid = identity_config.user_id; aid = identity_config.assistant_id; now = datetime.now().isoformat()
        target_id = aid if target_type == "assistant" else uid
        old_name = actor.target_name if target_type == "assistant" else actor.speaker_name
        if new_name == old_name: return True
        async with self.db_driver.session(database=neo4j_config.database) as db_session:
            await db_session.run("""
                MERGE (e:Entity {entity_id: $tid, owner_id: $uid})
                WITH e, COALESCE(e.aliases, []) AS old_aliases
                SET e.name = $new,
                    e.aliases = CASE WHEN $old IN old_aliases THEN old_aliases ELSE old_aliases + $old END,
                    e.persona_updated_at = $now
                WITH e
                MERGE (old_node:Entity {name: $old, owner_id: $uid})
                MERGE (old_node)-[r:IS_ALIAS_OF {owner_id: $uid}]->(e)
                SET r.valid_at = $now, r.status = 'active'
            """, tid=target_id, uid=uid, new=new_name, old=old_name, now=now)
            confirm_meta = {
                "old_value": old_name, "new_value": new_name,
                "evidence": "Regex match", "source": "fast_regex",
                "time": now
            }
            await db_session.run("""
                MERGE (evt:Event {
                    owner_id: $uid, action_type: 'STATE_CHANGE', subject: 'System_Kernel', predicate: 'IDENTITY_EVOLVED', object: $new
                })
                ON CREATE SET 
                    evt.event_id = $eid, evt.context = 'Identity Sovereign Transfer (Fast-Track)', evt.created_at = $now, evt.event_metadata = $meta_json
            """, eid=str(uuid.uuid4()), uid=uid, new=new_name, now=now, meta_json=json.dumps(confirm_meta))
            
            # --- [P1] CDC Outbox 增量埋点 (Identity Fast) ---
            outbox.append_change(
                owner_id=uid,
                op="upsert",
                entity_type="identity",
                entity_id=target_id,
                payload={"name": new_name, "type": target_type}
            )

        if target_type == "assistant":
            actor.target_name = new_name
            session.identity_state = reduce_identity_state(session.identity_state, {"asst_name": new_name, "source": "fast_track"})
            session.context_canvas["assistant_real_name"] = new_name
        else:
            actor.speaker_name = new_name; session.context_canvas["user_real_name"] = new_name
        session.current_actor = actor; session.context_canvas["Actor_Identity_Context"] = actor.to_context_string()
        session.context_canvas["evolve_path"] = "fast_track"
        print(f"{C_GREEN}[极速路径] 已秒级完成{target_type}身份更新: {old_name} -> {new_name}{C_RESET}")
        return True
