"""
AsyncGraphWriter - Persistence layer for Neo4j memory graph.
Handles entity canonicalization, idempotency logic, and temporal versioning for events and relations.
"""
import logging
import asyncio
import uuid
import json
import time
import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import List

import os
from neo4j import AsyncGraphDatabase
try:
    import dateparser
except Exception:
    dateparser = None

from config import neo4j_config, identity_config
from core.middleware import BaseMiddleware
from core.session import ChatSession
from memory.event.extractor import EventExtractor
from memory.event.slots import (
    MemoryEvent, EntityRelation, PersonaObservation, MemoryEpisode, MemorySaga,
    EventEnvelope
)
from memory.sql.event_repository import EventRepository
from memory.event.normalizer import ContentNormalizerAgent

from memory.identity.canonical import canonicalize_entity
from memory.integration.cdc_outbox import outbox

FORBIDDEN_NAMES = {"AI", "Andrew", "Hong", "assistant", "system"}

def _looks_like_real_person_name(value: str) -> bool:
    candidate = (value or "").strip()
    if not candidate:
        return False
    if candidate in FORBIDDEN_NAMES:
        return False
    if len(candidate) > 20:
        return False

    invalid_exact = {
        "\u7528\u6237", "\u4e3b\u4eba", "\u6211", "\u81ea\u5df1", "\u7cfb\u7edf", "\u52a9\u624b", "AI",
        "\u7a0b\u5e8f\u5458", "\u8bbe\u8ba1\u5e08", "\u79d8\u4e66", "\u52a9\u7406", "\u7ba1\u5bb6", "\u5b66\u751f", "\u8001\u5e08",
        "\u4ee3\u7801\u5c0f\u767d", "\u4ec0\u4e48\u5417", "\u662f\u8c01", "\u53eb\u4ec0\u4e48",
    }
    invalid_fragments = {
        "\u4ec0\u4e48", "\u7a0b\u5e8f\u5458", "\u8bbe\u8ba1\u5e08", "\u4ee3\u7801", "\u9879\u76ee", "\u804c\u4e1a",
        "\u8bb0\u5fc6\u4f53", "\u79d8\u4e66", "\u52a9\u7406", "\u7ba1\u5bb6", "\u662f\u8c01", "\u53eb\u4ec0\u4e48", "\u8bf7\u95ee", "\u4e00\u4e2a",
    }
    if candidate in invalid_exact:
        return False
    if any(fragment in candidate for fragment in invalid_fragments):
        return False
    if re.search(r"\d", candidate):
        return False
    if re.search(r"[,.!?;:\uFF0C\u3002\uFF01\uFF1F\uFF1A\uFF1B]", candidate):
        return False
    if len(candidate) > 6 and re.search(r"[\u4e00-\u9fff]{3,}", candidate):
        return False
    return True



class AsyncGraphWriter:
    def __init__(self):
        self._driver = AsyncGraphDatabase.driver(
            neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password)
        )
        self.database = neo4j_config.database or "neo4j"
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()
        try:
            from memory.identity.entity_resolution import EntityResolver
            self.resolver = EntityResolver()
        except Exception as exc:
            logging.getLogger(__name__).warning("[GraphWriter] EntityResolver disabled: %s", exc)
            self.resolver = None
            
        try:
            from .relation_reasoner import RelationReasoner
            self.reasoner = RelationReasoner(driver=self._driver)
        except Exception as exc:
            logging.getLogger(__name__).warning("[GraphWriter] RelationReasoner disabled: %s", exc)
            self.reasoner = None

    async def _ensure_entity_constraint_strategy(self):
        if self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            queries = [
                "DROP CONSTRAINT entity_id_unique IF EXISTS",
                "CREATE CONSTRAINT owner_entity_unique IF NOT EXISTS FOR (e:Entity) REQUIRE (e.owner_id, e.entity_id) IS UNIQUE",
                "CREATE INDEX entity_name_idx IF NOT EXISTS FOR (e:Entity) ON (e.name)",
                "CREATE INDEX entity_owner_idx IF NOT EXISTS FOR (e:Entity) ON (e.owner_id)",
            ]
            async with self._driver.session(database=self.database) as session:
                for query in queries:
                    await session.run(query)
            self._schema_ready = True

    def _norm(self, name: str) -> str:
        return canonicalize_entity(name)

    def _canonicalize(self, name: str) -> str:
        if not name: return ""
        n = name.strip()
        if n in FORBIDDEN_NAMES: return ""
        return n

    def _now_record_time(self) -> str:
        """Return a record timestamp string for event persistence."""
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _normalize_value_for_slot(self, slot: str, value: str) -> str:
        """Normalize a slot value for semantic deduplication."""
        v = (value or "").strip().lower()
        # Remove common introductory phrases for name/role matching
        noise = ["i am", "i'm", "my name is", "call me", "role is", "identity is"]
        if slot in ["name", "role"]:
            for n in noise:
                if v.startswith(n):
                    v = v[len(n):].strip()
        v = v.replace("，", ":").replace("：", ":").replace("。", "")
        return v

    def _semantic_idempotency_key(self, sub_stable_id: str, slot: str, normalized_value: str, owner_id: str) -> str:
        """Build a semantic idempotency key."""
        raw_key = f"{owner_id}|{sub_stable_id}|{slot}|{normalized_value}"
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def _infer_rel_temporal_slot(self, rel_type: str) -> str:
        rt = (rel_type or "").upper()
        if any(k in rt for k in ["NAME", "ALIAS"]):
            return "name_alias"
        if any(k in rt for k in ["ROLE", "OCCUPATION"]):
            return "role_binding"
        if any(k in rt for k in ["STATUS", "STATE"]):
            return "state_binding"
        return "generic"

    def _infer_temporal_slot(self, event: MemoryEvent) -> str:
        if event.action_type != "STATE_CHANGE":
            return "event"
        p = (event.predicate or "").lower()
        if any(k in p for k in ["name", "alias", "called", "rename"]):
            return "name"
        if any(k in p for k in ["role", "occupation", "identity"]):
            return "role"
        return "state"

    _ZH_DIGIT_MAP = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
                     "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}

    @classmethod
    def _zh_numerals_to_arabic(cls, text: str) -> str:
        # dateparser understands Arabic digits + Chinese units ("3天前") but not
        # Chinese numeral words ("三天前"). Convert numerals preceding 天/日/周/月/年.
        def _convert(match: "re.Match[str]") -> str:
            token = match.group(1)
            if token == "十":
                n = 10
            elif token.startswith("十"):
                n = 10 + cls._ZH_DIGIT_MAP.get(token[1], 0)
            elif token.endswith("十"):
                n = cls._ZH_DIGIT_MAP.get(token[0], 0) * 10
            elif "十" in token:
                head, tail = token.split("十", 1)
                n = cls._ZH_DIGIT_MAP.get(head, 0) * 10 + cls._ZH_DIGIT_MAP.get(tail, 0)
            else:
                n = cls._ZH_DIGIT_MAP.get(token, -1)
                if n < 0:
                    return token
            return str(n)
        return re.sub(r"([零一二两三四五六七八九十]+)(?=个?[天日周月年])", _convert, text)

    def _normalize_event_time(self, event: MemoryEvent) -> str:
        et = getattr(event, "event_time", None)
        if et:
            return str(et)
        ref = (event.timestamp_reference or "").strip()
        if not ref or ref.upper() == "SNAPSHOT":
            return self._now_record_time()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", ref):
            return ref
        if re.match(r"^\d{4}/\d{2}/\d{2}$", ref):
            return ref.replace("/", "-")
        if re.match(r"^\d{4}-\d{2}-\d{2}T", ref):
            return ref
        # Natural language time parsing (zh/en), e.g. 今天/昨天/三天前/last week.
        if dateparser is not None:
            try:
                base = datetime.now(timezone.utc).replace(tzinfo=None)
                candidates = [ref]
                converted = self._zh_numerals_to_arabic(ref)
                if converted != ref:
                    candidates.append(converted)
                for candidate in candidates:
                    parsed = dateparser.parse(
                        candidate,
                        languages=["zh", "en"],
                        settings={
                            "RELATIVE_BASE": base,
                            "PREFER_DATES_FROM": "current_period",
                            "RETURN_AS_TIMEZONE_AWARE": False,
                        },
                    )
                    if parsed:
                        return parsed.date().isoformat()
            except Exception:
                pass
        return self._now_record_time()

    async def _record_evidence_link(self, event_uuid: str, message_id: int):
        """Record the evidence link from Neo4j event UUID to SQL message ID."""
        if not message_id: 
            logging.getLogger(__name__).warning(f"[EVIDENCE_TRACE] Missing message_id for event {event_uuid}, skipping link.")
            return
        from memory.sql.pool import get_db
        try:
            async with get_db() as conn:
                is_sqlite = "sqlite" in str(type(conn)).lower()
                if is_sqlite:
                    await conn.execute(
                        "INSERT OR IGNORE INTO ef_event_evidence_links (event_uuid, message_id) VALUES (?, ?)",
                        (event_uuid, message_id)
                    )
                    await conn.commit()
                else:
                    await conn.execute(
                        "INSERT INTO ef_event_evidence_links (event_uuid, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        event_uuid, message_id
                    )
            logging.getLogger(__name__).info(f"[EVIDENCE_TRACE] Successfully linked Event {event_uuid} -> Msg {message_id}")
        except Exception as e:
            import traceback
            logging.getLogger(__name__).error(f"[EVIDENCE_TRACE] Failed to link {event_uuid} to {message_id} (DB: {'sqlite' if is_sqlite else 'postgres'}):\n{traceback.format_exc()}")

    def _get_entity_params(self, canonical_name: str, owner_id: str, current_names: dict = None, session: ChatSession = None):
        user_real = (current_names or {}).get("user")
        asst_real = (current_names or {}).get("assistant")
        can_norm = self._norm(canonical_name)
        ctx = {}
        user_aliases = []
        assistant_aliases = []
        external_entity_names = []

        if session:
            ctx = {
                "user_real_name": session.context_canvas.get("user_real_name"),
                "assistant_real_name": session.context_canvas.get("assistant_real_name"),
            }
            user_aliases = [str(item or "").strip() for item in (session.context_canvas.get("user_aliases") or []) if str(item or "").strip()]
            assistant_aliases = [str(item or "").strip() for item in (session.context_canvas.get("assistant_aliases") or []) if str(item or "").strip()]
            external_entity_names = [str(item or "").strip() for item in (session.context_canvas.get("external_entity_names") or []) if str(item or "").strip()]
        else:
            ctx = {}

        user_real = ctx.get("user_real_name") or user_real
        asst_real = ctx.get("assistant_real_name") or asst_real

        def _matches_alias_pool(raw_name: str, aliases: list) -> bool:
            probe = self._norm(raw_name)
            if not probe:
                return False
            return any(self._norm(alias) == probe for alias in aliases if alias)

        is_explicit_external_name = _matches_alias_pool(canonical_name, external_entity_names)
        
        if self.resolver and os.getenv("ENABLE_ENTITY_RESOLUTION", "true").lower() == "true" and not is_explicit_external_name:
            res = self.resolver.resolve(canonical_name, owner_id, ctx)
            
            if res.reason != "none" and session:
                audit = session.context_canvas.get("resolution_audit", [])
                audit_entry = {"name": canonical_name, "result": res.resolved_root_id, "reason": res.reason, "confidence": res.confidence}
                if audit_entry not in audit:
                    audit.append(audit_entry)
                    session.context_canvas["resolution_audit"] = audit[-20:] 

            resolved_id = self.resolver.filter_resolution(res, min_confidence=0.95)
            if not resolved_id and res.reason in {"canonical", "alias", "root_id"} and res.resolved_root_id in {identity_config.user_id, identity_config.assistant_id}:
                resolved_id = res.resolved_root_id
            if resolved_id:
                disp_name = res.resolved_name or canonical_name
                if resolved_id == identity_config.user_id: disp_name = user_real or disp_name
                if resolved_id == identity_config.assistant_id: disp_name = asst_real or disp_name
                return {"entity_id": resolved_id, "owner_id": owner_id}, disp_name

        user_norm = self._norm(user_real)
        asst_norm = self._norm(asst_real)
        if (
            can_norm in ["user", identity_config.user_id]
            or (user_norm and can_norm == user_norm)
            or (not is_explicit_external_name and _matches_alias_pool(canonical_name, user_aliases))
        ):
            return {"entity_id": identity_config.user_id, "owner_id": owner_id}, user_real or identity_config.default_user_name
        if (
            can_norm in ["andrew", identity_config.assistant_id]
            or (asst_norm and can_norm == asst_norm)
            or (not is_explicit_external_name and _matches_alias_pool(canonical_name, assistant_aliases))
        ):
            return {"entity_id": identity_config.assistant_id, "owner_id": owner_id}, asst_real or identity_config.default_asst_name
        
        return {"name": canonical_name, "owner_id": owner_id}, canonical_name

    def _find_source_message_text(self, chat_session: ChatSession, source_msg_id: int) -> str:
        if not chat_session:
            return ""
        if source_msg_id:
            for msg in reversed(chat_session.history):
                if getattr(msg, "msg_id", None) == source_msg_id and getattr(msg, "role", None) == "user":
                    return str(getattr(msg, "content", "") or "").strip()
        return str(chat_session.context_canvas.get("latest_user_input") or "").strip()

    def _is_user_self_report_text(self, text: str) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        first_person_patterns = [
            r"\u6211(?:\u53eb|\u662f|\u4eca\u5e74|\u540d\u5b57|\u7684\u540d\u5b57)",
            r"\u6211\s*\d{1,3}\s*\u5c81",
            r"\u6211(?:\u7537|\u5973|\u662f\u7537|\u662f\u5973)",
            r"\u672c\u4eba",
        ]
        second_person_patterns = [
            r"\u4f60(?:\u662f|\u53eb|\u7684)",
            r"\u4ee5\u540e\u4f60",
            r"\u6211\u5e0c\u671b\u4f60",
            r"\u6211\u7684(?:\u79d8\u4e66|\u52a9\u7406|\u7ba1\u5bb6)",
            r"\u7528\u6237\u7684(?:\u79d8\u4e66|\u52a9\u7406|\u7ba1\u5bb6)",
        ]
        has_first = any(re.search(pattern, raw) for pattern in first_person_patterns)
        has_second = any(re.search(pattern, raw) for pattern in second_person_patterns)
        return has_first and not (has_second and not has_first)
    async def _repair_user_root_from_session(self, session, record_time: str, chat_session: ChatSession):
        if not chat_session:
            return
        ctx = chat_session.context_canvas
        repair = {}
        user_name = str(ctx.get("user_real_name") or "").strip()
        if user_name:
            repair["name"] = user_name
            repair["primary_name"] = user_name
        user_age = str(ctx.get("user_age") or "").strip()
        if user_age:
            repair["age"] = user_age
        user_gender = str(ctx.get("user_gender") or "").strip()
        if user_gender:
            repair["gender"] = user_gender
        user_role = str(ctx.get("user_role") or "").strip()
        if user_role:
            repair["role"] = user_role
        aliases = [str(item or "").strip() for item in (ctx.get("user_aliases") or []) if str(item or "").strip()]
        if aliases:
            repair["aliases"] = aliases
        if not repair:
            return
        _set_clause = ", ".join([f"u.{k} = ${k}_repair" for k in repair])
        _params = {f"{k}_repair": v for k, v in repair.items()}
        _params["uid"] = identity_config.user_id
        _params["now"] = record_time
        await session.run(
            f"MATCH (u:Entity {{entity_id: $uid}}) SET {_set_clause}, u.persona_updated_at = $now",
            _params,
        )


    async def write_events(self, valid_events: List[MemoryEvent], candidate_events: List[MemoryEvent], session_id: str, owner_id: str, current_names: dict = None, chat_session: ChatSession = None) -> List[str]:
        await self._ensure_entity_constraint_strategy()
        logging.getLogger(__name__).info(f"[EVIDENCE_TRACE] Entering write_events with {len(valid_events)} valid events.")
        created_uuids = []
        async with self._driver.session(database=self.database) as session:
            for event in valid_events:
                logging.getLogger(__name__).info(f"[EVIDENCE_TRACE] Processing Event: {event.predicate} (source_msg_id: {getattr(event, 'source_msg_id', 'None')})")
                sub_can = self._canonicalize(event.subject)
                obj_can = self._canonicalize(event.object)
                if not sub_can: continue
                
                sub_match, sub_name = self._get_entity_params(sub_can, owner_id, current_names, session=chat_session)
                sub_props = [f"{k}: ${k}" for k in sub_match.keys()]
                cypher_sub = f"MERGE (s:Entity {{ {', '.join(sub_props)} }}) ON CREATE SET s.name = $sub_name WITH s "
                
                temporal_slot = self._infer_temporal_slot(event)
                record_time = self._now_record_time()
                sub_match_key = "entity_id" if "entity_id" in sub_match else "name"
                sub_stable_id = sub_match.get("entity_id") or sub_match.get("name")
                
                val_str = event.predicate + (f" {event.object}" if event.object else "")
                norm_val = self._normalize_value_for_slot(temporal_slot, val_str)
                semantic_key = self._semantic_idempotency_key(sub_stable_id, temporal_slot, norm_val, owner_id)

                cypher_body = f"""
                MATCH (s:Entity) WHERE s.{{sub_match_key}} = ${{sub_match_key}} AND s.owner_id = $owner_id
                
                OPTIONAL MATCH (s)-[:ACTOR_IN]->(old:Event {{status: 'active', owner_id: $owner_id}})
                WHERE (old.temporal_slot = $temporal_slot OR (old.temporal_slot IS NULL AND old.predicate = $predicate))
                  AND $temporal_slot IN ["name", "role", "state"]
                  AND old.semantic_key <> $semantic_key
                SET old.status = 'invalidated',
                    old.invalid_at = $record_time
                WITH s, collect(old) as olds
                
                MERGE (existing:Event {{
                    semantic_key: $semantic_key,
                    owner_id: $owner_id
                }})
                ON CREATE SET 
                    existing.uuid = $uuid,
                    existing.subject = $sub_can, 
                    existing.predicate = $predicate, 
                    existing.object = COALESCE($obj_can, ""),
                    existing.timestamp_reference = $timestamp,
                    existing.temporal_slot = $temporal_slot,
                    existing.action_type = $action_type,
                    existing.impact_score = $impact,
                    existing.confidence = $confidence,
                    existing.context = $context,
                    existing.event_metadata = $event_metadata,
                    existing.status = 'active',
                    existing.source_system = 'ebbingflow',
                    existing.valid_at = $record_time,
                    existing.invalid_at = null,
                    existing.mention_count = 1,
                    existing.event_time = $event_time,
                    existing.source_msg_id = $source_msg_id,
                    existing.record_time = $record_time,
                    existing.created_at = $record_time
                ON MATCH SET 
                    existing.mention_count = COALESCE(existing.mention_count, 0) + 1,
                    existing.updated_at = $record_time,
                    existing.record_time = $record_time,
                    existing.status = 'active',
                    existing.event_time = COALESCE(existing.event_time, $event_time),
                    existing.source_msg_id = COALESCE(existing.source_msg_id, $source_msg_id)
                
                MERGE (s)-[:ACTOR_IN]->(existing)
                WITH existing, olds
                UNWIND CASE WHEN size(olds) > 0 THEN olds ELSE [null] END AS inv_old
                RETURN existing.uuid as final_id, existing.semantic_key as final_sk,
                       collect(DISTINCT CASE WHEN inv_old IS NOT NULL THEN inv_old.uuid ELSE null END) as inv_ids
                """
                
                params = {
                    **sub_match,
                    "sub_name": sub_name, "sub_can": sub_can, "obj_can": obj_can,
                    "predicate": event.predicate, "action_type": event.action_type,
                    "uuid": str(uuid.uuid4()), "semantic_key": semantic_key,
                    "timestamp": event.timestamp_reference or "SNAPSHOT",
                    "impact": event.impact_score, "confidence": event.confidence,
                    "context": event.context, "event_metadata": json.dumps(event.event_metadata, ensure_ascii=False),
                    "temporal_slot": temporal_slot, "event_time": self._normalize_event_time(event),
                    "record_time": record_time, "owner_id": owner_id,
                    "source_msg_id": getattr(event, "source_msg_id", None),
                }
                
                res = await session.run(cypher_sub + cypher_body.replace("{sub_match_key}", sub_match_key), params)
                data = await res.single()
                if not data: continue
                
                final_id = data["final_id"]
                final_sk = data["final_sk"]
                inv_ids = data["inv_ids"]
                
                logging.getLogger(__name__).info(f"[EVIDENCE_TRACE] Created/Matched Event UUID: {final_id} for Msg ID: {getattr(event, 'source_msg_id', 'None')}")
                await self._record_evidence_link(final_id, getattr(event, "source_msg_id", None))

                cdc_eid = final_sk if final_sk else final_id
                outbox.append_change(
                    owner_id=owner_id,
                    op="upsert",
                    entity_type="event",
                    entity_id=cdc_eid,
                    payload={
                        "subject": sub_name,
                        "predicate": event.predicate,
                        "object": obj_can,
                        "slot": temporal_slot,
                        "record_time": record_time,
                        "event_time": params["event_time"],
                        "source_msg_id": getattr(event, "source_msg_id", None),
                    },
                )
                
                for iid in inv_ids:
                    if iid and iid != cdc_eid:
                        outbox.append_change(owner_id, "invalidate", "event", iid, {"slot": temporal_slot, "reason": "superseded"})


                # --- [Demographic Auto Write-back] ---
                # If the event subject is the user Entity and event_metadata contains
                # demographic keys, normalize via field_contract and SET onto Entity node.
                _is_user_subject = sub_match.get("entity_id") == identity_config.user_id
                if _is_user_subject and event.event_metadata:
                    _source_text = self._find_source_message_text(chat_session, getattr(event, "source_msg_id", None))
                    _allow_user_demo_writeback = self._is_user_self_report_text(_source_text)
                    _meta = event.event_metadata if isinstance(event.event_metadata, dict) else {}
                    _DEMOGRAPHIC_KEYS = {
                        "age", "gender", "sex", "birthday", "birth",
                        "occupation", "job", "work", "location", "city", "address",
                        "education", "school", "name",
                    }
                    _raw_demo = {k: v for k, v in _meta.items() if k.lower() in _DEMOGRAPHIC_KEYS}
                    if _raw_demo and _allow_user_demo_writeback:
                        try:
                            from memory.identity.field_contract import normalize_profile_updates
                            _demo_updates, _dropped = normalize_profile_updates(_raw_demo)
                        except Exception:
                            _demo_updates = {k: str(v) for k, v in _raw_demo.items() if v not in (None, "", "unknown")}
                        if _demo_updates:
                            if "name" in _demo_updates:
                                _demo_updates["primary_name"] = _demo_updates["name"]
                            _set_clause = ", ".join([f"u.{k} = ${k}_demo" for k in _demo_updates])
                            _demo_params = {f"{k}_demo": v for k, v in _demo_updates.items()}
                            _demo_params["uid"] = identity_config.user_id
                            _demo_params["now"] = record_time
                            await session.run(
                                f"MATCH (u:Entity {{entity_id: $uid}}) SET {_set_clause}, u.persona_updated_at = $now",
                                _demo_params
                            )
                            logging.getLogger(__name__).info(
                                f"[DemographicSync] Auto write-back user demographic fields: {_demo_updates}"
                            )
                    elif _raw_demo and not _allow_user_demo_writeback:
                        logging.getLogger(__name__).info(
                            f"[DemographicSync] Skipped user demographic write-back for assistant-directed text: {_source_text}"
                        )
                        await self._repair_user_root_from_session(session, record_time, chat_session)
                created_uuids.append(final_id)
        return created_uuids

    async def write_relations(self, relations: List[EntityRelation], session_id: str, owner_id: str, current_names: dict = None, chat_session: ChatSession = None):
        await self._ensure_entity_constraint_strategy()
        async with self._driver.session(database=self.database) as session:
            for rel in relations:
                from_can = self._canonicalize(rel.from_entity)
                to_can = self._canonicalize(rel.to_entity)
                if not from_can or not to_can: continue
                
                from_match, from_name = self._get_entity_params(from_can, owner_id, current_names, session=chat_session)
                to_match, to_name = self._get_entity_params(to_can, owner_id, current_names, session=chat_session)
                
                high_risk_kws = ["SAME_AS", "WIFE", "HUSBAND", "SPOUSE", "LOVER", "PAY", "BANK", "FINANCE", "DOCTOR", "PATIENT", "DISEASE", "MEDICAL"]
                rt_u = rel.relation_type.upper()
                if any(k in rt_u for k in high_risk_kws) and not rt_u.startswith("POSSIBLY_") and not getattr(rel, "confirmed", False):
                    rel.relation_type = f"POSSIBLY_{rel.relation_type}"
                    rel.confidence = 0.5

                temporal_slot = self._infer_rel_temporal_slot(rel.relation_type)
                record_time = self._now_record_time()
                
                a_props = [f"{k}: $a_{k}" for k in from_match.keys()]
                b_props = [f"{k}: $b_{k}" for k in to_match.keys()]
                
                cypher = f"MATCH (a:Entity {{ {', '.join(a_props)} }}), (b:Entity {{ {', '.join(b_props)} }}) "
                if temporal_slot != "generic":
                    cypher += f"""
                    OPTIONAL MATCH (a)-[old:RELATION {{owner_id: $owner_id, temporal_slot: $temporal_slot}}]->()
                    WHERE old.status = 'active' OR old.status IS NULL
                    SET old.status = 'invalidated', old.invalid_at = $record_time
                    WITH a, b, collect(DISTINCT old) as inv_rels
                    """
                else:
                    cypher += "WITH a, b, [] as inv_rels "
                
                cypher += """
                MERGE (a)-[r:RELATION {type: $rel_type, owner_id: $owner_id, to_name: $to_name}]->(b)
                SET r.status        = 'active', 
                    r.temporal_slot = $temporal_slot, 
                    r.valid_at      = $record_time, 
                    r.record_time   = $record_time,
                    r.from_id       = $from_id,
                    r.to_id         = $to_id,
                    r.source_msg_id = COALESCE(r.source_msg_id, $source_msg_id),
                    r.inferred      = $inferred,
                    r.inference_rule = $inference_rule,
                    r.confidence     = $confidence
                RETURN inv_rels
                """
                
                fid = from_match.get('entity_id') or from_name
                tid = to_match.get('entity_id') or to_name
                params = {
                    "rel_type": rel.relation_type, "temporal_slot": temporal_slot, "record_time": record_time,
                    "from_id": fid, "to_id": tid,
                    "from_name": from_name, "to_name": to_name, "owner_id": owner_id, "session_id": session_id,
                    "source_msg_id": getattr(rel, "source_msg_id", None),
                    "inferred": getattr(rel, "inferred", False),
                    "inference_rule": getattr(rel, "inference_rule", ""),
                    "confidence": getattr(rel, "confidence", 1.0)
                }
                for k, v in from_match.items(): params[f"a_{k}"] = v
                for k, v in to_match.items(): params[f"b_{k}"] = v
                
                res = await session.run(cypher, params)
                data = await res.single()
                inv_rels = data["inv_rels"] if data else []
                if not getattr(rel, "source_msg_id", None):
                    logging.getLogger(__name__).warning(
                        f"[EVIDENCE_TRACE] relation source_msg_id missing: {from_name} -[{rel.relation_type}]-> {to_name}"
                    )
                
                rel_id = f"{fid}|{rel.relation_type}|{tid}|{temporal_slot}"
                outbox.append_change(
                    owner_id,
                    "upsert",
                    "relation",
                    rel_id,
                    {
                        "from": from_name,
                        "to": to_name,
                        "type": rel.relation_type,
                        "slot": temporal_slot,
                        "source_msg_id": getattr(rel, "source_msg_id", None),
                    },
                )
                for ir in inv_rels:
                    ir_props = ir if isinstance(ir, dict) else dict(ir)
                    ityp = getattr(ir, "type", None) or ir_props.get("type") or ir_props.get("rel_type")
                    ifr = ir_props.get("from_id") or from_name
                    itr = ir_props.get("to_id") or ir_props.get("to_name")
                    isl = ir_props.get("temporal_slot")
                    ir_id = f"{ifr}|{ityp}|{itr}|{isl}"
                    outbox.append_change(owner_id, "invalidate", "relation", ir_id, {"slot": isl, "invalid_at": record_time})

        if self.reasoner and os.getenv("ENABLE_RELATION_REASONER", "true").lower() == "true":
            base_rels = [r for r in relations if not getattr(r, "inferred", False)]
            if base_rels:
                try:
                    reason_input = []
                    for r in base_rels:
                         fn_can = self._canonicalize(r.from_entity)
                         tn_can = self._canonicalize(r.to_entity)
                         if not fn_can or not tn_can: continue
                         f_m, _ = self._get_entity_params(fn_can, owner_id, current_names, session=chat_session)
                         t_m, _ = self._get_entity_params(tn_can, owner_id, current_names, session=chat_session)
                         reason_input.append({
                             "from": r.from_entity, "type": r.relation_type, "to": r.to_entity,
                             "from_id": f_m.get("entity_id") or f_m.get("name"),
                             "to_id": t_m.get("entity_id") or t_m.get("name"),
                             "source_msg_id": getattr(r, "source_msg_id", None),
                         })
                    
                    inferred_data = await self.reasoner.reason(reason_input, owner_id)
                    if inferred_data:
                        inf_objects = []
                        for i in inferred_data:
                            if i["from_id"] == i["to_id"]: continue
                            inf_objects.append(EntityRelation(
                                from_entity=i["from"],
                                relation_type=i["type"],
                                to_entity=i["to"],
                                inferred=True,
                                inference_rule=i["rule"],
                                confidence=0.9,
                                source_msg_id=i.get("source_msg_id"),
                            ))
                        if inf_objects:
                            await self.write_relations(inf_objects, session_id, owner_id, current_names, chat_session=chat_session)
                except Exception as ex:
                    logging.getLogger(__name__).warning(f"Relation reasoning failed for some relations: {ex}")

    async def write_episode(self, episode, session_id: str, owner_id: str):
        await self._ensure_entity_constraint_strategy()
        """Persist a MemoryEpisode node and link it to its events."""
        async with self._driver.session(database=self.database) as session:
            cypher = """
            MERGE (ep:Episode {episode_id: $episode_id, owner_id: $owner_id})
            ON CREATE SET
                ep.name = $name,
                ep.summary = $summary,
                ep.start_time = $start_time,
                ep.end_time = $end_time,
                ep.session_id = $session_id,
                ep.evidence_msg_ids = $evidence_msg_ids,
                ep.efstb_urgency_level = $efstb_urgency_level,
                ep.efstb_instruction_compliance = $efstb_instruction_compliance,
                ep.created_at = $record_time
            ON MATCH SET
                ep.name = $name,
                ep.summary = $summary,
                ep.efstb_urgency_level = $efstb_urgency_level,
                ep.efstb_instruction_compliance = $efstb_instruction_compliance
            WITH ep
            UNWIND CASE WHEN size($associated_events) > 0 THEN $associated_events ELSE [null] END AS event_id
            OPTIONAL MATCH (ev:Event {uuid: event_id})
            WITH ep, ev WHERE ev IS NOT NULL
            MERGE (ep)-[:CONTAINS_EVENT]->(ev)
            RETURN ep.episode_id
            """
            efstb_tags = getattr(episode, "efstb_tags", {}) or {}
            params = {
                "episode_id": episode.episode_id,
                "owner_id": owner_id,
                "name": episode.name,
                "summary": episode.summary,
                "start_time": episode.start_time,
                "end_time": episode.end_time,
                "session_id": session_id,
                "evidence_msg_ids": episode.evidence_msg_ids,
                "efstb_urgency_level": float(efstb_tags.get("urgency_level", 0.5)),
                "efstb_instruction_compliance": float(efstb_tags.get("instruction_compliance", 0.5)),
                "record_time": self._now_record_time(),
                "associated_events": episode.associated_events
            }
            try:
                result = await session.run(cypher, params)
                await result.consume()
                logging.getLogger(__name__).info(f"[EpisodeTrace] Saved MemoryEpisode {episode.episode_id}: {episode.name}")
            except Exception as e:
                logging.getLogger(__name__).error(f"[EpisodeTrace] Failed to save MemoryEpisode {episode.episode_id}: {e}")

    async def write_saga(self, saga, owner_id: str):
        await self._ensure_entity_constraint_strategy()
        """Persist a MemorySaga node and keep episode links updated."""
        async with self._driver.session(database=self.database) as session:
            cypher = """
            MERGE (sg:Saga {saga_id: $saga_id, owner_id: $owner_id})
            ON CREATE SET
                sg.title = $title,
                sg.description = $description,
                sg.start_time = $start_time,
                sg.last_active = $last_active,
                sg.created_at = $record_time
            ON MATCH SET
                sg.title = $title,
                sg.description = $description,
                sg.last_active = $last_active
            WITH sg
            UNWIND CASE WHEN size($episode_ids) > 0 THEN $episode_ids ELSE [null] END AS ep_id
            OPTIONAL MATCH (ep:Episode {episode_id: ep_id})
            WITH sg, ep WHERE ep IS NOT NULL
            MERGE (sg)-[:CONTAINS_EPISODE]->(ep)
            RETURN sg.saga_id
            """
            params = {
                "saga_id": saga.saga_id,
                "owner_id": owner_id,
                "title": saga.title,
                "description": saga.description,
                "start_time": saga.start_time,
                "last_active": saga.last_active,
                "episode_ids": saga.associated_episode_ids,
                "record_time": self._now_record_time()
            }
            try:
                await session.run(cypher, params)
                logging.getLogger(__name__).info(f"[SagaTrace] Saved MemorySaga {saga.saga_id}: {saga.title}")
            except Exception as e:
                logging.getLogger(__name__).error(f"[SagaTrace] Failed to save MemorySaga {saga.saga_id}: {e}")

    async def get_active_sagas(self, owner_id: str, limit: int = 10) -> List[MemorySaga]:
        """Provide recent saga context for clustering decisions."""
        async with self._driver.session(database=self.database) as session:
            cypher = "MATCH (sg:Saga {owner_id: $uid}) RETURN sg ORDER BY sg.last_active DESC LIMIT $limit"
            result = await session.run(cypher, uid=owner_id, limit=limit)
            records = await result.data()
            sagas = []
            for r in records:
                node = r['sg']
                sagas.append(MemorySaga(
                    saga_id=node['saga_id'],
                    title=node['title'],
                    description=node['description'],
                    start_time=node.get('start_time'),
                    last_active=node.get('last_active'),
                ))
            return sagas

    async def close(self):
        await self._driver.close()

class GraphWriterMiddleware(BaseMiddleware):
    """Middleware for extracting and persisting graph memory."""
    def __init__(self):
        self.writer = AsyncGraphWriter()
        self.extractor = EventExtractor()
        self.event_repo = EventRepository()
        self.normalizer = ContentNormalizerAgent()
        try:
            from memory.integration.episode_manager import EpisodeManager
            self.episode_manager = EpisodeManager()
        except Exception as exc:
            logging.getLogger(__name__).warning("[GraphWriterMiddleware] EpisodeManager disabled: %s", exc)
            self.episode_manager = None

        try:
            from memory.integration.saga_manager import SagaManager
            self.saga_manager = SagaManager()
        except Exception as exc:
            logging.getLogger(__name__).warning("[GraphWriterMiddleware] SagaManager disabled: %s", exc)
            self.saga_manager = None
            
        try:
            from memory.identity.evolution import IdentityEvolutionManager
            self.evolution_manager = IdentityEvolutionManager()
        except Exception as exc:
            logging.getLogger(__name__).warning("[GraphWriterMiddleware] IdentityEvolution disabled: %s", exc)
            self.evolution_manager = None

    async def close(self):
        if self.evolution_manager:
            await self.evolution_manager.close()
        await self.writer.close()

    async def process_response(self, ai_output: str, session: ChatSession):
        user_messages = [m for m in session.history if m.role == "user"]
        if not user_messages: return ai_output
        
        last_user_msg_obj = user_messages[-1]
        last_user_msg_text = last_user_msg_obj.content
        source_msg_id = getattr(last_user_msg_obj, "msg_id", None)
        
        actor = getattr(session, "current_actor", None)
        try:
            valid_events, candidate_events, relations, observations, valid_envelopes = await self.extractor.extract_events_from_text(
                last_user_msg_text, actor, source_msg_id=source_msg_id
            )
            
            # --- [M1 Fix] Apply persona observations to PersonaManager ---
            if observations:
                from memory.identity.manager import PersonaManager
                p_manager = PersonaManager()
                try:
                    await p_manager.apply_observations(session, observations)
                finally:
                    await p_manager.close()
            
            if not source_msg_id:
                logging.getLogger(__name__).error(f"[EVIDENCE_TRACE] source_msg_id is MISSING for message: '{last_user_msg_text[:20]}...', evidence chain will be BROKEN.")
            
            if self.evolution_manager: 
                await self.evolution_manager.detect_and_evolve(valid_events, session, last_user_msg_text)
            
            current_names = {
                "user": session.context_canvas.get("user_real_name"), 
                "assistant": session.context_canvas.get("assistant_real_name")
            }
            
            created_event_ids = []
            if valid_events: 
                created_event_ids = await self.writer.write_events(valid_events, candidate_events, session.session_id, session.user_id, current_names, chat_session=session)
            
            if relations: 
                await self.writer.write_relations(relations, session.session_id, session.user_id, current_names, chat_session=session)
            
            # --- [Structured Memory Events] ---
            if valid_envelopes:
                # 0. Normalization
                normalized_envelopes = self.normalizer.normalize_envelopes(valid_envelopes)
                
                for env in normalized_envelopes:
                    try:
                        # 1. SQL Insert
                        event_id = await self.event_repo.insert_event(env, owner_id=session.user_id)
                        # 2. Evidence Link
                        if event_id and source_msg_id:
                            await self.event_repo.link_evidence(event_id, source_msg_id)
                    except Exception as env_err:
                        logging.getLogger(__name__).error(f"[EventSQL] Sync failed: {env_err}")
                
            # [M2.1] Episode generation and persistence
            un_episoded_turns = session.context_canvas.get("un_episoded_turns", 0) + 1
            session.context_canvas["un_episoded_turns"] = un_episoded_turns
            
            ep_events_buffer = session.context_canvas.get("ep_events_buffer", [])
            if created_event_ids:
                ep_events_buffer.extend(created_event_ids)
            session.context_canvas["ep_events_buffer"] = ep_events_buffer
            
            # Use 5 turns as episode aggregation threshold
            if self.episode_manager and un_episoded_turns >= 5:
                msgs_dicts = [m.to_dict() for m in session.history][-10:]
                # --- [M1 Fix] Extract episode summary and behavior tags ---
                episode = await self.episode_manager.extract_episode(msgs_dicts, ep_events_buffer)
                if episode:
                    await self.writer.write_episode(episode, session.session_id, session.user_id)

                    # Keep latest short-term behavior tags in session context for persona injection.
                    episode_efstb = getattr(episode, "efstb_tags", {}) or {}
                    if episode_efstb:
                        session.context_canvas["latest_efstb_tags"] = episode_efstb
                    if getattr(episode, "mbti_hint", None):
                        session.context_canvas["mbti_label"] = episode.mbti_hint
                    if getattr(episode, "core_values_hint", None):
                        current_values = list(session.context_canvas.get("user_core_values", []) or [])
                        for value in episode.core_values_hint:
                            if value not in current_values:
                                current_values.append(value)
                        session.context_canvas["user_core_values"] = current_values[:8]
                    if getattr(episode, "big_five_observed", None):
                        for key, value in (episode.big_five_observed or {}).items():
                            try:
                                session.context_canvas[f"big_five_{key}"] = max(0.0, min(1.0, float(value)))
                            except (TypeError, ValueError):
                                continue
                    if (
                        getattr(episode, "mbti_hint", None)
                        or getattr(episode, "core_values_hint", None)
                        or getattr(episode, "big_five_observed", None)
                    ):
                        from memory.identity.manager import PersonaManager
                        p_manager = PersonaManager()
                        try:
                            await p_manager.apply_long_term_persona_hints(
                                session.user_id,
                                mbti_label=getattr(episode, "mbti_hint", None),
                                big_five=getattr(episode, "big_five_observed", None),
                                core_values=getattr(episode, "core_values_hint", None),
                            )
                        finally:
                            await p_manager.close()
                        try:
                            from memory.identity.schema import (
                                BigFiveVector,
                                DualLayerProfile,
                                LongTermPersona,
                                EfstbBehavioralTags,
                            )

                            short_term = EfstbBehavioralTags(
                                urgency_level=float(episode_efstb.get("urgency_level", 0.5)),
                                instruction_compliance=float(episode_efstb.get("instruction_compliance", 0.5)),
                                granularity_preference=str(episode_efstb.get("granularity_preference", "medium")),
                                logic_vs_emotion=float(episode_efstb.get("logic_vs_emotion", 0.5)),
                            )
                            long_term = LongTermPersona(
                                mbti_label=session.context_canvas.get("mbti_label"),
                                big_five=BigFiveVector(
                                    openness=float(session.context_canvas.get("big_five_openness", 0.5)),
                                    conscientiousness=float(session.context_canvas.get("big_five_conscientiousness", 0.5)),
                                    extraversion=float(session.context_canvas.get("big_five_extraversion", 0.5)),
                                    agreeableness=float(session.context_canvas.get("big_five_agreeableness", 0.5)),
                                    neuroticism=float(session.context_canvas.get("big_five_neuroticism", 0.5)),
                                ),
                                core_values=session.context_canvas.get("user_core_values", []),
                            )
                            session.context_canvas["dual_layer_profile"] = DualLayerProfile(
                                profile_id=session.user_id,
                                long_term=long_term,
                                short_term=short_term,
                            )
                        except Exception as e:
                            logging.getLogger(__name__).warning(f"[PersonaInjection] Build profile failed: {e}")
                    
                    # [M2.2] 闂佽崵鍠愰悷杈╃不閹达絻浜?Saga 闂備浇澹堟ご鎼佹嚌妤ｅ啫绠?
                    if self.saga_manager:
                        existing_sagas = await self.writer.get_active_sagas(session.user_id)
                        target_saga = await self.saga_manager.cluster_episodes_into_saga(episode, existing_sagas)
                        if target_saga:
                            # Ensure the current episode is linked into saga
                            if episode.episode_id not in target_saga.associated_episode_ids:
                                target_saga.associated_episode_ids.append(episode.episode_id)
                            await self.writer.write_saga(target_saga, session.user_id)

                session.context_canvas["un_episoded_turns"] = 0
                session.context_canvas["ep_events_buffer"] = []
                
        except Exception as e: 
            import traceback
            logging.getLogger(__name__).error(f"婵＄到閸?[MemoryGraph] Error: {e}\n{traceback.format_exc()}")
            
        return ai_output


