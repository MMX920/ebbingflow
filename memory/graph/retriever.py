"""
Memory graph retriever middleware.

This middleware performs two lightweight jobs before core reasoning:
1) hydrate user/assistant identity context from graph/persona storage
2) recall recent and keyword-related graph events into context_canvas
"""

import logging
from typing import List, Optional

from neo4j import AsyncGraphDatabase

from config import identity_config, neo4j_config
from core.middleware import BaseMiddleware
from core.session import ChatSession
from memory.identity.state_reducer import reduce_identity_state

logger = logging.getLogger(__name__)


class MemoryRetrieverMiddleware(BaseMiddleware):
    """Pre-request graph retriever and identity hydrator."""

    def __init__(self, top_k: int = 5):
        self.top_k = max(1, int(top_k))
        self._driver = AsyncGraphDatabase.driver(
            uri=neo4j_config.uri,
            auth=(neo4j_config.username, neo4j_config.password),
        )
        self.database = neo4j_config.database

    async def close(self):
        await self._driver.close()

    def __del__(self):
        try:
            import asyncio

            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.close())
        except Exception:
            pass

    async def process_request(self, user_input: str, session: ChatSession) -> str:
        """Recall graph memory snippets and sync identity context."""
        try:
            await self._query_root_identities(session)

            recent_facts = await self._query_recent_events(session.user_id)
            keywords = self._extract_keywords(user_input)
            related_facts = await self._query_related_events(session.user_id, keywords)

            all_facts = list(dict.fromkeys(recent_facts + related_facts))
            if all_facts:
                session.context_canvas["图谱事实记忆"] = "\n".join(all_facts[:10])
            else:
                session.context_canvas.pop("图谱事实记忆", None)

            plan_events = await self._query_plan_events(session.user_id)
            if plan_events:
                session.context_canvas["待跟进的计划事项"] = "\n".join(plan_events)
            else:
                session.context_canvas.pop("待跟进的计划事项", None)

            session.context_canvas["source_mix"] = {
                "event": len(all_facts),
                "episode": 0,
                "vector": 0,
                "bm25": 0,
            }
        except Exception as e:
            logger.exception("[MemoryRetriever] request prefetch failed: %s", e)

        return user_input

    @staticmethod
    def _extract_keywords(text: str) -> List[str]:
        raw = str(text or "").strip()
        if not raw:
            return []

        # Keep simple and robust: Chinese 2-grams + long alnum tokens.
        zh_bigrams = [raw[i : i + 2] for i in range(max(0, len(raw) - 1))]
        en_tokens = [tok for tok in raw.replace("\n", " ").split(" ") if len(tok) >= 4]

        seen = set()
        keywords = []
        for item in zh_bigrams + en_tokens:
            k = item.strip()
            if not k or k in seen:
                continue
            seen.add(k)
            keywords.append(k)
        return keywords[:30]

    @staticmethod
    def _fmt_event_line(subject: str, predicate: str, time_ref: str, default_time: str) -> str:
        s = str(subject or "").strip() or "未知主体"
        p = str(predicate or "").strip() or "发生事件"
        t = str(time_ref or "").strip() or default_time
        return f"- {s} {p}（{t}）"

    async def _query_recent_events(
        self,
        user_id: str,
        time_window_start: Optional[str] = None,
        time_window_end: Optional[str] = None,
    ) -> List[str]:
        """Query recently active events."""
        async with self._driver.session(database=self.database) as session:
            time_filter = ""
            params = {"uid": user_id}
            if time_window_start and time_window_end:
                time_filter = """
                AND (
                    (evt.event_time IS NOT NULL AND evt.event_time >= $start AND evt.event_time <= $end) OR
                    (evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start) AND (evt.record_time <= $end OR evt.created_at <= $end))
                )
                """
                params["start"] = time_window_start
                params["end"] = time_window_end

            result = await session.run(
                f"""
                MATCH (sub:Entity)-[]->(evt:Event)
                WHERE evt.owner_id = $uid AND evt.status = 'active' AND evt.invalid_at IS NULL
                {time_filter}
                RETURN sub.name AS subject, evt.predicate AS predicate, evt.timestamp_reference AS time_ref
                ORDER BY evt.record_time DESC, evt.created_at DESC
                LIMIT 3
                """,
                **params,
            )
            rows = await result.data()
            return [
                self._fmt_event_line(r.get("subject"), r.get("predicate"), r.get("time_ref"), "时间未知")
                for r in rows
            ]

    async def _query_related_events(
        self,
        user_id: str,
        keywords: List[str],
        time_window_start: Optional[str] = None,
        time_window_end: Optional[str] = None,
    ) -> List[str]:
        """Query related events based on keyword hits."""
        if not keywords:
            return []

        async with self._driver.session(database=self.database) as session:
            time_filter = ""
            params = {"keywords": keywords, "uid": user_id}
            if time_window_start and time_window_end:
                time_filter = """
                AND (
                    (evt.event_time IS NOT NULL AND evt.event_time >= $start AND evt.event_time <= $end) OR
                    (evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start) AND (evt.record_time <= $end OR evt.created_at <= $end))
                )
                """
                params["start"] = time_window_start
                params["end"] = time_window_end

            result = await session.run(
                f"""
                MATCH (sub:Entity)-[]-(evt:Event)
                WHERE evt.owner_id = $uid AND evt.status = 'active' AND evt.invalid_at IS NULL
                  AND (
                    ANY(kw IN $keywords WHERE coalesce(sub.name, '') CONTAINS kw) OR
                    ANY(kw IN $keywords WHERE coalesce(evt.predicate, '') CONTAINS kw) OR
                    ANY(kw IN $keywords WHERE coalesce(evt.context, '') CONTAINS kw)
                  )
                {time_filter}
                RETURN sub.name AS subject, evt.predicate AS predicate, evt.timestamp_reference AS time_ref
                ORDER BY evt.record_time DESC, evt.created_at DESC
                LIMIT 10
                """,
                **params,
            )
            rows = await result.data()
            return [
                self._fmt_event_line(r.get("subject"), r.get("predicate"), r.get("time_ref"), "往期记录")
                for r in rows
            ]

    async def _query_plan_events(
        self,
        user_id: str,
        time_window_start: Optional[str] = None,
        time_window_end: Optional[str] = None,
    ) -> List[str]:
        """Query active plan events."""
        async with self._driver.session(database=self.database) as session:
            time_filter = ""
            params = {"uid": user_id}
            if time_window_start and time_window_end:
                time_filter = """
                AND (
                    (evt.event_time IS NOT NULL AND evt.event_time >= $start AND evt.event_time <= $end) OR
                    (evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start) AND (evt.record_time <= $end OR evt.created_at <= $end))
                )
                """
                params["start"] = time_window_start
                params["end"] = time_window_end

            result = await session.run(
                f"""
                MATCH (sub:Entity)-[:ACTOR_IN]->(evt:Event)
                WHERE evt.owner_id = $uid AND evt.action_type = 'PLAN' AND evt.status = 'active' AND evt.invalid_at IS NULL
                {time_filter}
                RETURN sub.name AS subject, evt.predicate AS predicate, evt.timestamp_reference AS time_ref
                ORDER BY evt.record_time DESC, evt.created_at DESC
                LIMIT 3
                """,
                **params,
            )
            rows = await result.data()
            return [
                f"- {str(r.get('subject') or '未知主体').strip()} 曾计划：{str(r.get('predicate') or '').strip()}（预计：{str(r.get('time_ref') or '未知').strip()}）"
                for r in rows
            ]

    async def _query_root_identities(self, session: ChatSession):
        """Hydrate user/assistant identity and expose prompt-friendly fields."""
        session.context_canvas["persona_status"] = "active"
        p_manager = None
        try:
            from memory.identity.manager import PersonaManager

            p_manager = PersonaManager()
            user_profile = await p_manager.get_user_profile(session.user_id)
            identity = getattr(user_profile, "identity", {}) or {}

            profile_parts = []
            summary = (
                identity.get("personality_summary")
                or identity.get("biography")
                or identity.get("summary")
                or identity.get("description")
            )
            if summary and str(summary).strip().lower() != "unknown":
                profile_parts.append(str(summary).strip())

            role = str(identity.get("role") or "").strip()
            if role:
                profile_parts.append(f"Role: {role}")

            aliases = [str(a).strip() for a in (identity.get("aliases") or []) if str(a).strip()]
            if aliases:
                profile_parts.append(f"Aliases: {', '.join(aliases)}")

            mbti = str(identity.get("mbti_label") or "").strip().upper()
            if mbti:
                profile_parts.append(f"MBTI: {mbti}")

            bf_parts = []
            bf_map = {
                "openness": identity.get("big_five_openness"),
                "conscientiousness": identity.get("big_five_conscientiousness"),
                "extraversion": identity.get("big_five_extraversion"),
                "agreeableness": identity.get("big_five_agreeableness"),
                "neuroticism": identity.get("big_five_neuroticism"),
            }
            for key, value in bf_map.items():
                if value is None:
                    continue
                try:
                    bf_parts.append(f"{key}={round(float(value), 3)}")
                except (TypeError, ValueError):
                    continue
            if bf_parts:
                profile_parts.append("BigFive: " + ", ".join(bf_parts))

            core_values = [str(v).strip() for v in (identity.get("core_values") or []) if str(v).strip()]
            if core_values:
                profile_parts.append("CoreValues: " + ", ".join(core_values[:8]))

            current_state = str(identity.get("current_state") or "").strip()
            if current_state:
                profile_parts.append(f"State: {current_state}")

            if profile_parts:
                merged_profile = " | ".join(profile_parts)
                session.context_canvas["user_profile"] = merged_profile
                session.context_canvas["user_long_term_profile"] = merged_profile

            user_name = str(identity.get("name") or identity.get("primary_name") or "").strip()
            if user_name and user_name.lower() != "unknown":
                session.context_canvas["user_real_name"] = user_name

            async with self._driver.session(database=self.database) as db_session:
                res = await db_session.run(
                    """
                    MATCH (e:Entity {entity_id: $aid, owner_id: $uid})
                    RETURN e.name AS name,
                           COALESCE(e.persona, e.personality_summary) AS profile,
                           COALESCE(e.role, e.current_role) AS role
                    """,
                    aid=identity_config.assistant_id,
                    uid=session.user_id,
                )
                asst_data = await res.single()
                if asst_data and asst_data.get("name"):
                    session.identity_state = reduce_identity_state(
                        session.identity_state,
                        {"asst_name": asst_data.get("name"), "source": "history"},
                    )
                    current_asst_name = session.identity_state.get("asst_name") or asst_data.get("name")
                    session.context_canvas["assistant_real_name"] = current_asst_name
                    session.context_canvas["assistant_profile"] = (
                        asst_data.get("profile") or identity_config.default_asst_persona
                    )
                    session.context_canvas["assistant_current_role"] = asst_data.get("role") or "全能助理"

            actor = getattr(session, "current_actor", None)
            if actor:
                db_user_name = session.context_canvas.get("user_real_name")
                db_asst_name = session.context_canvas.get("assistant_real_name")
                if db_user_name:
                    actor.speaker_name = db_user_name
                if db_asst_name:
                    actor.target_name = db_asst_name
                session.current_actor = actor
                session.context_canvas["Actor_Identity_Context"] = actor.to_context_string()

        except Exception as e:
            session.context_canvas["persona_status"] = "degraded"
            err = str(e)
            if "Procedure not found" in err or "apoc" in err.lower():
                logger.exception(
                    "[PERSONA_CRITICAL_APOC_MISSING] APOC plugin may be missing; persona loading degraded: %s",
                    err,
                )
            else:
                logger.error("[PERSONA_LOAD_FAILED] %s", err)
        finally:
            if p_manager:
                await p_manager.close()
