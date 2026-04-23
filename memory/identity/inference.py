"""Identity inference engine for MBTI + Big Five."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from bridge.llm import LLMBridge

logger = logging.getLogger(__name__)

VALID_MBTI_LABELS = {
    "INTJ", "INTP", "INFJ", "INFP", "ISTJ", "ISTP", "ISFJ", "ISFP",
    "ENTJ", "ENTP", "ENFJ", "ENFP", "ESTJ", "ESTP", "ESFJ", "ESFP",
    "I", "E",
}

BIG_FIVE_DIMS = ("openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism")


class IdentityJudgment(BaseModel):
    """Structured identity judgment."""

    mbti_label: str = Field(..., description="MBTI label")
    big_five: Dict[str, float] = Field(..., description="Big Five scores in [0.0, 1.0]")
    reasoning: str = Field(..., description="Short evidence-grounded reasoning")
    references: List[str] = Field(default_factory=list, description="Evidence element IDs used")


class PersonaInferenceService:
    def __init__(self, driver, database: str = "neo4j"):
        self._driver = driver
        self.database = database
        from config import llm_config

        self.llm = LLMBridge(llm_config, category="persona_inference")

    @staticmethod
    def _clamp01(value: Any, default: float = 0.5) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return default

    async def run_judgment(
        self,
        uid: str,
        evidence_refs: Optional[List[str]] = None,
        audit_summary: Optional[Dict[str, Any]] = None,
    ) -> Optional[IdentityJudgment]:
        """Infer identity from settled personality evidence."""
        facts = await self._fetch_personality_evidence(uid, evidence_refs=evidence_refs)
        if not facts:
            logger.info("[Inference] No personality evidence found for user %s, skipped.", uid)
            return None

        facts_summary = self._format_facts_for_llm(facts)
        prompt = self._build_judgment_prompt(facts_summary, audit_summary=audit_summary)
        logger.info("[Inference] Running LLM judgment for user %s with %d facts.", uid, len(facts))

        response_text = await self.llm.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )

        known_refs = {str(f.get("uuid") or "").strip() for f in facts if str(f.get("uuid") or "").strip()}
        fallback = self._fallback_judgment_from_facts(facts)

        if not response_text:
            logger.warning("[Inference] LLM unavailable, using deterministic fallback judgment.")
            return fallback

        try:
            data = json.loads(str(response_text).strip())
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.error("[Inference] Failed to parse LLM judgment: %s", exc)
            return fallback

        return self._sanitize_judgment(data, known_refs=known_refs, fallback=fallback)

    async def _fetch_personality_evidence(
        self,
        uid: str,
        evidence_refs: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        refs = [str(ref).strip() for ref in (evidence_refs or []) if str(ref).strip()]
        async with self._driver.session(database=self.database) as session:
            if refs:
                result = await session.run(
                    """
                    MATCH (e:Entity {entity_id: $uid})-[:HAS_FACT]->(f:PersonalityEvidence)
                    WHERE elementId(f) IN $refs
                    RETURN
                        elementId(f) AS uuid,
                        f.predicate AS predicate,
                        f.object AS object,
                        f.evidence AS evidence,
                        coalesce(f.confidence, 0.5) AS confidence,
                        coalesce(f.updated_at, f.created_at, '') AS time,
                        coalesce(f.source, '') AS source,
                        coalesce(f.category, '') AS category
                    ORDER BY coalesce(f.updated_at, f.created_at) DESC
                    """,
                    uid=uid,
                    refs=refs,
                )
                rows = [record.data() async for record in result]
                if rows:
                    return rows

            result = await session.run(
                """
                MATCH (e:Entity {entity_id: $uid})-[:HAS_FACT]->(f:PersonalityEvidence)
                RETURN
                    elementId(f) AS uuid,
                    f.predicate AS predicate,
                    f.object AS object,
                    f.evidence AS evidence,
                    coalesce(f.confidence, 0.5) AS confidence,
                    coalesce(f.updated_at, f.created_at, '') AS time,
                    coalesce(f.source, '') AS source,
                    coalesce(f.category, '') AS category
                ORDER BY coalesce(f.updated_at, f.created_at) DESC
                LIMIT 200
                """,
                uid=uid,
            )
            return [record.data() async for record in result]

    @staticmethod
    def _format_facts_for_llm(facts: List[Dict[str, Any]]) -> str:
        lines = []
        for i, fact in enumerate(facts):
            lines.append(
                f"[{i}] id={fact.get('uuid')} | predicate={fact.get('predicate')} | "
                f"object={fact.get('object')} | confidence={fact.get('confidence')} | "
                f"source={fact.get('source')} | evidence={fact.get('evidence')}"
            )
        return "\n".join(lines)

    def _build_judgment_prompt(self, facts_summary: str, audit_summary: Optional[Dict[str, Any]] = None) -> str:
        audit_text = ""
        if isinstance(audit_summary, dict) and audit_summary:
            brief = {
                "quality_index": audit_summary.get("quality_index"),
                "conflict_groups": audit_summary.get("conflict_groups"),
                "disputed_predicates": audit_summary.get("disputed_predicates"),
                "selected_count": audit_summary.get("selected_count"),
            }
            audit_text = "\nAudit summary:\n" + json.dumps(brief, ensure_ascii=False)

        return (
            "You are an identity inference engine. Infer MBTI and Big Five from evidence.\n"
            "Prioritize behavioral consistency over self-claims when they conflict.\n"
            "Return strict JSON with keys: mbti_label, big_five, reasoning, references.\n"
            f"{audit_text}\n\n"
            "Evidence:\n"
            f"{facts_summary}\n\n"
            "Output format:\n"
            "{\n"
            '  "mbti_label": "XXXX",\n'
            '  "big_five": {\n'
            '    "openness": 0.0,\n'
            '    "conscientiousness": 0.0,\n'
            '    "extraversion": 0.0,\n'
            '    "agreeableness": 0.0,\n'
            '    "neuroticism": 0.0\n'
            "  },\n"
            '  "reasoning": "short explanation",\n'
            '  "references": ["id1", "id2"]\n'
            "}"
        )

    def _fallback_judgment_from_facts(self, facts: List[Dict[str, Any]]) -> IdentityJudgment:
        weighted_values: Dict[str, List[tuple[float, float]]] = {dim: [] for dim in BIG_FIVE_DIMS}
        mbti_scores: Dict[str, float] = {}

        for fact in facts:
            predicate = str(fact.get("predicate") or "").strip().lower()
            obj = str(fact.get("object") or "").strip()
            confidence = self._clamp01(fact.get("confidence"), default=0.5)

            if predicate.startswith("big_five_"):
                dim = predicate.replace("big_five_", "", 1).strip().lower()
                if dim in weighted_values:
                    try:
                        weighted_values[dim].append((float(obj), confidence))
                    except (TypeError, ValueError):
                        pass
                continue

            if predicate == "mbti_label":
                label = obj.upper()
                if label in VALID_MBTI_LABELS:
                    mbti_scores[label] = mbti_scores.get(label, 0.0) + confidence

        big_five: Dict[str, float] = {}
        for dim in BIG_FIVE_DIMS:
            pairs = weighted_values.get(dim) or []
            if not pairs:
                big_five[dim] = 0.5
                continue
            total_w = sum(max(weight, 1e-6) for _, weight in pairs)
            avg = sum(value * max(weight, 1e-6) for value, weight in pairs) / max(total_w, 1e-6)
            big_five[dim] = self._clamp01(avg, default=0.5)

        if mbti_scores:
            mbti = max(mbti_scores.items(), key=lambda item: item[1])[0]
        else:
            mbti = "E" if big_five["extraversion"] >= 0.55 else "I"

        refs = [str(f.get("uuid") or "").strip() for f in facts if str(f.get("uuid") or "").strip()][:8]
        reasoning = "[Fallback] Deterministic aggregation used (weighted Big Five + MBTI evidence voting)."
        return IdentityJudgment(
            mbti_label=mbti,
            big_five=big_five,
            reasoning=reasoning,
            references=refs,
        )

    def _sanitize_judgment(
        self,
        data: Dict[str, Any],
        known_refs: set[str],
        fallback: IdentityJudgment,
    ) -> IdentityJudgment:
        raw_mbti = str(data.get("mbti_label") or "").strip().upper()
        mbti = raw_mbti if raw_mbti in VALID_MBTI_LABELS else fallback.mbti_label

        raw_bf = data.get("big_five") or {}
        big_five: Dict[str, float] = {}
        for dim in BIG_FIVE_DIMS:
            if isinstance(raw_bf, dict):
                big_five[dim] = self._clamp01(raw_bf.get(dim), default=fallback.big_five.get(dim, 0.5))
            else:
                big_five[dim] = fallback.big_five.get(dim, 0.5)

        reasoning = str(data.get("reasoning") or "").strip() or fallback.reasoning
        raw_refs = data.get("references") or []
        refs = []
        if isinstance(raw_refs, list):
            for ref in raw_refs:
                key = str(ref or "").strip()
                if key and key in known_refs and key not in refs:
                    refs.append(key)
        if not refs:
            refs = [ref for ref in fallback.references if ref in known_refs][:8] or fallback.references[:8]

        return IdentityJudgment(
            mbti_label=mbti,
            big_five=big_five,
            reasoning=reasoning,
            references=refs,
        )
