"""Event extractor for facts (events/relations) and v3 persona observations."""

import json
import logging
import re
from typing import List, Optional, Tuple

from bridge.llm import LLMBridge
from config import memory_config, memory_llm_config
from memory.event.slots import EntityRelation, FullExtractionResult, MemoryEvent, EventEnvelope, MainEventType
from memory.identity.resolver import Actor

logger = logging.getLogger(__name__)


class EventExtractor:
    """Extract events, entity relations, and v3 persona observations from text."""

    def __init__(self):
        self.bridge = LLMBridge(memory_llm_config, category="memory")
        self.confidence_threshold = memory_config.event_confidence_threshold

        self.instruction_prompt = (
            "You are a structured memory extraction engine. "
            "Return strict JSON that follows the provided schema.\n\n"
            "Track 1 (events): extract factual actions, state changes, plans, achievements.\n"
            "Track 2 (relations): extract durable facts as entity relations.\n"
            "Track 3 (observations): dual-layer persona signals.\n"
            "Track 4 (event_envelopes): extract structured events (finance, health, etc.) with payloads (quantity, unit, currency).\n"
            "Do not emit legacy categories like trait/preference/goal for Track 1-3.\n"
            "If a track has no result, return an empty array for that track."
        )

    @staticmethod
    def detect_reported_target(text: str) -> Optional[str]:
        """Detect reported speech target, e.g. '我跟王哥说: 你...'."""
        raw = str(text or "").strip()
        if not raw:
            return None

        patterns = [
            r"我(?:跟|和)([^，。！？：:；;\s]{1,24})说[：:]?",
            r"我对([^，。！？：:；;\s]{1,24})说[：:]?",
            r"\bI\s+(?:told|said\s+to)\s+([A-Za-z][A-Za-z0-9_-]{0,24})\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = str(match.group(1) or "").strip()
            if not candidate:
                continue
            if candidate.lower() in {"you", "u", "你", "您", "妳"}:
                continue
            return candidate
        return None

    @staticmethod
    def _is_second_person(token: str) -> bool:
        value = str(token or "").strip().lower()
        return value in {"you", "你", "您", "妳"}

    def _bind_reported_you_to_object(self, text: str, result: FullExtractionResult) -> None:
        """Bind second-person pronouns in extracted events to concrete entities."""
        if not result or not result.events:
            return

        target = self.detect_reported_target(text)
        said_to_assistant = bool(
            re.search(r"我(?:对|跟|和)你说", str(text or ""))
            or re.search(r"\bI\s+(?:told|said\s+to)\s+you\b", str(text or ""), flags=re.IGNORECASE)
        )

        for event in result.events:
            if target:
                if self._is_second_person(event.object):
                    event.object = target
                if self._is_second_person(event.subject) and not event.object:
                    event.subject = target
                continue

            if said_to_assistant:
                if self._is_second_person(event.object):
                    event.object = "assistant"
                if self._is_second_person(event.subject) and not event.object:
                    event.subject = "assistant"

    @staticmethod
    def _strip_markdown_json(raw: str) -> str:
        text = str(raw or "").strip()
        if "```" not in text:
            return text
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start : end + 1]
        return text

    async def extract_events_from_text(
        self,
        text: str,
        actor: Actor,
        known_entities: List[str] = None,
        source_msg_id: Optional[int] = None,
    ) -> Tuple[List[MemoryEvent], List[MemoryEvent], List[EntityRelation], list, List[EventEnvelope]]:
        entities = ", ".join(known_entities or []) or "none"
        prompt = (
            "Task: extract events, relations, persona observations, and structured event envelopes from the text.\n"
            "Rules:\n"
            "1) Durable preferences/goals should be represented as relations/facts.\n"
            "2) Observations must only use categories: long_term_persona or short_term_efstb.\n"
            "3) Extract structured envelopes for quantifiable events (spending, health metrics).\n"
            "4) Keep JSON strictly valid and schema-compliant.\n\n"
            f"Known entities: {entities}\n"
            f"Input text: {text}"
        )

        try:
            schema_json = FullExtractionResult.model_json_schema()
            messages = [
                {
                    "role": "system",
                    "content": self.instruction_prompt + "\n\nJSON Schema: " + json.dumps(schema_json, ensure_ascii=False),
                },
                {"role": "user", "content": prompt},
            ]
            json_str = await self.bridge.chat_completion(
                messages=messages,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            if not json_str:
                return [], [], [], [], []

            parsed_raw = json.loads(self._strip_markdown_json(json_str))

            # Compatibility: relation -> relation_type
            for relation in parsed_raw.get("relations", []) or []:
                if "relation" in relation and "relation_type" not in relation:
                    relation["relation_type"] = relation.pop("relation")

            result = FullExtractionResult.model_validate(parsed_raw)
            self._bind_reported_you_to_object(text, result)

            valid_events: List[MemoryEvent] = []
            candidate_events: List[MemoryEvent] = []

            for event in result.events:
                raw_source = event.source_entity
                if raw_source and raw_source != actor.speaker_name:
                    event.event_metadata = event.event_metadata or {}
                    event.event_metadata["reported_source"] = raw_source

                event.source_entity = actor.speaker_name
                event.trust_score = 1.0
                event.source_msg_id = source_msg_id

                if (event.confidence or 0.0) >= self.confidence_threshold:
                    valid_events.append(event)
                else:
                    candidate_events.append(event)

            relations: List[EntityRelation] = []
            for rel in result.relations:
                rel.source_entity = actor.speaker_name
                rel.source_msg_id = source_msg_id
                if (rel.confidence or 0.0) >= self.confidence_threshold:
                    relations.append(rel)

            final_observations = []
            for obs in result.observations:
                obs.source_msg_id = source_msg_id
                final_observations.append(obs)

            # Process Envelopes
            valid_envelopes: List[EventEnvelope] = []
            for env in result.event_envelopes:
                env.source_msg_id = source_msg_id
                if (env.confidence or 0.0) >= self.confidence_threshold:
                    valid_envelopes.append(env)

            return valid_events, candidate_events, relations, final_observations, valid_envelopes

        except Exception as exc:
            logger.error("[EventExtractor] extraction failed: %s", exc)
            return [], [], [], [], []
