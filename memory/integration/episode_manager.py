"""
Episode manager.
"""
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from bridge.llm import LLMBridge
from config import memory_llm_config
from memory.event.slots import MemoryEpisode

logger = logging.getLogger(__name__)


class EpisodeManager:
    """Build episode summaries and lightweight persona hints from recent turns."""

    def __init__(self):
        self.bridge = LLMBridge(memory_llm_config, category="memory")

    async def extract_episode(self, messages_buffer: List[Dict], events_buffer: List[str]) -> Optional[MemoryEpisode]:
        if not messages_buffer:
            return None

        prompt = (
            "你是一个记忆叙事专家。请总结以下对话片段。\n"
            "【提取要求】:\n"
            "1. name: 给这段对话起一个切中核心的 3-8 字短名。\n"
            "2. summary: 提供一句 50 字内的摘要。\n"
            "3. behavior: 给出短期行为态 EFSTB 分数，包括 urgency_level、instruction_compliance、granularity_preference(low/medium/high)、logic_vs_emotion。\n"
            "4. persona: 如果对话中出现稳定人格线索，给出 mbti_hint、core_values_hint(0-3 个短语)、big_five_observed(五维 0-1，可部分缺省)。\n\n"
            "【对话回放】\n"
        )

        msg_ids: List[int] = []
        start_time = None
        end_time = None

        for message in messages_buffer:
            role = "AI" if message["role"] == "assistant" else "User"
            prompt += f"[{role}]: {message['content']}\n"
            mid = message.get("id") or message.get("msg_id")
            if mid:
                msg_ids.append(mid)
            ts = message.get("timestamp")
            if ts:
                if not start_time:
                    start_time = ts
                end_time = ts

        try:
            light_schema = {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "summary": {"type": "string"},
                    "urgency_level": {"type": "number"},
                    "instruction_compliance": {"type": "number"},
                    "granularity_preference": {"type": "string"},
                    "logic_vs_emotion": {"type": "number"},
                    "mbti_hint": {"type": "string"},
                    "core_values_hint": {"type": "array", "items": {"type": "string"}},
                    "big_five_observed": {
                        "type": "object",
                        "properties": {
                            "openness": {"type": "number"},
                            "conscientiousness": {"type": "number"},
                            "extraversion": {"type": "number"},
                            "agreeableness": {"type": "number"},
                            "neuroticism": {"type": "number"},
                        },
                    },
                },
                "required": ["name", "summary"],
            }

            messages = [
                {"role": "system", "content": f"输出请严格满足以下 JSON 格式:\n{json.dumps(light_schema, ensure_ascii=False)}"},
                {"role": "user", "content": prompt},
            ]

            logger.info("[EpisodeManager] Triggering summary for %s messages...", len(messages_buffer))
            json_str = await self.bridge.chat_completion(
                messages=messages,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            if not json_str:
                return None

            json_str = json_str.strip()
            if json_str.startswith("```"):
                start = json_str.find("{")
                end = json_str.rfind("}")
                if start != -1 and end != -1:
                    json_str = json_str[start : end + 1]

            parsed = json.loads(json_str)
            observed = parsed.get("big_five_observed") or {}
            clean_observed = {}
            for key, value in observed.items():
                try:
                    clean_observed[key] = max(0.0, min(1.0, float(value)))
                except (TypeError, ValueError):
                    continue

            return MemoryEpisode(
                episode_id=str(uuid.uuid4()),
                name=parsed.get("name", "未命名片段"),
                summary=parsed.get("summary", "无摘要"),
                start_time=start_time or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                end_time=end_time or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                associated_events=events_buffer,
                evidence_msg_ids=msg_ids,
                efstb_tags={
                    "urgency_level": parsed.get("urgency_level", 0.5),
                    "instruction_compliance": parsed.get("instruction_compliance", 0.5),
                    "granularity_preference": parsed.get("granularity_preference", "medium"),
                    "logic_vs_emotion": parsed.get("logic_vs_emotion", 0.5),
                },
                mbti_hint=(parsed.get("mbti_hint") or "").strip() or None,
                big_five_observed=clean_observed,
                core_values_hint=[str(v).strip() for v in (parsed.get("core_values_hint") or []) if str(v).strip()][:3],
            )
        except Exception as exc:
            logger.error("[EpisodeManager] Episode extraction failed: %s", exc)
            return None
