"""
Normalization Engine for Structured Memory Events (v2)
Handles rule-based and LLM-assisted normalization of units, currencies, and timestamps.
"""
import re
import logging
import dateparser
import datetime
import json
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Tuple

from memory.event.slots import EventEnvelope, MainEventType, TypedPayload, NormalizationMeta, MemoryEvent
from config import memory_config, memory_llm_config
from bridge.llm import LLMBridge

logger = logging.getLogger(__name__)

class NormalizationEngine:
    """Rule-based normalization for financial and physical units."""

    def __init__(self):
        self.default_currency = memory_config.default_currency
        self.precision = memory_config.decimal_precision

    def normalize_envelope(self, env: EventEnvelope) -> EventEnvelope:
        """Apply all normalization rules to an event envelope."""
        if not env.payload:
            return env

        rules_applied = []
        
        # 1. Currency Normalization
        if env.main_type == MainEventType.FINANCE or env.payload.currency_source:
            self._normalize_currency(env, rules_applied)

        # 2. Unit Normalization
        if env.payload.quantity_unit:
            self._normalize_units(env, rules_applied)

        # 3. Magnitude Scaling
        if env.payload.quantity is not None:
            env.payload.quantity = self._quantize(env.payload.quantity)
        if env.payload.amount is not None:
            env.payload.amount = self._quantize(env.payload.amount)

        if rules_applied:
            env.normalization.rules_applied.extend(rules_applied)
            env.normalization.method = "rule"
            
        return env

    def _normalize_currency(self, env: EventEnvelope, rules: List[str]):
        source = env.payload.currency_source or env.payload.original_text or ""
        if not source: return

        # Explicit regex patterns
        patterns = [
            (r"(\d+(\.\d+)?)\s*(元|块|人民币|cny|rmb)", "CNY"),
            (r"(\d+(\.\d+)?)\s*(刀|dollars?|usd|\$)", "USD"),
            (r"(\d+(\.\d+)?)\s*(港币|hkd)", "HKD"),
            (r"(\d+(\.\d+)?)\s*(日元|jpy|yen)", "JPY"),
            (r"(\d+(\.\d+)?)\s*(欧元|eur)", "EUR"),
        ]

        found = False
        for pattern, currency in patterns:
            match = re.search(pattern, source, re.IGNORECASE)
            if match:
                env.payload.currency = currency
                rules.append(f"currency_match_{currency}")
                found = True
                break
        
        if "五毛" in source:
            env.payload.amount = Decimal("0.5")
            env.payload.currency = "CNY"
            rules.append("slang_wumao")
            found = True

        if not env.payload.currency:
            if any(c in source for c in "元块￥"):
                env.payload.currency = "CNY"
                rules.append("currency_guess_cny")
            elif any(c in source for c in "$"):
                env.payload.currency = "USD"
                rules.append("currency_guess_usd")
            else:
                env.payload.currency = self.default_currency
                rules.append("currency_default")

        # Ensure amount is set if quantity was extracted as money
        if env.main_type == MainEventType.FINANCE and env.payload.amount is None and env.payload.quantity is not None:
            env.payload.amount = env.payload.quantity
            rules.append("copy_quantity_to_amount")

    def _normalize_units(self, env: EventEnvelope, rules: List[str]):
        unit = env.payload.quantity_unit.lower().strip()
        val = env.payload.quantity
        if val is None: return

        # Weight: 斤 -> kg (0.5)
        if unit in ["斤", "市斤"]:
            env.payload.quantity = val * Decimal("0.5")
            env.payload.quantity_unit = "kg"
            rules.append("unit_jin_to_kg")
        
        # Weight: lb/pound -> kg (0.4535)
        elif unit in ["lb", "lbs", "pound", "pounds", "磅"]:
            env.payload.quantity = val * Decimal("0.4536")
            env.payload.quantity_unit = "kg"
            rules.append("unit_lb_to_kg")
        
        # Time: h/hour -> min (60)
        elif unit in ["h", "hr", "hour", "hours", "小时"]:
            env.payload.quantity = val * Decimal("60")
            env.payload.quantity_unit = "min"
            rules.append("unit_hour_to_min")

    def _quantize(self, val: Decimal) -> Decimal:
        return val.quantize(Decimal(f"1.{'0' * self.precision}"), rounding=ROUND_HALF_UP)


class ContentNormalizerAgent:
    """Legacy and v2 combined normalizer."""
    def __init__(self):
        self.bridge = LLMBridge(memory_llm_config, category="memory")
        self.engine = NormalizationEngine()
        
    async def run_pipeline(self, events: List[MemoryEvent]) -> List[MemoryEvent]:
        # Legacy pipeline for MemoryEvent
        for event in events:
            self._normalize_temporal_anchor(event)
            if event.event_metadata:
                await self._agentic_metadata_cleanup(event)
        return events

    def normalize_envelopes(self, envelopes: List[EventEnvelope]) -> List[EventEnvelope]:
        return [self.engine.normalize_envelope(env) for env in envelopes]

    def _normalize_temporal_anchor(self, event: MemoryEvent):
        if not event.timestamp_reference: return
        try:
            current_time = datetime.datetime.now()
            parsed_date = dateparser.parse(
                event.timestamp_reference, 
                settings={'TIMEZONE': 'Asia/Shanghai', 'RELATIVE_BASE': current_time}
            )
            if parsed_date:
                time_keywords = ['点', '分', '时', ':', 'am', 'pm', 'AM', 'PM', '早', '中', '晚', '夜', '凌晨']
                if any(k in event.timestamp_reference for k in time_keywords):
                    iso_stamp = parsed_date.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    iso_stamp = parsed_date.strftime('%Y-%m-%d')
                event.timestamp_reference = f"{iso_stamp} (原词: {event.timestamp_reference})"
        except Exception as e:
            logger.warning(f"[Tool: 时间锚定器] 解析异常: {e}")

    async def _agentic_metadata_cleanup(self, event: MemoryEvent):
        original_meta = json.dumps(event.event_metadata, ensure_ascii=False)
        schema = {
            "type": "object",
            "properties": {
                "cleaned_metadata": {"type": "object"}
            },
            "required": ["cleaned_metadata"]
        }
        prompt = (
            "你是一个工业级数据清洗专家。\n"
            "清洗法则：\n"
            "1. 【货币】一律添加 'currency'='CNY'，主面额提取为纯数字 amount。\n"
            "2. 【度量衡】清洗并体现到键名上 (如 weight_kg: 65)。\n"
            "待清洗 Metadata JSON:\n{original_meta}"
        )
        json_str = await self.bridge.chat_completion(
            messages=[
                {"role": "system", "content": "Return ONLY JSON matching schema: " + json.dumps(schema)},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        if json_str:
            try:
                result = json.loads(json_str)
                if "cleaned_metadata" in result:
                    event.event_metadata = result["cleaned_metadata"]
            except (json.JSONDecodeError, TypeError) as exc:
                logger.debug("[MetadataNormalizer] Invalid cleanup JSON ignored: %s", exc)
