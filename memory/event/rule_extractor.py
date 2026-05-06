"""Deterministic structured-event extraction for high-confidence facts.

The rule layer is a reliability floor, not a full semantic parser. It captures
facts that should survive even when LLM extraction returns nothing: money,
inventory/resource deltas, measurements, dated plans, tasks, and strong
preference/avoidance statements.
"""
from __future__ import annotations

import re
from decimal import Decimal
from typing import List, Optional

from memory.event.slots import EventEnvelope, MainEventType, NormalizationMeta, TypedPayload
from memory.event.temporal import resolve as _resolve_temporal


_MONEY_RE = re.compile(
    r"(?P<prefix>[¥￥$]?)\s*(?P<amount>\d+(?:\.\d+)?)\s*(?P<currency>元|块|人民币|rmb|cny|美元|usd|刀|美金|港币|hkd|日元|jpy|欧元|eur)?",
    re.IGNORECASE,
)
_SPEND_WORDS = ("花", "买", "消费", "支付", "付了", "支出", "用了", "花费", "付款", "下单", "点了")
_INCOME_WORDS = ("收入", "赚了", "到账", "收款", "收到", "进账", "报销到账", "工资", "奖金")
_REFUND_WORDS = ("退款", "退了", "退回", "退还", "返现")
_DEBT_WORDS = ("借给", "借了", "欠", "还款", "垫付")
_NEGATIVE_WORDS = ("难喝", "不好喝", "不好", "不佳", "差", "失望", "避雷", "难吃", "糟糕", "不值")
_POSITIVE_WORDS = ("好喝", "好吃", "不错", "满意", "喜欢", "推荐", "值得", "划算")

_RESOURCE_UNITS = (
    "包", "件", "把", "石", "箱", "袋", "瓶", "桶", "辆", "匹", "只", "支", "套", "个",
    "kg", "公斤", "斤", "吨", "升", "发", "枚", "units",
)
_RESOURCE_HINTS = (
    "物资", "补给", "库存", "军械", "马匹", "战马", "粮草", "粮食", "药材", "木材",
    "铁矿", "布匹", "燃料", "弹药", "武器", "装备", "弓箭", "箭", "军需", "辎重",
)
_RESOURCE_GAIN = ("运回", "缴获", "获得", "收到", "补给", "入库", "到货", "采购", "买入", "增加", "存入", "补充")
_RESOURCE_LOSS = ("消耗", "损耗", "损失", "用掉", "出库", "折损", "减少", "花掉", "耗费", "阵亡", "丢失")

_HEALTH_PATTERNS = [
    (re.compile(r"(?:体重|重)\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>kg|公斤|斤)"), "weight", "体重"),
    (re.compile(r"(?:身高|高)\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>cm|厘米|米|m)"), "height", "身高"),
    (re.compile(r"(?:心率|心跳)\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>bpm|次/分|次每分)?"), "heart_rate", "心率"),
    (re.compile(r"血压\s*(?P<sys>\d{2,3})\s*/\s*(?P<dia>\d{2,3})"), "blood_pressure", "血压"),
    (re.compile(r"体温\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>度|℃|摄氏度)?"), "temperature", "体温"),
]
_SYMPTOM_WORDS = ("头疼", "头痛", "发烧", "咳嗽", "胃疼", "失眠", "嗓子疼", "恶心", "腹泻", "过敏")
_MEDICATION_WORDS = ("吃了", "服用", "用了", "打了", "涂了")

_TEMPORAL_RE = re.compile(
    r"(第\s*(?P<narrative_day>\d+)\s*天|day\s*(?P<day>\d+)|今天|明天|后天|大后天|昨天|前天|"
    r"\d{1,2}\s*月\s*\d{1,2}\s*[号日]?那几天|\d{1,2}\s*月\s*(?:初|中|末|上旬|中旬|下旬)(?:那几天)?|"
    r"\d{1,2}\s*月\s*\d{1,2}\s*[号日]?|周[一二三四五六日天]|星期[一二三四五六日天]|"
    r"\d+\s*天后|\d+\s*天前|前几天|这几天|那几天|最近)"
    r"(?:\s*(?:上午|中午|下午|晚上|早上|凌晨)?\s*\d{1,2}(?::\d{2})?\s*(?:点|:)?(?:\d{1,2}分)?)?",
    re.IGNORECASE,
)
_TASK_WORDS = ("提醒", "记得", "待办", "todo", "要做", "需要", "别忘", "截止", "deadline")
_PLAN_WORDS = ("计划", "打算", "准备", "安排", "预约", "约了", "要去", "要买", "要见", "开会")
_PREFERENCE_WORDS = ("喜欢", "不喜欢", "讨厌", "避雷", "推荐", "不推荐", "下次还", "以后不想再", "以后不", "不想再")


class RuleBasedStructuredExtractor:
    """Extract high-precision EventEnvelope rows from a single user message."""

    def extract(
        self,
        text: str,
        *,
        actor_name: Optional[str] = None,
        source_msg_id: Optional[int] = None,
        source_timestamp: Optional[str] = None,
    ) -> List[EventEnvelope]:
        raw = str(text or "").strip()
        if not raw:
            return []
        subject = actor_name or "user"
        events: List[EventEnvelope] = []
        temporal = self._temporal_metadata(raw)

        resolved_dt, precision, time_part = _resolve_temporal(raw, source_timestamp)
        source_timestamp = resolved_dt.isoformat()
        temporal["event_time_precision"] = precision
        if time_part:
            temporal["time_part"] = time_part

        events.extend(self._extract_finance(raw, subject, source_msg_id, source_timestamp, temporal))
        events.extend(self._extract_resources(raw, subject, source_msg_id, source_timestamp, temporal))
        events.extend(self._extract_health(raw, subject, source_msg_id, source_timestamp, temporal))
        events.extend(self._extract_schedule_or_task(raw, subject, source_msg_id, source_timestamp, temporal))
        events.extend(self._extract_preference(raw, subject, source_msg_id, source_timestamp, temporal))
        return events

    def _extract_finance(
        self,
        text: str,
        subject: str,
        source_msg_id: Optional[int],
        source_timestamp: Optional[str],
        temporal: dict,
    ) -> List[EventEnvelope]:
        has_finance_word = any(word in text for word in (_SPEND_WORDS + _INCOME_WORDS + _REFUND_WORDS + _DEBT_WORDS))
        if not has_finance_word and not re.search(r"[¥￥$]\s*\d", text):
            return []
        out: List[EventEnvelope] = []
        for match in _MONEY_RE.finditer(text):
            if not (match.group("currency") or match.group("prefix")):
                continue
            amount = Decimal(match.group("amount"))
            currency_source = match.group("currency") or match.group("prefix")
            subtype, predicate = self._finance_action(text)
            obj = self._infer_purchase_object(text, match.start()) or self._infer_tail_object(text) or "消费"
            metadata = {
                "rule_extracted": True,
                "rule": "finance_amount",
                "original_text": text,
                **temporal,
            }
            if any(word in text for word in _NEGATIVE_WORDS):
                metadata["experience"] = "negative"
            elif any(word in text for word in _POSITIVE_WORDS):
                metadata["experience"] = "positive"
            out.append(
                EventEnvelope(
                    main_type=MainEventType.FINANCE,
                    subtype=subtype,
                    event_time=source_timestamp,
                    event_time_precision=temporal.get("event_time_precision"),
                    subject=subject,
                    predicate=predicate,
                    object=obj,
                    payload=TypedPayload(amount=amount, currency_source=currency_source, original_text=text),
                    normalization=NormalizationMeta(method="rule", rules_applied=["rule_finance_amount"]),
                    confidence=1.0,
                    source_msg_id=source_msg_id,
                    metadata=metadata,
                )
            )
        return out

    def _extract_resources(
        self,
        text: str,
        subject: str,
        source_msg_id: Optional[int],
        source_timestamp: Optional[str],
        temporal: dict,
    ) -> List[EventEnvelope]:
        if not any(hint in text for hint in _RESOURCE_HINTS) and not any(unit in text for unit in _RESOURCE_UNITS):
            return []
        unit_alt = "|".join(re.escape(unit) for unit in _RESOURCE_UNITS)
        pattern = re.compile(r"(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>" + unit_alt + r")\s*(?P<object>[\u4e00-\u9fffA-Za-z0-9_]{1,12})?")
        out: List[EventEnvelope] = []
        is_loss = any(word in text for word in _RESOURCE_LOSS)
        is_gain = any(word in text for word in _RESOURCE_GAIN)
        if not is_loss and not is_gain and not any(hint in text for hint in _RESOURCE_HINTS):
            return []
        for match in pattern.finditer(text):
            obj = self._infer_resource_object(text, match) or "物资"
            subtype = "expenditure" if is_loss else "acquisition"
            predicate = "consume" if is_loss else "acquire"
            out.append(
                EventEnvelope(
                    main_type=MainEventType.RESOURCE,
                    subtype=subtype,
                    event_time=source_timestamp,
                    event_time_precision=temporal.get("event_time_precision"),
                    subject=subject,
                    predicate=predicate,
                    object=obj,
                    payload=TypedPayload(
                        quantity=Decimal(match.group("qty")),
                        quantity_unit=match.group("unit"),
                        original_text=text,
                    ),
                    normalization=NormalizationMeta(method="rule", rules_applied=["rule_resource_quantity"]),
                    confidence=1.0,
                    source_msg_id=source_msg_id,
                    metadata={"rule_extracted": True, "rule": "resource_quantity", "original_text": text, **temporal},
                )
            )
        return out

    def _extract_health(
        self,
        text: str,
        subject: str,
        source_msg_id: Optional[int],
        source_timestamp: Optional[str],
        temporal: dict,
    ) -> List[EventEnvelope]:
        out: List[EventEnvelope] = []
        for pattern, subtype, obj in _HEALTH_PATTERNS:
            for match in pattern.finditer(text):
                if subtype == "blood_pressure":
                    quantity = Decimal(match.group("sys"))
                    unit = f"/{match.group('dia')}mmHg"
                else:
                    quantity = Decimal(match.group("value"))
                    unit = match.groupdict().get("unit") or ("bpm" if subtype == "heart_rate" else "")
                out.append(
                    EventEnvelope(
                        main_type=MainEventType.HEALTH,
                        subtype=subtype,
                        event_time=source_timestamp,
                        event_time_precision=temporal.get("event_time_precision"),
                        subject=subject,
                        predicate="measure",
                        object=obj,
                        payload=TypedPayload(quantity=quantity, quantity_unit=unit, original_text=text),
                        normalization=NormalizationMeta(method="rule", rules_applied=["rule_health_measurement"]),
                        confidence=1.0,
                        source_msg_id=source_msg_id,
                        metadata={"rule_extracted": True, "rule": "health_measurement", "original_text": text, **temporal},
                    )
                )
        for symptom in _SYMPTOM_WORDS:
            if symptom in text:
                out.append(self._simple_event(MainEventType.HEALTH, "symptom", "has_symptom", symptom, text, subject, source_msg_id, source_timestamp, "health_symptom", temporal))
        medication = self._extract_after_words(text, _MEDICATION_WORDS)
        if medication and any(word in text for word in _MEDICATION_WORDS):
            out.append(self._simple_event(MainEventType.HEALTH, "medication", "take", medication, text, subject, source_msg_id, source_timestamp, "health_medication", temporal))
        return out

    def _extract_schedule_or_task(
        self,
        text: str,
        subject: str,
        source_msg_id: Optional[int],
        source_timestamp: Optional[str],
        temporal: dict,
    ) -> List[EventEnvelope]:
        if not temporal and not any(word in text for word in (_TASK_WORDS + _PLAN_WORDS)):
            return []
        out: List[EventEnvelope] = []
        target = self._strip_temporal_prefix(self._infer_tail_object(text) or text)
        if any(word in text for word in _TASK_WORDS):
            out.append(self._simple_event(MainEventType.TASK, "todo", "need_do", target, text, subject, source_msg_id, source_timestamp, "task_or_reminder", temporal))
        elif any(word in text for word in _PLAN_WORDS):
            out.append(self._simple_event(MainEventType.SCHEDULE, "plan", "scheduled", target, text, subject, source_msg_id, source_timestamp, "dated_plan", temporal))
        return out

    def _extract_preference(
        self,
        text: str,
        subject: str,
        source_msg_id: Optional[int],
        source_timestamp: Optional[str],
        temporal: dict,
    ) -> List[EventEnvelope]:
        if not any(word in text for word in _PREFERENCE_WORDS + _NEGATIVE_WORDS + _POSITIVE_WORDS):
            return []
        obj = self._extract_after_words(text, _PREFERENCE_WORDS) or self._infer_purchase_object(text, len(text)) or self._infer_tail_object(text)
        if not obj:
            return []
        sentiment = "negative" if any(word in text for word in _NEGATIVE_WORDS) or any(word in text for word in ("不喜欢", "讨厌", "避雷", "不推荐", "以后不", "不想再")) else "positive"
        event = self._simple_event(MainEventType.OPINION, "preference", "dislike" if sentiment == "negative" else "like", obj, text, subject, source_msg_id, source_timestamp, "preference_signal", temporal)
        event.metadata["sentiment"] = sentiment
        return [event]

    def _simple_event(
        self,
        main_type: MainEventType,
        subtype: str,
        predicate: str,
        obj: str,
        text: str,
        subject: str,
        source_msg_id: Optional[int],
        source_timestamp: Optional[str],
        rule: str,
        temporal: dict,
    ) -> EventEnvelope:
        return EventEnvelope(
            main_type=main_type,
            subtype=subtype,
            event_time=source_timestamp,
            event_time_precision=temporal.get("event_time_precision"),
            subject=subject,
            predicate=predicate,
            object=self._clean_object(obj) or obj,
            payload=TypedPayload(original_text=text),
            normalization=NormalizationMeta(method="rule", rules_applied=[f"rule_{rule}"]),
            confidence=1.0,
            source_msg_id=source_msg_id,
            metadata={"rule_extracted": True, "rule": rule, "original_text": text, **temporal},
        )

    def _finance_action(self, text: str) -> tuple[str, str]:
        if any(word in text for word in _INCOME_WORDS):
            return "income", "receive"
        if any(word in text for word in _REFUND_WORDS):
            return "refund", "refund"
        if any(word in text for word in _DEBT_WORDS):
            return "debt", "debt"
        return "purchase", "spend"

    def _infer_purchase_object(self, text: str, amount_pos: int) -> Optional[str]:
        prefix = text[:amount_pos]
        matches = re.findall(r"(?:买了?|购买了?|点了?|喝了?|吃了?|下单了?)([^，,。；;]{1,30})(?:，|,|。|花|消费|支付|付|用了|$)", prefix)
        if matches:
            return self._clean_object(matches[-1])
        m = re.search(r"(?:买了?|购买了?|点了?|喝了?|吃了?|下单了?)(?P<object>[^，,。；;]{1,30})$", prefix)
        if m:
            return self._clean_object(m.group("object"))
        m = re.search(r"(?P<object>[^，,。；;]{1,20})$", prefix)
        if m and any(word in prefix for word in ("买", "购买", "点")):
            return self._clean_object(m.group("object"))
        return None

    def _infer_resource_object(self, text: str, match: "re.Match[str]") -> Optional[str]:
        after = self._clean_object(match.group("object") or "")
        if after and after not in {"人", "的"}:
            return after
        prefix = text[: match.start()]
        m = re.search(r"([\u4e00-\u9fffA-Za-z0-9_]{1,12})$", prefix)
        if m:
            candidate = self._clean_object(m.group(1))
            if candidate and candidate not in _RESOURCE_GAIN and candidate not in _RESOURCE_LOSS:
                return candidate
        for hint in _RESOURCE_HINTS:
            if hint in text:
                return hint
        return None

    def _temporal_metadata(self, text: str) -> dict:
        metadata = {}
        matches = [m for m in _TEMPORAL_RE.finditer(text)]
        if not matches:
            return metadata
        expressions = [m.group(0).strip() for m in matches if m.group(0).strip()]
        if expressions:
            metadata["temporal_expression"] = expressions[0]
            metadata["temporal_expressions"] = expressions
        for match in matches:
            day = match.groupdict().get("narrative_day") or match.groupdict().get("day")
            if day:
                metadata["narrative_day"] = int(day)
                break
        if any(token in text for token in ("那几天", "前几天", "这几天", "最近", "月中", "中旬", "上旬", "下旬", "月初", "月末")):
            metadata["temporal_window"] = True
        return metadata

    def _extract_after_words(self, text: str, words: tuple[str, ...]) -> Optional[str]:
        for word in sorted(words, key=len, reverse=True):
            if word not in text:
                continue
            tail = text.split(word, 1)[1]
            tail = re.split(r"[，,。；;！!？?]", tail, 1)[0]
            cleaned = self._clean_object(tail)
            if cleaned:
                return cleaned
        return None

    def _infer_tail_object(self, text: str) -> Optional[str]:
        parts = [p.strip() for p in re.split(r"[，,。；;！!？?]", text) if p.strip()]
        if not parts:
            return None
        return self._clean_object(parts[-1])

    def _strip_temporal_prefix(self, value: str) -> str:
        return _TEMPORAL_RE.sub("", value or "").strip(" ，,。；;") or value

    def _clean_object(self, value: str) -> Optional[str]:
        cleaned = str(value or "").strip()
        cleaned = re.sub(r"^(今天|中午|上午|下午|晚上|早上|刚刚|这次|一次|一杯|一个|这个|那个|的|我|我们|再|还|去|做|把|给|想再|喝|吃|买)+", "", cleaned)
        cleaned = re.sub(r"(花了?|消费|支付|付了|用了|共|一共|总共|难喝|难吃|不好喝|不好吃|不佳|糟糕|。|，)$", "", cleaned).strip()
        return cleaned or None
