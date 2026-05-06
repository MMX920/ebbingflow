"""Resolve natural-language temporal expressions in user text into a
(event_time, precision, time_part) tuple.

precision values:
  - "exact"        : 时分明确（"昨天下午2点"、"14:30"）
  - "part_of_day"  : 仅时段（"昨天下午"）
  - "day"          : 仅日期（"昨天"、"5月3日"）
  - "message"      : 文本无时间表达，使用消息发送时间兜底
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple, Union


# 时段起始时刻（Asia/Shanghai 本地时间）。"开始"而非"中点"是为了便于范围查询语义。
_PART_OF_DAY_HOURS = {
    "凌晨": 0,
    "清晨": 5,
    "早上": 6,
    "早": 6,
    "上午": 9,
    "中午": 11,
    "下午": 12,
    "傍晚": 17,
    "晚上": 18,
    "晚": 18,
    "夜里": 22,
    "深夜": 22,
    "夜": 22,
}
_PM_PARTS = {"下午", "傍晚", "晚上", "晚", "夜里", "深夜", "夜"}
_AM_PARTS = {"凌晨", "清晨", "早上", "早", "上午"}

_RELATIVE_DAY_OFFSET = {
    "大前天": -3,
    "前天": -2,
    "昨天": -1,
    "昨日": -1,
    "今天": 0,
    "今日": 0,
    "明天": 1,
    "明日": 1,
    "后天": 2,
    "大后天": 3,
}

_PART_RE = re.compile(
    "(" + "|".join(sorted(_PART_OF_DAY_HOURS, key=len, reverse=True)) + ")"
)
_REL_DAY_RE = re.compile(
    "(" + "|".join(sorted(_RELATIVE_DAY_OFFSET, key=len, reverse=True)) + ")"
)
_DATE_PATTERNS = re.compile(
    r"(?P<month>\d{1,2})\s*月\s*(?P<day>\d{1,2})\s*[号日]?"
    r"|周(?P<wd1>[一二三四五六日天])"
    r"|星期(?P<wd2>[一二三四五六日天])"
    r"|(?P<ago>\d+)\s*天前"
    r"|(?P<later>\d+)\s*天后"
)
_EXACT_TIME_RE = re.compile(r"(?<!\d)(\d{1,2})\s*(?:点|:|时)\s*(\d{0,2})")

_WEEKDAY_MAP = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6, "天": 6}
_SHANGHAI_TZ = timezone(timedelta(hours=8))


def _to_shanghai(dt: datetime) -> datetime:
    """Convert any datetime to tz-aware Shanghai time.

    A naive datetime is interpreted as **server local time** (whatever TZ the
    process is configured with via ``TZ`` env var or system default), not
    forcibly UTC — this matches what unannotated ``datetime.now().isoformat()``
    actually means and avoids a +8h drift when the container TZ is Asia/Shanghai.
    """
    if dt.tzinfo is None:
        # astimezone() with no argument treats naive as local time.
        dt = dt.astimezone()
    return dt.astimezone(_SHANGHAI_TZ)


def _coerce_base(base: Union[str, datetime, None]) -> datetime:
    """Return a tz-aware Shanghai datetime for use as a relative anchor."""
    if isinstance(base, datetime):
        return _to_shanghai(base)
    if base:
        try:
            dt = datetime.fromisoformat(str(base).replace("Z", "+00:00"))
            return _to_shanghai(dt)
        except ValueError:
            pass
    return _to_shanghai(datetime.now(timezone.utc))


def _resolve_date(text: str, base_dt: datetime) -> Tuple[date, bool]:
    """Find the calendar date referenced by text. Returns (date, found)."""
    rel = _REL_DAY_RE.search(text)
    if rel:
        return (base_dt + timedelta(days=_RELATIVE_DAY_OFFSET[rel.group(1)])).date(), True

    m = _DATE_PATTERNS.search(text)
    if not m:
        return base_dt.date(), False

    if m.group("month") and m.group("day"):
        try:
            month = int(m.group("month"))
            day = int(m.group("day"))
            candidate = base_dt.replace(month=month, day=day).date()
            return candidate, True
        except ValueError:
            return base_dt.date(), False
    if m.group("wd1") or m.group("wd2"):
        wd_char = m.group("wd1") or m.group("wd2")
        target_wd = _WEEKDAY_MAP[wd_char]
        delta = target_wd - base_dt.weekday()
        return (base_dt + timedelta(days=delta)).date(), True
    if m.group("ago"):
        return (base_dt - timedelta(days=int(m.group("ago")))).date(), True
    if m.group("later"):
        return (base_dt + timedelta(days=int(m.group("later")))).date(), True
    return base_dt.date(), False


def resolve(
    text: str, base: Union[str, datetime, None]
) -> Tuple[datetime, str, Optional[str]]:
    """Resolve temporal expression in `text` relative to `base`.

    Returns (event_time, precision, time_part_label). All datetimes are tz-aware.
    """
    base_dt = _coerce_base(base)
    raw = str(text or "")

    target_date, has_date = _resolve_date(raw, base_dt)
    part_match = _PART_RE.search(raw)
    exact_match = _EXACT_TIME_RE.search(raw)

    if exact_match:
        hour = int(exact_match.group(1))
        minute = int(exact_match.group(2) or 0)
        if part_match:
            label = part_match.group(1)
            if label in _PM_PARTS and hour < 12:
                hour += 12
            elif label in _AM_PARTS and hour >= 12:
                hour -= 12
        if 0 <= hour < 24 and 0 <= minute < 60:
            dt = datetime.combine(target_date, datetime.min.time(), tzinfo=_SHANGHAI_TZ).replace(
                hour=hour, minute=minute
            )
            return dt, "exact", None

    if part_match:
        label = part_match.group(1)
        hour = _PART_OF_DAY_HOURS[label]
        dt = datetime.combine(target_date, datetime.min.time(), tzinfo=_SHANGHAI_TZ).replace(
            hour=hour
        )
        return dt, "part_of_day", label

    if has_date:
        dt = datetime.combine(target_date, datetime.min.time(), tzinfo=_SHANGHAI_TZ)
        return dt, "day", None

    return base_dt, "message", None
