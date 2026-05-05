"""Tests for RP-style narrative day handling in knowledge_engine.

The graph retrieval fallback used to flood the prompt with global
highest-impact events when the keyword pass missed (no entity name
mentioned). For queries like '第 61 天我们做了什么' that hit no entity,
the result was creation-day genesis events being injected as Day-N
evidence — the LLM then hallucinated confidently.

These tests cover:
  - _infer_narrative_day correctly parses '第 N 天' and 'day N' forms
  - returns None when the query has no narrative-day token
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _engine():
    from memory.knowledge_engine import KnowledgeBaseEngine
    return KnowledgeBaseEngine.__new__(KnowledgeBaseEngine)


def test_infer_narrative_day_chinese_form():
    e = _engine()
    assert e._infer_narrative_day("第 61 天我们做了什么") == 61
    assert e._infer_narrative_day("第36天运回多少物资") == 36
    assert e._infer_narrative_day("今天第 47 天，赵云击溃多少人？") == 47


def test_infer_narrative_day_english_form():
    e = _engine()
    assert e._infer_narrative_day("what happened on day 23") == 23
    assert e._infer_narrative_day("Day-9 recap please") == 9
    assert e._infer_narrative_day("Day#12 status") == 12


def test_infer_narrative_day_returns_none_for_unrelated():
    e = _engine()
    assert e._infer_narrative_day("现在有多少物资") is None
    assert e._infer_narrative_day("今天天气怎么样") is None
    assert e._infer_narrative_day("赵云击溃多少人") is None
    assert e._infer_narrative_day("") is None
    assert e._infer_narrative_day(None) is None


def test_infer_narrative_day_does_not_match_year_or_address():
    """'2026 年' / '第三季度' / '110' 不应被误读成 narrative day."""
    e = _engine()
    assert e._infer_narrative_day("2026 年发生了什么") is None
    assert e._infer_narrative_day("第三季度的产量") is None
    # '110次' is a count, not a day
    assert e._infer_narrative_day("110 次请求") is None


def test_infer_narrative_day_handles_extra_whitespace():
    e = _engine()
    assert e._infer_narrative_day("第   100   天") == 100


def test_infer_time_window_relative_fuzzy_days(monkeypatch):
    import memory.knowledge_engine as ke

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(ke, "datetime", FixedDateTime)
    e = _engine()

    assert e._infer_time_window("前几天发生了什么") == (
        "2026-03-17T00:00:00Z",
        "2026-03-20T23:59:59Z",
        "nlp_inferred",
    )
    assert e._infer_time_window("过去5天的物资变化") == (
        "2026-03-15T00:00:00Z",
        "2026-03-20T23:59:59Z",
        "nlp_inferred",
    )


def test_infer_time_window_calendar_day_and_nearby_days(monkeypatch):
    import memory.knowledge_engine as ke

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(ke, "datetime", FixedDateTime)
    e = _engine()

    assert e._infer_time_window("3月8号发生了什么") == (
        "2026-03-08T00:00:00Z",
        "2026-03-08T23:59:59Z",
        "nlp_inferred",
    )
    assert e._infer_time_window("3月8号那几天有什么记录") == (
        "2026-03-06T00:00:00Z",
        "2026-03-10T23:59:59Z",
        "nlp_inferred",
    )
    assert e._infer_time_window("2025年3月中旬的情况") == (
        "2025-03-11T00:00:00Z",
        "2025-03-20T23:59:59Z",
        "nlp_inferred",
    )
