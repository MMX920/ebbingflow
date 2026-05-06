import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_rule_extractor_finance_starbucks_negative_experience():
    from memory.event.rule_extractor import RuleBasedStructuredExtractor
    from memory.event.slots import MainEventType

    events = RuleBasedStructuredExtractor().extract(
        "今天中午买星巴克咖啡，花了30元，难喝。",
        actor_name="usr_1",
        source_msg_id=491,
        source_timestamp="2026-05-05T05:58:09+00:00",
    )

    finance = [ev for ev in events if ev.main_type == MainEventType.FINANCE]
    assert len(finance) == 1
    ev = finance[0]
    assert ev.subtype == "purchase"
    assert ev.object == "星巴克咖啡"
    assert ev.payload.amount == 30
    assert ev.payload.currency_source == "元"
    assert ev.source_msg_id == 491
    assert ev.metadata["experience"] == "negative"

    opinion = [ev for ev in events if ev.main_type == MainEventType.OPINION]
    assert opinion[0].object == "星巴克咖啡"
    assert opinion[0].metadata["sentiment"] == "negative"


def test_rule_extractor_resource_loss_with_narrative_day():
    from memory.event.rule_extractor import RuleBasedStructuredExtractor
    from memory.event.slots import MainEventType

    events = RuleBasedStructuredExtractor().extract("第61天损耗30石粮草", actor_name="u", source_msg_id=1)

    assert len(events) == 1
    ev = events[0]
    assert ev.main_type == MainEventType.RESOURCE
    assert ev.subtype == "expenditure"
    assert ev.payload.quantity == 30
    assert ev.payload.quantity_unit == "石"
    assert ev.object == "粮草"
    assert ev.metadata["narrative_day"] == 61
    assert ev.metadata["temporal_expression"] == "第61天"


def test_rule_extractor_health_measurement_symptom_and_medication():
    from memory.event.rule_extractor import RuleBasedStructuredExtractor
    from memory.event.slots import MainEventType

    events = RuleBasedStructuredExtractor().extract("今天体重70kg，血压120/80，有点头疼，吃了布洛芬", actor_name="u", source_msg_id=2)
    health = [ev for ev in events if ev.main_type == MainEventType.HEALTH]

    assert {ev.subtype for ev in health} == {"weight", "blood_pressure", "symptom", "medication"}
    symptom = next(ev for ev in health if ev.subtype == "symptom")
    medication = next(ev for ev in health if ev.subtype == "medication")
    assert symptom.object == "头疼"
    assert medication.object == "布洛芬"


def test_rule_extractor_schedule_and_task_temporal_windows():
    from memory.event.rule_extractor import RuleBasedStructuredExtractor
    from memory.event.slots import MainEventType

    events = RuleBasedStructuredExtractor().extract("3月中那几天提醒我检查库存，明天下午3点开会", actor_name="u", source_msg_id=3)

    task = [ev for ev in events if ev.main_type == MainEventType.TASK]
    assert len(task) == 1
    assert task[0].metadata["temporal_window"] is True
    assert "3月中那几天" in task[0].metadata["temporal_expressions"]
    assert "明天下午3点" in task[0].metadata["temporal_expressions"]


def test_rule_extractor_preference_without_money():
    from memory.event.rule_extractor import RuleBasedStructuredExtractor
    from memory.event.slots import MainEventType

    events = RuleBasedStructuredExtractor().extract("以后不想再喝星巴克，太难喝了", actor_name="u", source_msg_id=4)

    assert len(events) == 1
    ev = events[0]
    assert ev.main_type == MainEventType.OPINION
    assert ev.subtype == "preference"
    assert ev.predicate == "dislike"
    assert ev.object == "星巴克"
    assert ev.metadata["sentiment"] == "negative"


def test_event_repository_postgres_insert_avoids_partial_index_conflict(monkeypatch):
    import asyncio

    from memory.event.slots import EventEnvelope, MainEventType, TypedPayload
    from memory.sql.event_repository import EventRepository

    calls = []

    class FakePgConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def fetchval(self, sql, *params):
            calls.append(("fetchval", sql, params))
            if "SELECT event_id FROM ef_memory_events" in sql:
                return None
            assert "ON CONFLICT (owner_id, source_msg_id" not in sql
            assert ") RETURNING event_id" in " ".join(sql.split())
            return "event-1"

        async def execute(self, sql, *params):
            calls.append(("execute", sql, params))

        async def fetch(self, *args, **kwargs):
            return []

    monkeypatch.setattr("memory.sql.event_repository.get_db", lambda: FakePgConn())
    event = EventEnvelope(
        main_type=MainEventType.FINANCE,
        subtype="purchase",
        subject="u",
        predicate="spend",
        object="咖啡",
        payload=TypedPayload(amount=16, currency_source="元"),
        source_msg_id=493,
    )

    result = asyncio.run(EventRepository().insert_event(event, owner_id="u"))

    assert result == "event-1"
    assert any("IS NOT DISTINCT FROM" in sql for _, sql, _ in calls)
