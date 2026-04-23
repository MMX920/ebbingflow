import pytest
import uuid

from memory.event.slots import EventEnvelope, MainEventType
from memory.sql.event_repository import EventRepository
from memory.knowledge_engine import KnowledgeBaseEngine


@pytest.mark.asyncio
async def test_plan_items_are_returned_as_plan_source():
    repo = EventRepository()
    uid = f"plan_user_{uuid.uuid4().hex[:8]}"

    pending = EventEnvelope(
        main_type=MainEventType.TASK,
        subtype="todo",
        subject="User",
        predicate="finish",
        object="roadmap doc",
        metadata={"status": "pending", "priority": "high"},
        source_msg_id=120001,
    )
    done = EventEnvelope(
        main_type=MainEventType.TASK,
        subtype="todo",
        subject="User",
        predicate="buy",
        object="coffee",
        metadata={"status": "done"},
        source_msg_id=120002,
    )
    await repo.insert_event(pending, owner_id=uid)
    await repo.insert_event(done, owner_id=uid)

    kb = KnowledgeBaseEngine()
    try:
        candidates = await kb._retrieve_plan_items("today todo list", user_id=uid)
    finally:
        await kb.close()

    assert candidates, "expected at least one plan candidate"
    assert all(c.source_type == "plan" for c in candidates)
    merged = " ".join(c.content.lower() for c in candidates)
    assert "roadmap doc" in merged
    assert "coffee" not in merged

