import pytest
from memory.sql.event_repository import EventRepository
import uuid

@pytest.mark.asyncio
async def test_evidence_chain_linking():
    repo = EventRepository()
    event_id = str(uuid.uuid4())
    msg_id = 88888
    test_uid = "test_user_chain"
    
    # 1. Link evidence
    await repo.link_evidence(event_id, msg_id)
    
    # 2. Verify link exists
    from memory.sql.pool import get_db
    async with get_db() as conn:
        is_sqlite = "sqlite" in str(type(conn)).lower()
        query = "SELECT message_id FROM ef_event_evidence_links WHERE event_uuid = ?" if is_sqlite else "SELECT message_id FROM ef_event_evidence_links WHERE event_uuid = $1"
        
        if is_sqlite:
            cur = await conn.execute(query, (event_id,))
            row = await cur.fetchone()
        else:
            row = await conn.fetchrow(query, event_id)
            
        assert row is not None
        val = int(row[0]) if not isinstance(row, dict) else int(row['message_id'])
        assert val == msg_id

def test_knowledge_engine_importable():
    from memory.knowledge_engine import KnowledgeBaseEngine
    assert KnowledgeBaseEngine is not None
