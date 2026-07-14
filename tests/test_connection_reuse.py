import pytest

from memory.identity.manager import PersonaManager
from memory.knowledge_engine import KnowledgeBaseEngine


class FakeDriver:
    def __init__(self):
        self.closed = 0

    async def close(self):
        self.closed += 1


class FakeVectorStorer:
    chat_collection = object()
    doc_collection = object()


@pytest.mark.asyncio
async def test_persona_manager_does_not_close_borrowed_driver():
    driver = FakeDriver()
    manager = PersonaManager(driver=driver)

    await manager.close()

    assert driver.closed == 0


@pytest.mark.asyncio
async def test_knowledge_engine_does_not_close_borrowed_driver():
    driver = FakeDriver()
    engine = KnowledgeBaseEngine(driver=driver, vector_storer=FakeVectorStorer())

    await engine.close()

    assert driver.closed == 0
