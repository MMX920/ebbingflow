from core.middleware import BaseMiddleware, MiddlewareChain
from core.session import ChatSession
from memory.graph.writer import GraphWriterMiddleware


class _BoomMiddleware(BaseMiddleware):
    async def process_response(self, ai_output, session):
        raise RuntimeError("boom")


def test_response_phase_exception_emits_error_audit():
    events = []
    chain = MiddlewareChain()
    chain.add(_BoomMiddleware())

    async def audit_callback(step, status, **kwargs):
        events.append((step, status, kwargs.get("reason")))

    import asyncio

    asyncio.run(
        chain.execute_response_phase(
            "ok",
            ChatSession(session_id="test-response-audit"),
            audit_callback=audit_callback,
        )
    )

    assert events == [("response_phase", "error", "_BoomMiddleware: boom")]


class _ExplodingExtractor:
    async def extract_events_from_text(self, *args, **kwargs):
        raise RuntimeError("extract boom")


def test_graph_writer_internal_exception_marks_remaining_steps_error():
    events = []
    middleware = object.__new__(GraphWriterMiddleware)
    middleware.extractor = _ExplodingExtractor()
    middleware.evolution_manager = None
    middleware.episode_manager = None
    middleware.saga_manager = None
    middleware.writer = None
    middleware.event_repo = None
    middleware.normalizer = None

    async def audit_callback(step, status, **kwargs):
        events.append((step, status, kwargs.get("reason")))

    session = ChatSession(session_id="test-writer-audit")
    session.audit_callback = audit_callback
    session.add_user_message("hello")

    import asyncio

    asyncio.run(
        middleware.process_response(
            "ok",
            session,
        )
    )

    assert events == [
        ("08", "error", "extract boom"),
        ("09", "error", "extract boom"),
        ("10", "error", "extract boom"),
        ("11", "error", "extract boom"),
        ("12", "error", "extract boom"),
    ]
