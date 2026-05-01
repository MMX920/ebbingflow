import json
import asyncio

from api import server


class _DummyRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = type("Client", (), {"host": client_host})()
        self.headers = {}
        self.query_params = {}


class _FakeSession:
    user_id = "user_001"
    session_id = "master_session"


class _FakeEngine:
    def __init__(self):
        self.calls = []

    async def chat_stream(self, user_input, chat_session, status_callback=None, external_system_prompt=None):
        self.calls.append((user_input, chat_session, external_system_prompt))
        yield "hello"
        yield " world"


def test_extract_openai_user_input_uses_latest_user_message():
    req = server.OpenAIChatCompletionRequest(
        model="ebbingflow",
        messages=[
            {"role": "system", "content": "You are another character."},
            {"role": "user", "content": "old context should be ignored"},
            {"role": "assistant", "content": "previous answer"},
            {"role": "user", "content": "current turn"},
        ],
    )

    assert server._extract_openai_user_input(req.messages) == "current turn"


def test_extract_openai_external_system_prompt_includes_system_and_developer():
    req = server.OpenAIChatCompletionRequest(
        model="ebbingflow",
        messages=[
            {"role": "system", "content": "Use <happy> tags."},
            {"role": "developer", "content": "Keep replies under two sentences."},
            {"role": "assistant", "content": "previous answer"},
            {"role": "user", "content": "current turn"},
        ],
    )

    assert (
        server._extract_openai_external_system_prompt(req.messages)
        == "Use <happy> tags.\n\nKeep replies under two sentences."
    )


def test_message_content_extracts_text_parts_only():
    text = server._message_content_to_text(
        [
            {"type": "text", "text": "first"},
            {"type": "image_url", "image_url": {"url": "https://example.test/image.png"}},
            {"type": "text", "text": "second"},
        ]
    )

    assert text == "first\nsecond"


def test_openai_chat_completion_calls_engine_with_latest_user(monkeypatch):
    fake_engine = _FakeEngine()

    async def fake_get_user_session(user_id):
        return _FakeSession()

    monkeypatch.setattr(server, "engine", fake_engine)
    monkeypatch.setattr(server, "_get_user_session", fake_get_user_session)
    monkeypatch.setattr(server.identity_config, "user_id", "user_001")

    req = server.OpenAIChatCompletionRequest(
        model="ebbingflow",
        messages=[
            {"role": "system", "content": "format-only external prompt"},
            {"role": "user", "content": "real input"},
        ],
        stream=False,
    )

    response = asyncio.run(server._handle_openai_chat_completion(req, _DummyRequest()))

    assert response["object"] == "chat.completion"
    assert response["choices"][0]["message"]["content"] == "hello world"
    assert fake_engine.calls[0][0] == "real input"
    assert fake_engine.calls[0][2] == "format-only external prompt"


def test_openai_stream_emits_sse(monkeypatch):
    fake_engine = _FakeEngine()

    async def fake_get_user_session(user_id):
        return _FakeSession()

    monkeypatch.setattr(server, "engine", fake_engine)
    monkeypatch.setattr(server, "_get_user_session", fake_get_user_session)
    monkeypatch.setattr(server.identity_config, "user_id", "user_001")

    req = server.OpenAIChatCompletionRequest(
        model="ebbingflow",
        messages=[{"role": "user", "content": "stream me"}],
        stream=True,
    )

    async def collect_chunks():
        response = await server._handle_openai_chat_completion(req, _DummyRequest())
        chunks = []
        async for raw in response.body_iterator:
            text = raw.decode() if isinstance(raw, bytes) else raw
            chunks.append(text)
        return chunks

    chunks = asyncio.run(collect_chunks())

    assert chunks[-1] == "data: [DONE]\n\n"
    payloads = [
        json.loads(chunk.removeprefix("data: ").strip())
        for chunk in chunks[:-1]
        if chunk.startswith("data: {")
    ]
    assert payloads[0]["choices"][0]["delta"]["role"] == "assistant"
    assert any(p["choices"][0]["delta"].get("content") == "hello" for p in payloads)
