# OpenAI-Compatible Chat API

EbbingFlow exposes a local OpenAI-compatible chat endpoint for external frontends.
The endpoint reuses the normal EbbingFlow memory engine, including SQL history,
Neo4j memory, vector recall, and response-phase memory writing.

## Endpoint

```http
POST /v1/chat/completions
POST /api/chat/completions
```

Use this as the Base URL in OpenAI-compatible clients:

```text
http://localhost:8000/v1
```

The local open-source edition does not validate API keys yet. If a client
requires one, use any placeholder value such as `local`.

Common client settings:

```text
Provider: OpenAI Compatible
Base URL: http://localhost:8000/v1
Model: ebbingflow
API Key: local
```

If a client asks for the full endpoint instead of a Base URL, use:

```text
http://localhost:8000/v1/chat/completions
```

## Request

```json
{
  "model": "ebbingflow",
  "messages": [
    { "role": "system", "content": "External app formatting instructions." },
    { "role": "user", "content": "I met Alice today." }
  ],
  "stream": false
}
```

`model` is optional. When omitted, the backend defaults it to `ebbingflow`.
Many clients still require a model field in their UI; enter `ebbingflow` there.

For compatibility, EbbingFlow uses the latest non-empty `user` message as the
current turn. External `system` and `developer` messages are injected into the
main prompt as an `[EXTERNAL_SYSTEM_PROMPT]` block. External `assistant` and
historical user messages are not replayed as conversation history.

When connecting another frontend, remove identity, character, and memory-control
instructions from its system prompt. Keep only output-format requirements such
as emotion tags, speech synthesis constraints, response length, or JSON shape.

## Non-Streaming Response

```json
{
  "id": "chatcmpl-...",
  "object": "chat.completion",
  "created": 1770000000,
  "model": "ebbingflow",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "..."
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0
  }
}
```

## Streaming

Set `stream` to `true` to receive Server-Sent Events:

```json
{
  "model": "ebbingflow",
  "messages": [{ "role": "user", "content": "Hello" }],
  "stream": true
}
```

The response emits `chat.completion.chunk` payloads followed by:

```text
data: [DONE]
```

## curl Example

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "ebbingflow",
    "messages": [
      {"role": "user", "content": "Please remember that I prefer concise replies."}
    ]
  }'
```

## Startup Requirements

EbbingFlow is a memory engine, so core storage must be available before the API
starts. Startup fails intentionally when SQL history, Neo4j graph memory, or the
vector store cannot initialize. This prevents a false-ready state where a
frontend can chat but memory is not recorded.

For local development:

- SQLite is used automatically when PostgreSQL is not configured.
- Neo4j must be running and reachable at `NEO4J_URI`, usually `bolt://localhost:7687`.
- Chroma and the configured embedding model must initialize successfully.
