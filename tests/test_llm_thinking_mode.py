from types import SimpleNamespace

from bridge.llm import LLMBridge
from config import LLMConfig


def _bridge(thinking_mode: str) -> LLMBridge:
    bridge = object.__new__(LLMBridge)
    bridge.config = SimpleNamespace(thinking_mode=thinking_mode)
    return bridge


def test_disabled_thinking_is_added_to_extra_body():
    options = _bridge("disabled")._apply_provider_options({})

    assert options == {"extra_body": {"thinking": {"type": "disabled"}}}


def test_thinking_option_preserves_other_extra_body_fields():
    options = _bridge("enabled")._apply_provider_options(
        {"extra_body": {"trace_id": "benchmark"}}
    )

    assert options == {
        "extra_body": {
            "trace_id": "benchmark",
            "thinking": {"type": "enabled"},
        }
    }


def test_unset_thinking_mode_does_not_change_request():
    assert _bridge("")._apply_provider_options({"max_tokens": 64}) == {
        "max_tokens": 64
    }


def test_deepseek_defaults_to_disabled_thinking(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "deepseek-v4-flash")
    monkeypatch.setenv("LLM_BASE_URL", "https://api.deepseek.com")
    monkeypatch.delenv("LLM_THINKING_MODE", raising=False)

    assert LLMConfig(prefix="LLM").thinking_mode == "disabled"


def test_non_deepseek_does_not_receive_provider_specific_default(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "gpt-compatible-model")
    monkeypatch.setenv("LLM_BASE_URL", "https://example.com/v1")
    monkeypatch.delenv("LLM_THINKING_MODE", raising=False)

    assert LLMConfig(prefix="LLM").thinking_mode == ""


def test_explicit_thinking_mode_overrides_deepseek_default(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "deepseek-v4-flash")
    monkeypatch.setenv("LLM_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("LLM_THINKING_MODE", "enabled")

    assert LLMConfig(prefix="LLM").thinking_mode == "enabled"
