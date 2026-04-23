import pytest

from core.chat_engine import ChatEngine


@pytest.mark.parametrize(
    "current_role, expected",
    [
        ("", "\u7ba1\u5bb6"),
        (None, "\u7ba1\u5bb6"),
        ("\u79c1\u4eba\u79d8\u4e66", "\u79d8\u4e66"),
        ("\u667a\u80fd\u52a9\u624b", "\u52a9\u624b"),
        ("\u4e13\u4e1a\u5bfc\u5e08", "\u4e13\u4e1a\u5bfc\u5e08"),
        ("AI\u7ba1\u5bb6", "\u7ba1\u5bb6"),
    ],
)
def test_resolve_role_label(current_role, expected):
    engine = ChatEngine()
    assert engine._resolve_role_label(current_role) == expected
