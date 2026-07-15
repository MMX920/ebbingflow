import time

import pytest

from scripts.benchmark_longmemeval_native import build_report, validate_stripped_dataset


def test_stripped_dataset_rejects_official_answers_and_assistant_messages():
    with pytest.raises(ValueError, match="official answer"):
        validate_stripped_dataset([{"question_id": "a", "answer": "hidden"}])

    with pytest.raises(ValueError, match="non-user history message"):
        validate_stripped_dataset(
            [
                {
                    "question_id": "a",
                    "haystack_sessions": [[{"role": "assistant", "content": "hidden"}]],
                }
            ]
        )


def test_native_report_declares_runtime_has_no_official_answers(tmp_path):
    selected = [{"question_id": "a"}]
    results = [
        {
            "history_user_turn_count": 1,
            "history_failed_turn_count": 0,
            "memory_event_count": 2,
            "full_response_ms": 1000,
            "generated_history": [{"assistant_response": "own reply", "full_response_ms": 900}],
        }
    ]

    report = build_report(tmp_path / "input.json", selected, results, time.perf_counter())

    contract = report["evaluation_contract"]
    assert contract["official_assistant_messages_available_to_runtime"] == 0
    assert contract["official_answers_available_to_runtime"] is False
    assert contract["automated_judge_used"] is False
    assert report["generated_assistant_turn_count"] == 1
