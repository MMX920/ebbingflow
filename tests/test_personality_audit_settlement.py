from datetime import datetime, timedelta

from memory.event.slots import ObservationCategory, PersonaObservation
from memory.identity.manager import PersonaManager


def test_build_personality_evidence_rows_for_long_term_observation():
    obs = PersonaObservation(
        target="user",
        category=ObservationCategory.LONG_TERM_PERSONA,
        content="Observed stable analytical behavior",
        confidence=0.9,
        big_five_update={"extraversion": 0.22, "openness": "0.61"},
        source_msg_id=12,
    )

    rows = PersonaManager._build_personality_evidence_rows(obs)
    predicates = {row["predicate"] for row in rows}

    assert "big_five_extraversion" in predicates
    assert "big_five_openness" in predicates
    assert all(row["category"] == ObservationCategory.LONG_TERM_PERSONA.value for row in rows)
    assert all(row["source_msg_id"] == 12 for row in rows)


def test_settle_rows_applies_conflict_and_noise_penalty():
    now = datetime.now()
    rows = [
        {
            "evidence_id": "1",
            "predicate": "mbti_label",
            "object": "ENFP",
            "evidence": "I am ENFP, just kidding.",
            "confidence": 0.95,
            "source": "self_report",
            "created_at": now.isoformat(),
            "category": ObservationCategory.LONG_TERM_PERSONA.value,
        },
        {
            "evidence_id": "2",
            "predicate": "mbti_label",
            "object": "INTJ",
            "evidence": "Observed consistent analytical behavior.",
            "confidence": 0.88,
            "source": "behavior_observation",
            "created_at": now.isoformat(),
            "category": ObservationCategory.LONG_TERM_PERSONA.value,
        },
        {
            "evidence_id": "3",
            "predicate": "big_five_extraversion",
            "object": "0.2",
            "evidence": "Low social engagement over multiple sessions.",
            "confidence": 0.9,
            "source": "behavior_observation",
            "created_at": now.isoformat(),
            "category": ObservationCategory.LONG_TERM_PERSONA.value,
        },
        {
            "evidence_id": "4",
            "predicate": "big_five_extraversion",
            "object": "0.85",
            "evidence": "Maybe extrovert, guess so.",
            "confidence": 0.7,
            "source": "self_report",
            "created_at": now.isoformat(),
            "category": ObservationCategory.LONG_TERM_PERSONA.value,
        },
    ]

    settlement = PersonaManager._settle_rows(rows, lookback_days=30)

    assert settlement["conflict_groups"] >= 1
    assert "2" in settlement["selected_refs"]
    assert settlement["quality_index"] > 0
    # the noisy/disputed ENFP evidence should be down-weighted out of top refs
    assert "1" not in settlement["selected_refs"]


def test_settle_rows_counts_recent_records_by_lookback():
    now = datetime.now()
    old_time = (now - timedelta(days=80)).isoformat()
    new_time = now.isoformat()

    rows = [
        {
            "evidence_id": "old",
            "predicate": "big_five_openness",
            "object": "0.7",
            "evidence": "Historical preference signal",
            "confidence": 0.8,
            "source": "behavior_observation",
            "created_at": old_time,
            "category": ObservationCategory.LONG_TERM_PERSONA.value,
        },
        {
            "evidence_id": "new",
            "predicate": "big_five_openness",
            "object": "0.72",
            "evidence": "Recent stable openness signal",
            "confidence": 0.85,
            "source": "behavior_observation",
            "created_at": new_time,
            "category": ObservationCategory.LONG_TERM_PERSONA.value,
        },
    ]

    settlement = PersonaManager._settle_rows(rows, lookback_days=30)

    assert settlement["total"] == 2
    assert settlement["recent_count"] == 1
    assert settlement["selected_count"] >= 1
