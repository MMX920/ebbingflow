import pytest
from unittest.mock import AsyncMock

from memory.event.slots import ObservationCategory, EfstbBehavioralTags, PersonaObservation
from memory.identity.manager import PersonaManager


class FakeSession:
    def __init__(self):
        self.run_calls = []

    async def run(self, query, **kwargs):
        self.run_calls.append((query, kwargs))

        class _R:
            async def single(self):
                return None

            async def consume(self):
                return None

        return _R()


@pytest.mark.anyio
async def test_long_term_observation_routes_to_big_five_momentum(monkeypatch):
    pm = PersonaManager()
    monkeypatch.setattr(pm, "bootstrap_genesis_identities", AsyncMock())
    momentum_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(pm, "update_big_five_momentum", momentum_mock)

    obs = PersonaObservation(
        target="user_001",
        category=ObservationCategory.LONG_TERM_PERSONA,
        content="More extroverted in recent interactions",
        confidence=0.9,
        big_five_update={"extraversion": 0.81, "openness": 0.66},
    )

    await pm._update_user_persona(FakeSession(), "user_001", obs)

    momentum_mock.assert_awaited_once()
    _, args, kwargs = momentum_mock.mock_calls[0]
    assert args[0] == "user_001"
    assert kwargs == {}
    filtered = args[1]
    assert filtered["extraversion"] == 0.81
    assert filtered["openness"] == 0.66


@pytest.mark.anyio
async def test_short_term_observation_updates_efstb_fields(monkeypatch):
    pm = PersonaManager()
    monkeypatch.setattr(pm, "bootstrap_genesis_identities", AsyncMock())

    fake_session = FakeSession()
    obs = PersonaObservation(
        target="user_001",
        category=ObservationCategory.SHORT_TERM_EFSTB,
        content="Needs concise answer quickly",
        confidence=0.92,
        efstb_update=EfstbBehavioralTags(
            urgency_level=0.93,
            granularity_preference="low",
            instruction_compliance=0.87,
            logic_vs_emotion=0.72,
        ),
    )

    await pm._update_user_persona(fake_session, "user_001", obs)

    assert fake_session.run_calls, "expected at least one query for EFSTB update"
    query, params = fake_session.run_calls[-1]
    assert "efstb_urgency" in query
    assert params["uid"] == "user_001"
    assert params["urgency"] == pytest.approx(0.93)
    assert params["granularity"] == "low"
    assert params["instruction"] == pytest.approx(0.87)
    assert params["logic"] == pytest.approx(0.72)
