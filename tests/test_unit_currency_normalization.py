import pytest
from decimal import Decimal
from memory.event.slots import EventEnvelope, MainEventType, TypedPayload
from memory.event.normalizer import NormalizationEngine

@pytest.fixture
def engine():
    return NormalizationEngine()

def test_currency_slang(engine):
    env = EventEnvelope(
        main_type=MainEventType.FINANCE,
        subject="User",
        predicate="spent",
        payload=TypedPayload(original_text="买了五毛钱的糖")
    )
    normalized = engine.normalize_envelope(env)
    assert normalized.payload.amount == Decimal("0.5000")
    assert normalized.payload.currency == "CNY"
    assert "slang_wumao" in normalized.normalization.rules_applied

def test_unit_normalization_jin(engine):
    env = EventEnvelope(
        main_type=MainEventType.HEALTH,
        subject="User",
        predicate="weighs",
        payload=TypedPayload(quantity=Decimal("100"), quantity_unit="斤")
    )
    normalized = engine.normalize_envelope(env)
    assert normalized.payload.quantity == Decimal("50.0000")
    assert normalized.payload.quantity_unit == "kg"
    assert "unit_jin_to_kg" in normalized.normalization.rules_applied

def test_currency_inference_rmb(engine):
    env = EventEnvelope(
        main_type=MainEventType.FINANCE,
        subject="User",
        predicate="spent",
        payload=TypedPayload(quantity=Decimal("15"), original_text="15块钱")
    )
    normalized = engine.normalize_envelope(env)
    assert normalized.payload.currency == "CNY"
    assert normalized.payload.amount == Decimal("15.0000")

def test_unit_normalization_lb(engine):
    env = EventEnvelope(
        main_type=MainEventType.HEALTH,
        subject="User",
        predicate="weighs",
        payload=TypedPayload(quantity=Decimal("100"), quantity_unit="lb")
    )
    normalized = engine.normalize_envelope(env)
    # 100 * 0.4536 = 45.36
    assert normalized.payload.quantity == Decimal("45.3600")
    assert normalized.payload.quantity_unit == "kg"
