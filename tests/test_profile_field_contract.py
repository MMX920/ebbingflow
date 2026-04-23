import pytest
from memory.identity.field_contract import normalize_profile_updates

def test_alias_and_whitelist_mapping():
    raw = {
        "userage": "18",
        "sex": "male",
        "foo": "bar",
        "location": "Shanghai",
    }
    clean, dropped = normalize_profile_updates(raw)

    assert clean["age"] == "18"
    assert clean["gender"] == "male"
    assert "location" not in clean
    assert dropped["location"] == "Shanghai"
    assert "foo" in dropped
    assert "foo" not in clean

def test_empty_input():
    clean, dropped = normalize_profile_updates({})
    assert clean == {}
    assert dropped == {}

def test_none_values_are_not_written():
    raw = {"age": None, "gender": "", "occupation": "engineer"}
    clean, dropped = normalize_profile_updates(raw)

    assert clean == {}
    assert dropped == {"occupation": "engineer"}
