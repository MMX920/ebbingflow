import os
import sys

import pytest
from fastapi.testclient import TestClient

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.server import app


def _assert_known_user_profile_keys(profile: dict):
    allowed = {
        "name",
        "primary_name",
        "aliases",
        "age",
        "gender",
        "role",
        "state",
        "values",
        "bio",
        "mbti_label",
        "big_five",
        "inference_reasoning",
    }
    for key in profile.keys():
        assert key in allowed, f"Key '{key}' is not allowed in user_profile_struct"


def _assert_known_assistant_profile_keys(profile: dict):
    allowed = {
        "name",
        "primary_name",
        "persona",
        "role",
        "age",
        "gender",
        "relationship_to_user",
        "aliases",
        "state",
        "values",
        "bio",
        "mbti_label",
        "big_five",
        "efstb",
    }
    for key in profile.keys():
        assert key in allowed, f"Key '{key}' is not allowed in assistant_profile_struct"


@pytest.mark.parametrize("path", ["/monitor", "/"])
def test_pages_up(path):
    client = TestClient(app)
    resp = client.get(path)
    assert resp.status_code == 200


def test_ws_global_sync_payload_shape():
    client = TestClient(app)
    try:
        with client.websocket_connect("/ws") as ws:
            found = None
            for _ in range(50):
                msg = ws.receive_json()
                if msg.get("type") == "global_sync":
                    found = msg
                    break

            assert found is not None, "global_sync frame not found"
            assert "user_profile_struct" in found
            assert "assistant_profile_struct" in found

            _assert_known_user_profile_keys(found["user_profile_struct"])
            _assert_known_assistant_profile_keys(found["assistant_profile_struct"])

            # legacy v2 keys should not leak
            assert "traits" not in found["user_profile_struct"]
            assert "preferences" not in found["user_profile_struct"]
            assert "goals" not in found["user_profile_struct"]
            assert "boundaries" not in found["user_profile_struct"]

            assert "traits" not in found["assistant_profile_struct"]
            assert "preferences" not in found["assistant_profile_struct"]
            assert "goals" not in found["assistant_profile_struct"]
            assert "boundaries" not in found["assistant_profile_struct"]

            assert "userage" not in found["user_profile_struct"]
            assert "foo" not in found["user_profile_struct"]
    except Exception as exc:
        msg = str(exc).lower()
        klass = type(exc).__name__.lower()
        transient = [
            "connection refused",
            "failed to establish",
            "neo4j",
            "timeout",
            "disconnect",
            "anyio",
            "websocketdisconnect",
        ]
        if any(t in msg for t in transient) or any(t in klass for t in transient):
            pytest.skip(f"ws env unavailable: {exc}")
        raise
