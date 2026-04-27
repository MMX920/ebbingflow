import os
import sys

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api import server


class _DummyWS:
    def __init__(self, headers=None, query_params=None):
        self.headers = headers or {}
        self.query_params = query_params or {}


class _DummyRequest:
    def __init__(self, headers=None, query_params=None, client_host="127.0.0.1"):
        self.headers = headers or {}
        self.query_params = query_params or {}
        self.client = type("Client", (), {"host": client_host})()


@pytest.fixture
def _ws_auth_reset(monkeypatch):
    monkeypatch.setattr(server.server_config, "ws_auth_required", False)
    monkeypatch.setattr(server.server_config, "ws_auth_token", "")
    monkeypatch.setattr(server.server_config, "ws_auth_query_param", "ws_token")
    monkeypatch.setattr(server.server_config, "maintenance_token", "")


def test_extract_ws_token_priority(_ws_auth_reset):
    ws = _DummyWS(
        headers={"authorization": "Bearer bearer-token", "x-ws-token": "header-token"},
        query_params={"ws_token": "query-token", "token": "legacy-token"},
    )
    assert server._extract_ws_token(ws) == "bearer-token"

    ws = _DummyWS(
        headers={"x-ws-token": "header-token"},
        query_params={"ws_token": "query-token"},
    )
    assert server._extract_ws_token(ws) == "header-token"

    ws = _DummyWS(headers={}, query_params={"ws_token": "query-token"})
    assert server._extract_ws_token(ws) == "query-token"


def test_is_ws_authorized_with_required_toggle(_ws_auth_reset, monkeypatch):
    monkeypatch.setattr(server.server_config, "ws_auth_required", True)
    monkeypatch.setattr(server.server_config, "ws_auth_token", "secret")

    no_token_ws = _DummyWS(headers={}, query_params={})
    wrong_token_ws = _DummyWS(headers={"x-ws-token": "wrong"}, query_params={})
    ok_token_ws = _DummyWS(headers={"x-ws-token": "secret"}, query_params={})

    assert server._is_ws_authorized(no_token_ws) is False
    assert server._is_ws_authorized(wrong_token_ws) is False
    assert server._is_ws_authorized(ok_token_ws) is True


def test_ws_endpoint_rejects_unauthorized(_ws_auth_reset, monkeypatch):
    monkeypatch.setattr(server.server_config, "ws_auth_required", True)
    monkeypatch.setattr(server.server_config, "ws_auth_token", "secret")

    client = TestClient(server.app)

    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect("/ws"):
            pass

    assert exc_info.value.code == 1008


def test_extract_http_token_priority(_ws_auth_reset):
    req = _DummyRequest(
        headers={
            "authorization": "Bearer bearer-token",
            "x-maintenance-token": "maintenance-token",
            "x-ws-token": "ws-token",
        },
        query_params={"ws_token": "query-token", "token": "legacy-token"},
    )
    assert server._extract_http_token(req) == "bearer-token"

    req = _DummyRequest(headers={"x-maintenance-token": "maintenance-token"}, query_params={})
    assert server._extract_http_token(req) == "maintenance-token"

    req = _DummyRequest(headers={}, query_params={"ws_token": "query-token"})
    assert server._extract_http_token(req) == "query-token"


def test_maintenance_endpoint_rejects_unauthorized(_ws_auth_reset, monkeypatch):
    monkeypatch.setattr(server.server_config, "maintenance_token", "maint-secret")

    client = TestClient(server.app)
    response = client.post("/maintenance/wipe-memory", json={"items": ["cognitive"]})
    assert response.status_code == 401


def test_maintenance_allows_loopback_when_auth_disabled_and_no_token(_ws_auth_reset):
    req = _DummyRequest(client_host="127.0.0.1")
    assert server._is_maintenance_authorized(req) is True


def test_maintenance_rejects_remote_when_auth_disabled_and_no_token(_ws_auth_reset):
    req = _DummyRequest(client_host="10.0.0.20")
    assert server._is_maintenance_authorized(req) is False
