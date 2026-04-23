import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class CDCOutbox:
    """Durable outbox for CDC incremental sync."""

    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CDCOutbox, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.db_path = os.path.join(".data", "cdc_outbox.db")
        os.makedirs(".data", exist_ok=True)
        self._init_db()
        self.failure_count = 0
        self._initialized = True

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox (
                    version INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id TEXT NOT NULL,
                    op TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    payload TEXT,
                    record_time TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_owner_version ON outbox(owner_id, version)"
            )

    def append_change(
        self,
        owner_id: str,
        op: str,
        entity_type: str,
        entity_id: str,
        payload: Dict[str, Any],
    ) -> int:
        try:
            s_owner_id = str(owner_id)
            s_op = str(op)
            s_entity_type = str(entity_type)
            s_entity_id = str(entity_id)

            clean_payload: Dict[str, Any] = {}
            if isinstance(payload, dict):
                for k, v in payload.items():
                    if isinstance(v, (str, int, float, bool, list, dict, type(None))):
                        clean_payload[k] = v
                    else:
                        clean_payload[k] = str(v)

            payload_json = json.dumps(clean_payload, ensure_ascii=False)
            record_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "INSERT INTO outbox (owner_id, op, entity_type, entity_id, payload, record_time) VALUES (?, ?, ?, ?, ?, ?)",
                    (s_owner_id, s_op, s_entity_type, s_entity_id, payload_json, record_time),
                )
                return int(cursor.lastrowid)
        except Exception as e:
            self.failure_count += 1
            logger.error(
                "[CDC_OUTBOX_FAILURE] Append failed (Total: %s): %s",
                self.failure_count,
                e,
            )
            return -1

    def list_changes_since(self, owner_id: str, since_version: int, limit: int = 200) -> List[Dict[str, Any]]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM outbox WHERE owner_id = ? AND version > ? ORDER BY version ASC LIMIT ?",
                    (owner_id, since_version, limit),
                )
                results: List[Dict[str, Any]] = []
                for row in cursor.fetchall():
                    item = dict(row)
                    item["payload"] = json.loads(item["payload"]) if item["payload"] else {}
                    results.append(item)
                return results
        except Exception as e:
            logger.error("[CDC_OUTBOX] List failed: %s", e)
            return []

    def get_latest_version(self, owner_id: str) -> int:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT MAX(version) FROM outbox WHERE owner_id = ?", (owner_id,)
                )
                res = cursor.fetchone()
                return int(res[0]) if res and res[0] is not None else 0
        except Exception as e:
            logger.error("[CDC_OUTBOX] GetVersion failed: %s", e)
            return 0


outbox = CDCOutbox()
