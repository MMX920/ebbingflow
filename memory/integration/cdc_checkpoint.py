"""
CDC 消费位点管理器 (CDC Checkpoint Manager)
使用 SQLite 维护不同消费者的位点信息以及重放去重记录。
"""
import sqlite3
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DB_PATH = ".data/cdc_checkpoint.db"

class CDCCheckpointManager:
    def __init__(self):
        os.makedirs(".data", exist_ok=True)
        self.conn = sqlite3.connect(DB_PATH)
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()
        # 1. 消费位点表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consumer_checkpoint (
                consumer_id TEXT,
                owner_id TEXT,
                last_version INTEGER,
                updated_at TEXT,
                PRIMARY KEY (consumer_id, owner_id)
            )
        """)
        # 2. 回放去重表 (Phase-2.5 L4)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS replay_dedup (
                idempotency_key TEXT PRIMARY KEY,
                processed_at TEXT
            )
        """)
        self.conn.commit()

    def get_checkpoint(self, consumer_id: str, owner_id: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_version FROM consumer_checkpoint WHERE consumer_id = ? AND owner_id = ?",
            (consumer_id, owner_id)
        )
        row = cursor.fetchone()
        return row[0] if row else 0

    def ack_checkpoint(self, consumer_id: str, owner_id: str, version: int) -> int:
        """更新位点，只允许前进"""
        current = self.get_checkpoint(consumer_id, owner_id)
        if version <= current:
            return current
        
        cursor = self.conn.cursor()
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        cursor.execute("""
            INSERT INTO consumer_checkpoint (consumer_id, owner_id, last_version, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(consumer_id, owner_id) DO UPDATE SET
                last_version = excluded.last_version,
                updated_at = excluded.updated_at
        """, (consumer_id, owner_id, version, now))
        self.conn.commit()
        return version

    def is_replayed(self, key: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM replay_dedup WHERE idempotency_key = ?", (key,))
        return cursor.fetchone() is not None

    def mark_replayed(self, key: str):
        cursor = self.conn.cursor()
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        cursor.execute("INSERT OR IGNORE INTO replay_dedup (idempotency_key, processed_at) VALUES (?, ?)", (key, now))
        self.conn.commit()

    def close(self):
        self.conn.close()
