import asyncio
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import sqlite_config


def _bootstrap(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE ef_chat_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            metadata TEXT
        );
        CREATE TABLE ef_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            role TEXT,
            speaker TEXT,
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metadata TEXT
        );
        CREATE TABLE ef_event_evidence_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_uuid TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            span_start INT,
            span_end INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_uuid, message_id)
        );
        CREATE TABLE ef_memory_events (
            event_id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            main_type TEXT NOT NULL,
            subtype TEXT,
            event_time TIMESTAMP,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT,
            quantity NUMERIC,
            quantity_unit TEXT,
            amount NUMERIC,
            currency TEXT,
            currency_source TEXT,
            confidence FLOAT DEFAULT 1.0,
            source_msg_id INTEGER,
            needs_confirmation BOOLEAN DEFAULT 0,
            metadata TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE ef_structured_extraction_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id TEXT NOT NULL,
            session_id TEXT,
            message_id INTEGER,
            status TEXT NOT NULL,
            rule_event_count INT DEFAULT 0,
            llm_event_count INT DEFAULT 0,
            normalized_event_count INT DEFAULT 0,
            written_event_count INT DEFAULT 0,
            error TEXT,
            metadata TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute("INSERT INTO ef_chat_sessions (session_id, user_id) VALUES ('s1', 'alice')")
    conn.execute(
        "INSERT INTO ef_chat_messages (id, session_id, role, speaker, content, timestamp) VALUES (491, 's1', 'user', 'alice', ?, ?)",
        ("今天中午买星巴克咖啡，花了30元，难喝。", "2026-05-05T05:58:09+00:00"),
    )
    conn.commit()
    conn.close()


def test_backfill_structured_events_writes_finance_opinion_and_audit(tmp_path):
    db_path = str(tmp_path / "structured.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)
        from scripts.backfill_structured_events import backfill

        result = asyncio.run(backfill(owner_id="alice", since_msg=491, limit=10, dry_run=False))
        assert result["events_written"] == 2

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        ev = dict(conn.execute("SELECT * FROM ef_memory_events WHERE main_type = 'FINANCE'").fetchone())
        assert ev["main_type"] == "FINANCE"
        assert ev["object"] == "星巴克咖啡"
        assert float(ev["amount"]) == 30.0
        assert ev["currency"] == "CNY"
        assert ev["source_msg_id"] == 491
        opinion = dict(conn.execute("SELECT * FROM ef_memory_events WHERE main_type = 'OPINION'").fetchone())
        assert opinion["object"] == "星巴克咖啡"
        audit = dict(conn.execute("SELECT * FROM ef_structured_extraction_audit").fetchone())
        assert audit["status"] == "backfilled"
        assert audit["rule_event_count"] == 2
        assert audit["written_event_count"] == 2
        md = json.loads(audit["metadata"] or "{}")
        assert md["source"] == "backfill_structured_events"
        conn.close()
    finally:
        sqlite_config.db_path = original
