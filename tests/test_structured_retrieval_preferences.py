import asyncio
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import sqlite_config


def _bootstrap(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
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
        """
    )
    conn.executemany(
        """
        INSERT INTO ef_memory_events (
            event_id, owner_id, main_type, subtype, event_time, subject, predicate,
            object, amount, currency, currency_source, source_msg_id, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "e1",
                "alice",
                "FINANCE",
                "purchase",
                "2026-05-05T05:58:09+00:00",
                "alice",
                "spend",
                "星巴克咖啡",
                30,
                "CNY",
                "元",
                491,
                '{"experience":"negative"}',
            ),
            (
                "e2",
                "alice",
                "OPINION",
                "preference",
                "2026-05-05T05:58:09+00:00",
                "alice",
                "dislike",
                "星巴克咖啡",
                None,
                None,
                None,
                491,
                '{"sentiment":"negative"}',
            ),
            (
                "e3",
                "alice",
                "FINANCE",
                "purchase",
                "2026-05-05T06:46:38+00:00",
                "alice",
                "spend",
                "古茗的咖啡",
                9.9,
                "CNY",
                "元",
                495,
                '{"experience":"positive"}',
            ),
            (
                "e4",
                "alice",
                "OPINION",
                "preference",
                "2026-05-05T06:46:38+00:00",
                "alice",
                "like",
                "古茗的咖啡",
                None,
                None,
                None,
                495,
                '{"sentiment":"positive"}',
            ),
        ],
    )
    conn.commit()
    conn.close()


def test_structured_retrieval_surfaces_preference_events(tmp_path):
    db_path = str(tmp_path / "pref.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)
        from memory.knowledge_engine import KnowledgeBaseEngine

        engine = KnowledgeBaseEngine.__new__(KnowledgeBaseEngine)

        async def run():
            cands = await engine._retrieve_structured_events("我之前避雷过什么咖啡？有什么好喝的？", user_id="alice")
            joined = " | ".join(c.content for c in cands)
            assert "OPINION" in joined
            assert "星巴克咖啡" in joined
            assert "古茗的咖啡" in joined
            assert any(c.source_name == "SQL:Events" and c.source_msg_id in (491, 495) for c in cands)

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original


def test_structured_retrieval_chinese_finance_aggregation(tmp_path):
    db_path = str(tmp_path / "finance.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)
        from memory.knowledge_engine import KnowledgeBaseEngine

        engine = KnowledgeBaseEngine.__new__(KnowledgeBaseEngine)

        async def run():
            cands = await engine._retrieve_structured_events("我买咖啡总共花了多少钱？列出明细", user_id="alice")
            joined = " | ".join(c.content for c in cands)
            assert "39.9" in joined
            assert "星巴克咖啡" in joined
            assert "古茗的咖啡" in joined
            assert any(c.source_name == "SQL:Aggregate" for c in cands)

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original
