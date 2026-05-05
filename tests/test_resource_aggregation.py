"""Tests for the RESOURCE / quantity aggregation path.

Verifies:
  - aggregate_quantities() groups by (subject, object, subtype, quantity_unit)
    and SUMs quantity, scoped to a single owner_id.
  - list_quantitative_events() returns rows with non-null quantity.
  - knowledge_engine._retrieve_structured_events triggers on resource keywords
    AND falls back to quantitative-only lookup when no typed trigger fires
    but the query carries aggregation intent ("现在有多少物资", "列出明细").
"""
import os
import sys
import sqlite3
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import sqlite_config


def _bootstrap_events_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ef_memory_events (
            event_id VARCHAR(64) PRIMARY KEY,
            owner_id VARCHAR(64) NOT NULL,
            main_type VARCHAR(32) NOT NULL,
            subtype VARCHAR(64),
            event_time TIMESTAMP,
            subject VARCHAR(255) NOT NULL,
            predicate VARCHAR(255) NOT NULL,
            object VARCHAR(255),
            quantity NUMERIC(20, 4),
            quantity_unit VARCHAR(32),
            amount NUMERIC(20, 4),
            currency VARCHAR(10),
            currency_source VARCHAR(64),
            confidence FLOAT DEFAULT 1.0,
            source_msg_id INTEGER,
            needs_confirmation BOOLEAN DEFAULT 0,
            metadata TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    rows = [
        # owner=alice, two acquisition deliveries of horses on different days
        ("e1", "alice", "RESOURCE", "acquisition", "2026-04-15T10:00", "andrew", "delivers", "马匹",  500, "匹"),
        ("e2", "alice", "RESOURCE", "acquisition", "2026-04-26T11:00", "andrew", "delivers", "马匹",  120, "匹"),
        # weapons
        ("e3", "alice", "RESOURCE", "acquisition", "2026-04-15T10:00", "andrew", "delivers", "军械", 1200, "件"),
        ("e4", "alice", "RESOURCE", "acquisition", "2026-04-30T09:00", "andrew", "delivers", "军械",  300, "件"),
        # Some grain (different unit)
        ("e5", "alice", "RESOURCE", "acquisition", "2026-04-22T08:00", "andrew", "stocks",   "粮草",   80, "石"),
        # A loss/expenditure row should subtract from the inventory total
        ("e5_loss", "alice", "RESOURCE", "loss", "2026-04-23T08:00", "andrew", "损耗", "粮草", 30, "石"),
        # An untyped quantitative event — extractor mis-tagged it as PROPERTY
        ("e6", "alice", "PROPERTY", "stocktake",   "2026-04-29T08:00", "alice",  "holds",   "弓箭",  240, "把"),
        # bob's data should not leak into alice's totals
        ("e7", "bob",   "RESOURCE", "acquisition", "2026-04-20T12:00", "bob",    "stocks",   "粮草",  999, "石"),
    ]
    conn.executemany(
        "INSERT INTO ef_memory_events "
        "(event_id, owner_id, main_type, subtype, event_time, subject, predicate, object, quantity, quantity_unit) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_aggregate_quantities_groups_by_object_and_unit(tmp_path):
    db_path = str(tmp_path / "events.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap_events_db(db_path)
        from memory.sql.event_repository import EventRepository
        from memory.event.slots import MainEventType

        async def run():
            repo = EventRepository()
            results = await repo.aggregate_quantities(
                owner_id="alice", main_type=MainEventType.RESOURCE
            )
            # Roll-up keyed by (object, unit) — we'll assert against that subset.
            totals = {(r["object"], r["quantity_unit"]): float(r["total_quantity"]) for r in results}
            assert totals[("马匹", "匹")] == 620.0   # 500 + 120
            assert totals[("军械", "件")] == 1500.0  # 1200 + 300
            assert totals[("粮草", "石")] == 50.0
            raw_totals = {(r["object"], r["quantity_unit"]): float(r["raw_total_quantity"]) for r in results}
            assert raw_totals[("粮草", "石")] == 110.0
            # bob's粮草 must NOT appear in alice's totals
            assert all(r.get("object") != "粮草" or float(r["total_quantity"]) == 50.0 for r in results)

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original


def test_list_quantitative_events_only_returns_quantitied_rows(tmp_path):
    db_path = str(tmp_path / "events2.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap_events_db(db_path)
        # Add a row WITHOUT quantity — it must be filtered out.
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO ef_memory_events (event_id, owner_id, main_type, subtype, subject, predicate, object) "
            "VALUES ('e8', 'alice', 'OPINION', 'remark', 'alice', 'thinks', '战局尚可')"
        )
        conn.commit()
        conn.close()

        from memory.sql.event_repository import EventRepository

        async def run():
            repo = EventRepository()
            rows = await repo.list_quantitative_events(owner_id="alice", limit=50)
            assert len(rows) == 7  # e1..e6 plus e5_loss; the OPINION row has no quantity
            assert all(r["quantity"] is not None for r in rows)
            assert all(r["owner_id"] == "alice" for r in rows)

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original


def test_structured_retrieval_triggers_on_resource_keyword(tmp_path):
    """'现在有多少物资？列出明细' must surface RESOURCE aggregates."""
    db_path = str(tmp_path / "events3.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap_events_db(db_path)
        from memory.knowledge_engine import KnowledgeBaseEngine

        engine = KnowledgeBaseEngine.__new__(KnowledgeBaseEngine)

        async def run():
            cands = await engine._retrieve_structured_events(
                "现在有多少物资？列出明细", user_id="alice"
            )
            assert cands, "expected at least one candidate"
            # Aggregator output should be present
            agg_lines = [c for c in cands if c.source_name == "SQL:Aggregate"]
            assert agg_lines, "expected SQL:Aggregate rows"
            joined = " | ".join(c.content for c in agg_lines)
            assert "马匹" in joined and "620" in joined
            assert "军械" in joined and "1500" in joined

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original


def test_aggregation_fallback_when_no_typed_trigger(tmp_path):
    """When the user asks 'list current totals' without naming a resource
    keyword, the fallback path should still surface every quantitative
    event for the owner."""
    db_path = str(tmp_path / "events4.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap_events_db(db_path)
        from memory.knowledge_engine import KnowledgeBaseEngine

        engine = KnowledgeBaseEngine.__new__(KnowledgeBaseEngine)

        async def run():
            cands = await engine._retrieve_structured_events(
                "盘点一下，现在有多少？列出明细", user_id="alice"
            )
            assert cands, "fallback should surface candidates"
            # Fallback aggregator should produce QTY-prefixed rows
            qty_rows = [c for c in cands if c.content.startswith("[QTY]")]
            raw_rows = [c for c in cands if c.source_name == "SQL:QtyFallback"]
            assert qty_rows, "fallback aggregate rows expected"
            assert raw_rows, "fallback raw rows expected"
            # PROPERTY-tagged 弓箭 row (e6) must reach the LLM via fallback
            assert any("弓箭" in c.content for c in cands)

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original


def test_no_aggregation_intent_no_resource_keyword_returns_empty(tmp_path):
    """If neither typed triggers nor aggregation intent are present, the
    structured track should stay silent (other tracks handle it)."""
    db_path = str(tmp_path / "events5.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap_events_db(db_path)
        from memory.knowledge_engine import KnowledgeBaseEngine

        engine = KnowledgeBaseEngine.__new__(KnowledgeBaseEngine)

        async def run():
            cands = await engine._retrieve_structured_events(
                "今天天气怎么样？", user_id="alice"
            )
            assert cands == []

        asyncio.run(run())
    finally:
        sqlite_config.db_path = original
