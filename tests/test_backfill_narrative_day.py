"""End-to-end test for scripts/backfill_narrative_day.py.

Seeds an isolated SQLite database with chat messages that mention
'第 N 天', plus matching ef_memory_events rows linked via source_msg_id
and via ef_event_evidence_links. Runs the backfill and asserts metadata
on every event row gained the right narrative_day.

Neo4j stamping is skipped (config import is unreachable in unit env);
the script handles that gracefully.
"""
import os
import sys
import json
import sqlite3
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import sqlite_config


def _bootstrap(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ef_chat_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            metadata TEXT
        );
        CREATE TABLE IF NOT EXISTS ef_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            role TEXT,
            speaker TEXT,
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metadata TEXT
        );
        CREATE TABLE IF NOT EXISTS ef_memory_events (
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
        CREATE TABLE IF NOT EXISTS ef_event_evidence_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_uuid TEXT NOT NULL,
            message_id INTEGER NOT NULL
        );
    """)

    conn.execute("INSERT INTO ef_chat_sessions (session_id, user_id) VALUES ('s1', 'alice')")
    conn.execute("INSERT INTO ef_chat_sessions (session_id, user_id) VALUES ('s2', 'bob')")
    rows = [
        # alice: msg 1 mentions day 36, msg 2 has no day, msg 3 day 47
        (1, "s1", "user", "user",      "第36天运回多少物资？"),
        (2, "s1", "assistant", "Andrew", "马匹五百匹，军械一千二百件。"),
        (3, "s1", "user", "user",      "今天第 47 天，赵云击溃多少人？"),
        # bob: day 9 — must NOT cross into alice
        (4, "s2", "user", "user",      "day 9 — recap please"),
    ]
    conn.executemany(
        "INSERT INTO ef_chat_messages (id, session_id, role, speaker, content) VALUES (?, ?, ?, ?, ?)",
        rows,
    )

    # event linked DIRECTLY via source_msg_id = 1 (alice, day 36)
    conn.execute(
        "INSERT INTO ef_memory_events "
        "(event_id, owner_id, main_type, subject, predicate, object, quantity, quantity_unit, source_msg_id, metadata) "
        "VALUES ('ev_a', 'alice', 'RESOURCE', 'andrew', 'delivers', '马匹', 500, '匹', 1, '{}')"
    )
    # event linked DIRECTLY via msg 3 (alice, day 47), already has unrelated metadata
    conn.execute(
        "INSERT INTO ef_memory_events "
        "(event_id, owner_id, main_type, subject, predicate, object, source_msg_id, metadata) "
        "VALUES ('ev_b', 'alice', 'OPINION', 'zhao_yun', 'defeats', 'enemy_officers', 3, ?)",
        (json.dumps({"foo": "bar"}),),
    )
    # event linked INDIRECTLY via evidence_links to msg 1 (alice, day 36)
    conn.execute(
        "INSERT INTO ef_memory_events "
        "(event_id, owner_id, main_type, subject, predicate, object, source_msg_id, metadata) "
        "VALUES ('ev_c', 'alice', 'PROPERTY', 'andrew', 'delivers', '军械', null, '{}')"
    )
    conn.execute(
        "INSERT INTO ef_event_evidence_links (event_uuid, message_id) VALUES ('ev_c', 1)"
    )
    # bob's event — must NOT be touched by alice's backfill
    conn.execute(
        "INSERT INTO ef_memory_events "
        "(event_id, owner_id, main_type, subject, predicate, object, source_msg_id, metadata) "
        "VALUES ('ev_d', 'bob', 'RESOURCE', 'bob', 'recaps', 'day9', 4, '{}')"
    )

    conn.commit()
    conn.close()


def _read_metadata(db_path, event_id):
    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT metadata FROM ef_memory_events WHERE event_id = ?", (event_id,))
    raw = cur.fetchone()[0]
    conn.close()
    return json.loads(raw or "{}")


def test_parse_narrative_day_helper():
    from scripts.backfill_narrative_day import parse_narrative_day
    assert parse_narrative_day("第36天运回多少物资") == 36
    assert parse_narrative_day("Day 9 recap") == 9
    assert parse_narrative_day("现在有多少物资") is None
    assert parse_narrative_day(None) is None


def test_backfill_stamps_direct_and_indirect_events_and_isolates_owner(tmp_path):
    db_path = str(tmp_path / "narrative.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)

        # Sanity: scope to alice — bob's event must remain untouched.
        from scripts.backfill_narrative_day import backfill
        result = asyncio.run(backfill(owner_id="alice", dry_run=False))

        assert result["messages_with_day"] == 2  # msg 1 (day36), msg 3 (day47)
        sql = result["sql"]
        # ev_a + ev_b directly stamped; ev_c via evidence_links
        assert sql["direct"] == 2
        assert sql["via_evidence"] == 1

        # Verify metadata
        assert _read_metadata(db_path, "ev_a")["narrative_day"] == 36
        assert _read_metadata(db_path, "ev_b") == {"foo": "bar", "narrative_day": 47}
        assert _read_metadata(db_path, "ev_c")["narrative_day"] == 36
        # bob's event must be untouched
        assert "narrative_day" not in _read_metadata(db_path, "ev_d")
    finally:
        sqlite_config.db_path = original


def test_backfill_is_idempotent(tmp_path):
    db_path = str(tmp_path / "narrative_idem.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)
        from scripts.backfill_narrative_day import backfill

        first = asyncio.run(backfill(owner_id="alice", dry_run=False))
        assert first["sql"]["direct"] == 2
        assert first["sql"]["via_evidence"] == 1

        # Second run: every row already has the right narrative_day
        # → all writes should be skipped.
        second = asyncio.run(backfill(owner_id="alice", dry_run=False))
        assert second["sql"]["direct"] == 0
        assert second["sql"]["via_evidence"] == 0
        assert second["sql"]["skipped_already_set"] == 3
    finally:
        sqlite_config.db_path = original


def test_backfill_dry_run_does_not_write(tmp_path):
    db_path = str(tmp_path / "narrative_dry.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)
        from scripts.backfill_narrative_day import backfill

        result = asyncio.run(backfill(owner_id="alice", dry_run=True))
        assert result["dry_run"] is True
        assert result["sql"]["direct"] >= 1  # would-have-stamped count

        # No row should actually have narrative_day set
        for ev in ("ev_a", "ev_b", "ev_c"):
            assert "narrative_day" not in _read_metadata(db_path, ev)
    finally:
        sqlite_config.db_path = original


def test_backfill_global_scope_covers_all_owners(tmp_path):
    db_path = str(tmp_path / "narrative_global.db")
    original = sqlite_config.db_path
    sqlite_config.db_path = db_path
    try:
        _bootstrap(db_path)
        from scripts.backfill_narrative_day import backfill

        result = asyncio.run(backfill(owner_id=None, dry_run=False))
        assert result["messages_with_day"] == 3  # alice's 2 + bob's 1
        # bob's event_d via msg 4 (day 9)
        assert _read_metadata(db_path, "ev_d")["narrative_day"] == 9
    finally:
        sqlite_config.db_path = original
