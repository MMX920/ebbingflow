"""
Backfill metadata.narrative_day on existing memory events.

Existing chat messages and memory events were captured before the
extractor learned to tag '第 N 天' / 'day N' onto every emitted event.
After upgrading the retrieval layer to filter by narrative_day, those
old events become invisible to day-anchored queries unless we walk back
through chat history and stamp them.

Strategy
--------
1. Scan ef_chat_messages for narrative-day tokens in `content`.
2. For each message that mentions a day, propagate the day index to
   every ef_memory_events row that points at it via:
     - direct: ef_memory_events.source_msg_id
     - linked: ef_event_evidence_links.message_id
3. Best-effort mirror to Neo4j Event nodes that carry an event_id
   matching the SQL event_uuid.

Idempotent: re-running won't double-stamp. Skips rows whose
metadata.narrative_day is already correct.

Usage
-----
    # Dry-run — print counts only
    python scripts/backfill_narrative_day.py --dry-run

    # Backfill all users
    python scripts/backfill_narrative_day.py

    # Scope to one user
    python scripts/backfill_narrative_day.py --owner-id usr_abc123

The script is safe to run while the server is up; updates touch only
the metadata JSON column and a single Neo4j property.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from typing import Dict, Optional, Tuple, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory.sql.pool import get_db

logger = logging.getLogger("backfill_narrative_day")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


_NARRATIVE_DAY_PATTERNS = [
    re.compile(r"第\s*([0-9]{1,4})\s*天"),
    re.compile(r"\bday[\s\-#:]*([0-9]{1,4})\b", flags=re.IGNORECASE),
]


def parse_narrative_day(text: Optional[str]) -> Optional[int]:
    """Return the first narrative-day index referenced in text, or None."""
    if not text:
        return None
    for pat in _NARRATIVE_DAY_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        try:
            return int(m.group(1))
        except ValueError:
            continue
    return None


def _is_sqlite(conn) -> bool:
    return "sqlite" in str(type(conn)).lower()


async def collect_message_day_map(
    owner_id: Optional[str] = None,
) -> Dict[int, Tuple[int, str]]:
    """Scan ef_chat_messages for narrative-day mentions.

    Returns {message_id: (narrative_day, owner_id)}.
    Owner is resolved via the session join.
    """
    out: Dict[int, Tuple[int, str]] = {}
    async with get_db() as conn:
        sqlite = _is_sqlite(conn)
        if sqlite:
            if owner_id:
                cur = await conn.execute(
                    "SELECT m.id, m.content, s.user_id FROM ef_chat_messages m "
                    "JOIN ef_chat_sessions s ON s.session_id = m.session_id "
                    "WHERE s.user_id = ?",
                    (owner_id,),
                )
            else:
                cur = await conn.execute(
                    "SELECT m.id, m.content, s.user_id FROM ef_chat_messages m "
                    "JOIN ef_chat_sessions s ON s.session_id = m.session_id"
                )
            rows = await cur.fetchall()
        else:
            if owner_id:
                rows = await conn.fetch(
                    "SELECT m.id, m.content, s.user_id FROM ef_chat_messages m "
                    "JOIN ef_chat_sessions s ON s.session_id = m.session_id "
                    "WHERE s.user_id = $1",
                    owner_id,
                )
            else:
                rows = await conn.fetch(
                    "SELECT m.id, m.content, s.user_id FROM ef_chat_messages m "
                    "JOIN ef_chat_sessions s ON s.session_id = m.session_id"
                )
        for r in rows:
            r = dict(r)
            day = parse_narrative_day(r.get("content"))
            if day is None:
                continue
            mid = r.get("id")
            uid = r.get("user_id")
            if mid is None or not uid:
                continue
            out[int(mid)] = (day, uid)
    return out


async def _stamp_event_row(conn, sqlite: bool, event_id: str, day: int) -> bool:
    """Update a single ef_memory_events row's metadata.narrative_day.
    Returns True iff a write actually happened (idempotent skip otherwise)."""
    if sqlite:
        cur = await conn.execute(
            "SELECT metadata FROM ef_memory_events WHERE event_id = ?",
            (event_id,),
        )
        row = await cur.fetchone()
        if not row:
            return False
        raw = row[0] if not hasattr(row, "keys") else row["metadata"]
        try:
            md = json.loads(raw) if raw else {}
        except Exception:
            md = {}
        if md.get("narrative_day") == day:
            return False
        md["narrative_day"] = day
        await conn.execute(
            "UPDATE ef_memory_events SET metadata = ?, updated_at = CURRENT_TIMESTAMP WHERE event_id = ?",
            (json.dumps(md, ensure_ascii=False), event_id),
        )
        return True
    # PostgreSQL path: jsonb_set is atomic and idempotent-safe.
    res = await conn.execute(
        """
        UPDATE ef_memory_events
        SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{narrative_day}', to_jsonb($2::int)),
            updated_at = CURRENT_TIMESTAMP
        WHERE event_id = $1
          AND COALESCE((metadata ->> 'narrative_day')::int, -1) <> $2
        """,
        event_id, day,
    )
    try:
        return int(str(res).split()[-1]) > 0
    except Exception:
        return True


async def backfill_sql_events(
    msg_day_map: Dict[int, Tuple[int, str]],
    dry_run: bool,
) -> Dict[str, int]:
    """Update SQL events whose source_msg_id is in the day map.
    Also walks ef_event_evidence_links to catch events linked indirectly.
    """
    counts = {"direct": 0, "via_evidence": 0, "skipped_already_set": 0}
    if not msg_day_map:
        return counts

    async with get_db() as conn:
        sqlite = _is_sqlite(conn)

        # 1) Direct: source_msg_id ∈ map
        msg_ids = list(msg_day_map.keys())
        if sqlite:
            placeholders = ",".join(["?"] * len(msg_ids))
            cur = await conn.execute(
                f"SELECT event_id, owner_id, source_msg_id FROM ef_memory_events "
                f"WHERE source_msg_id IN ({placeholders})",
                msg_ids,
            )
            rows = [dict(r) for r in await cur.fetchall()]
        else:
            rows = [dict(r) for r in await conn.fetch(
                "SELECT event_id, owner_id, source_msg_id FROM ef_memory_events "
                "WHERE source_msg_id = ANY($1::bigint[])",
                msg_ids,
            )]

        for r in rows:
            mid = int(r["source_msg_id"])
            day, msg_owner = msg_day_map[mid]
            if r.get("owner_id") and msg_owner and r["owner_id"] != msg_owner:
                continue
            if dry_run:
                counts["direct"] += 1
                continue
            wrote = await _stamp_event_row(conn, sqlite, r["event_id"], day)
            counts["direct" if wrote else "skipped_already_set"] += 1

        # 2) Indirect: via ef_event_evidence_links
        try:
            if sqlite:
                placeholders = ",".join(["?"] * len(msg_ids))
                cur = await conn.execute(
                    f"SELECT DISTINCT l.event_uuid AS event_id, l.message_id "
                    f"FROM ef_event_evidence_links l "
                    f"WHERE l.message_id IN ({placeholders})",
                    msg_ids,
                )
                links = [dict(r) for r in await cur.fetchall()]
            else:
                links = [dict(r) for r in await conn.fetch(
                    "SELECT DISTINCT l.event_uuid AS event_id, l.message_id "
                    "FROM ef_event_evidence_links l "
                    "WHERE l.message_id = ANY($1::bigint[])",
                    msg_ids,
                )]
        except Exception as e:
            logger.warning("[Backfill] evidence_links read skipped: %s", e)
            links = []

        for r in links:
            mid = int(r["message_id"])
            day, _ = msg_day_map[mid]
            if dry_run:
                counts["via_evidence"] += 1
                continue
            wrote = await _stamp_event_row(conn, sqlite, r["event_id"], day)
            counts["via_evidence" if wrote else "skipped_already_set"] += 1

        if not dry_run:
            try:
                await conn.commit()
            except AttributeError:
                pass

    return counts


async def backfill_neo4j_events(
    msg_day_map: Dict[int, Tuple[int, str]],
    dry_run: bool,
) -> Dict[str, int]:
    """Mirror narrative_day onto Neo4j Event nodes that share event_id with
    a stamped SQL row. Best-effort — older Event nodes that never carried
    event_id are skipped (the SQL row still wins for the structured track).
    """
    out = {"matched": 0, "set": 0, "skipped": 0}
    if not msg_day_map:
        return out
    if dry_run:
        return out

    try:
        from neo4j import AsyncGraphDatabase
        from config import neo4j_config
    except Exception as e:
        logger.warning("[Backfill] neo4j unavailable: %s", e)
        return out

    # Collect (event_id, owner_id, day) tuples from SQL we just stamped.
    triples: List[Tuple[str, str, int]] = []
    async with get_db() as conn:
        sqlite = _is_sqlite(conn)
        msg_ids = list(msg_day_map.keys())
        if not msg_ids:
            return out
        if sqlite:
            placeholders = ",".join(["?"] * len(msg_ids))
            cur = await conn.execute(
                f"SELECT event_id, owner_id, source_msg_id FROM ef_memory_events "
                f"WHERE source_msg_id IN ({placeholders})",
                msg_ids,
            )
            rows = [dict(r) for r in await cur.fetchall()]
        else:
            rows = [dict(r) for r in await conn.fetch(
                "SELECT event_id, owner_id, source_msg_id FROM ef_memory_events "
                "WHERE source_msg_id = ANY($1::bigint[])",
                msg_ids,
            )]
        for r in rows:
            mid = int(r["source_msg_id"])
            day, owner = msg_day_map[mid]
            triples.append((r["event_id"], r.get("owner_id") or owner, day))

    if not triples:
        return out

    try:
        driver = AsyncGraphDatabase.driver(
            neo4j_config.uri,
            auth=(neo4j_config.username, neo4j_config.password),
        )
        try:
            async with driver.session() as session:
                for ev_id, owner, day in triples:
                    res = await session.run(
                        """
                        MATCH (evt:Event {event_id: $eid, owner_id: $uid})
                        WITH evt
                        WHERE coalesce(evt.narrative_day, -1) <> $day
                        SET evt.narrative_day = $day
                        RETURN count(evt) AS n
                        """,
                        eid=ev_id, uid=owner, day=day,
                    )
                    rec = await res.single()
                    if rec and int(rec["n"]) > 0:
                        out["matched"] += 1
                        out["set"] += int(rec["n"])
                    else:
                        out["skipped"] += 1
        finally:
            await driver.close()
    except Exception as e:
        logger.warning("[Backfill] neo4j stamping failed: %s", e)
    return out


async def backfill(owner_id: Optional[str], dry_run: bool) -> dict:
    msg_map = await collect_message_day_map(owner_id=owner_id)
    logger.info(
        "[Backfill] scanned messages with narrative-day tokens: %d (owner=%s)",
        len(msg_map), owner_id or "*",
    )
    sql_counts = await backfill_sql_events(msg_map, dry_run=dry_run)
    logger.info("[Backfill] SQL: %s", sql_counts)
    neo_counts = await backfill_neo4j_events(msg_map, dry_run=dry_run)
    if neo_counts["matched"] or neo_counts["skipped"]:
        logger.info("[Backfill] Neo4j: %s", neo_counts)
    return {
        "messages_with_day": len(msg_map),
        "sql": sql_counts,
        "neo4j": neo_counts,
        "dry_run": dry_run,
        "owner_id": owner_id,
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill narrative_day onto historical memory events.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would change without writing.")
    parser.add_argument("--owner-id", default=None, help="Limit to a single owner (defaults to all users).")
    args = parser.parse_args()
    result = asyncio.run(backfill(owner_id=args.owner_id, dry_run=args.dry_run))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
