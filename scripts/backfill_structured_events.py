"""Backfill deterministic structured events from historical chat messages.

This is intentionally rule-based so it can safely recover high-confidence
facts such as spending, inventory/resource quantities, and health metrics
without needing an LLM.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory.event.rule_extractor import RuleBasedStructuredExtractor
from memory.event.normalizer import ContentNormalizerAgent
from memory.sql.event_repository import EventRepository
from memory.sql.pool import get_db


def _is_sqlite(conn) -> bool:
    return "sqlite" in str(type(conn)).lower()


async def _load_messages(owner_id: Optional[str], since_msg: Optional[int], limit: int):
    async with get_db() as conn:
        sqlite = _is_sqlite(conn)
        clauses = []
        params = []
        if owner_id:
            clauses.append("s.user_id = ?")
            params.append(owner_id)
        if since_msg:
            clauses.append("m.id >= ?")
            params.append(int(since_msg))
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        sql = (
            "SELECT m.id, m.session_id, m.role, m.speaker, m.content, m.timestamp, s.user_id "
            "FROM ef_chat_messages m JOIN ef_chat_sessions s ON s.session_id = m.session_id "
            f"{where} ORDER BY m.id ASC LIMIT {int(limit)}"
        )
        if sqlite:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
        else:
            pg_sql = sql
            for idx, _ in enumerate(params, start=1):
                pg_sql = pg_sql.replace("?", f"${idx}", 1)
            rows = await conn.fetch(pg_sql, *params)
        return [dict(r) for r in rows]


async def backfill(owner_id: Optional[str], since_msg: Optional[int], limit: int, dry_run: bool) -> dict:
    extractor = RuleBasedStructuredExtractor()
    normalizer = ContentNormalizerAgent()
    repo = EventRepository()
    rows = await _load_messages(owner_id, since_msg, limit)
    stats = {
        "messages_scanned": 0,
        "messages_with_events": 0,
        "events_extracted": 0,
        "events_written": 0,
        "dry_run": dry_run,
    }

    for row in rows:
        if str(row.get("role") or "").lower() != "user":
            continue
        stats["messages_scanned"] += 1
        events = extractor.extract(
            row.get("content") or "",
            actor_name=row.get("speaker") or row.get("user_id") or "user",
            source_msg_id=int(row["id"]),
            source_timestamp=str(row.get("timestamp") or ""),
        )
        if not events:
            continue
        stats["messages_with_events"] += 1
        normalized = normalizer.normalize_envelopes(events)
        stats["events_extracted"] += len(normalized)
        if dry_run:
            continue
        written = 0
        for env in normalized:
            event_id = await repo.insert_event(env, owner_id=row["user_id"])
            if event_id:
                written += 1
                await repo.link_evidence(event_id, int(row["id"]))
        stats["events_written"] += written
        await repo.record_extraction_audit(
            owner_id=row["user_id"],
            session_id=row.get("session_id"),
            message_id=int(row["id"]),
            status="backfilled" if written else "empty",
            rule_event_count=len(events),
            llm_event_count=0,
            normalized_event_count=len(normalized),
            written_event_count=written,
            metadata={"source": "backfill_structured_events"},
        )

    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill structured events from chat history.")
    parser.add_argument("--owner-id", default=None, help="Limit to one owner/user id.")
    parser.add_argument("--since-msg", type=int, default=None, help="Only scan messages with id >= this value.")
    parser.add_argument("--limit", type=int, default=5000, help="Maximum chat messages to scan.")
    parser.add_argument("--dry-run", action="store_true", help="Extract and count without writing.")
    args = parser.parse_args()
    result = asyncio.run(backfill(args.owner_id, args.since_msg, args.limit, args.dry_run))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
