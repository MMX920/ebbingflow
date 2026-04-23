"""
Structured Memory Event Repository
Handles CRUD operations for ef_memory_events with support for both PostgreSQL and SQLite fallback.
"""
import logging
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from decimal import Decimal
import uuid

from memory.sql.pool import get_db
from memory.event.slots import EventEnvelope, MainEventType, TypedPayload, NormalizationMeta

logger = logging.getLogger(__name__)

class EventRepository:
    """Repository for structured memory events."""

    async def insert_event(self, event: EventEnvelope, owner_id: str) -> Optional[str]:
        """
        Insert a structured event with idempotency check.
        Returns the event_id (UUID string) if successful or found existing.
        """
        # Ensure event_id exists (Application-side primary key generation for compatibility)
        ev_id = event.event_id or str(uuid.uuid4())

        sql_pg = """
        INSERT INTO ef_memory_events (
            event_id, owner_id, main_type, subtype, event_time, subject, predicate, object,
            quantity, quantity_unit, amount, currency, currency_source,
            confidence, source_msg_id, needs_confirmation, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        ) 
        ON CONFLICT (owner_id, source_msg_id, main_type, subtype, subject, predicate, object) 
        DO UPDATE SET updated_at = CURRENT_TIMESTAMP
        RETURNING event_id;
        """
        
        sql_sqlite = """
        INSERT INTO ef_memory_events (
            event_id, owner_id, main_type, subtype, event_time, subject, predicate, object,
            quantity, quantity_unit, amount, currency, currency_source,
            confidence, source_msg_id, needs_confirmation, metadata
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(owner_id, source_msg_id, main_type, subtype, subject, predicate, object) 
        DO UPDATE SET updated_at = CURRENT_TIMESTAMP
        RETURNING event_id;
        """

        # Prepare parameters
        async with get_db() as conn:
            is_pg = hasattr(conn, 'fetchval')
            
        def db_val(v):
            if not is_pg and isinstance(v, Decimal):
                return str(v)
            return v

        params = [
            ev_id,
            owner_id,
            event.main_type.value,
            event.subtype,
            event.event_time,
            event.subject,
            event.predicate,
            event.object,
            db_val(event.payload.quantity),
            event.payload.quantity_unit,
            db_val(event.payload.amount),
            event.payload.currency,
            event.payload.currency_source,
            event.confidence,
            event.source_msg_id,
            event.needs_confirmation or event.normalization.needs_confirmation,
            json.dumps(event.metadata, ensure_ascii=False)
        ]

        try:
            async with get_db() as conn:
                if is_pg:
                    event_id = await conn.fetchval(sql_pg, *params)
                    return str(event_id)
                else:
                    # SQLite fallback
                    try:
                        cur = await conn.execute(sql_sqlite, params)
                        row = await cur.fetchone()
                        await conn.commit()
                        if row:
                            return str(row[0])
                    except Exception as e:
                        logger.debug("[EventRepo] SQLite INSERT fallback: %s", e)
                        find_sql = "SELECT event_id FROM ef_memory_events WHERE owner_id=? AND source_msg_id=? AND main_type=? AND subtype=? AND subject=? AND predicate=? AND object=?"
                        cur = await conn.execute(find_sql, (params[1], params[14], params[2], params[3], params[5], params[6], params[7]))
                        row = await cur.fetchone()
                        if row:
                            return str(row[0])
                        
                        insert_only = sql_sqlite.split("ON CONFLICT")[0]
                        await conn.execute(insert_only, params)
                        await conn.commit()
                        return ev_id
        except Exception as exc:
            logger.error("[EventRepo] Failed to insert event: %s", exc)
            return None

    async def list_events(self, 
                          owner_id: str,
                          main_type: Optional[MainEventType] = None, 
                          time_start: Optional[datetime] = None,
                          time_end: Optional[datetime] = None,
                          limit: int = 50, 
                          offset: int = 0) -> List[Dict[str, Any]]:
        """List events with tenant and time filtering."""
        params = []
        
        try:
            async with get_db() as conn:
                is_pg = hasattr(conn, 'fetch')
                idx = 1
                
                def next_p():
                    nonlocal idx
                    p = f"${idx}" if is_pg else "?"
                    idx += 1
                    return p

                where_clauses = [f"owner_id = {next_p()}"]
                params.append(owner_id)
                
                if main_type:
                    where_clauses.append(f"main_type = {next_p()}")
                    params.append(main_type.value)
                
                if time_start:
                    where_clauses.append(f"event_time >= {next_p()}")
                    params.append(time_start)
                if time_end:
                    where_clauses.append(f"event_time <= {next_p()}")
                    params.append(time_end)
                    
                sql = "SELECT * FROM ef_memory_events"
                if where_clauses:
                    sql += " WHERE " + " AND ".join(where_clauses)
                    
                sql += f" ORDER BY event_time DESC, created_at DESC LIMIT {limit} OFFSET {offset}"
                
                if is_pg:
                    rows = await conn.fetch(sql, *params)
                else:
                    cur = await conn.execute(sql, params)
                    rows = await cur.fetchall()
                
                return [dict(row) for row in rows]
        except Exception as exc:
            logger.error("[EventRepo] Failed to list events: %s", exc)
            return []

    async def aggregate_events(self, 
                               owner_id: str,
                               main_type: MainEventType, 
                               subtype: Optional[str] = None,
                               time_start: Optional[datetime] = None,
                               time_end: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Aggregate financial amounts with strict tenant and time filtering."""
        params = []
        try:
            async with get_db() as conn:
                is_pg = hasattr(conn, 'fetch')
                idx = 1
                def next_p():
                    nonlocal idx
                    p = f"${idx}" if is_pg else "?"
                    idx += 1
                    return p

                where_clauses = [f"owner_id = {next_p()}", f"main_type = {next_p()}"]
                params.extend([owner_id, main_type.value])
                
                if subtype:
                    where_clauses.append(f"subtype = {next_p()}")
                    params.append(subtype)
                if time_start:
                    where_clauses.append(f"event_time >= {next_p()}")
                    params.append(time_start)
                if time_end:
                    where_clauses.append(f"event_time <= {next_p()}")
                    params.append(time_end)
                    
                sql = f"""
                SELECT currency, SUM(amount) as total_amount, COUNT(*) as count
                FROM ef_memory_events
                WHERE {" AND ".join(where_clauses)}
                GROUP BY currency
                """
                
                if is_pg:
                    rows = await conn.fetch(sql, *params)
                else:
                    cur = await conn.execute(sql, params)
                    rows = await cur.fetchall()
                return [dict(row) for row in rows]
        except Exception as exc:
            logger.error("[EventRepo] Aggregate failed: %s", exc)
            return []

    async def link_evidence(self, event_uuid: str, message_id: int):
        """Link an event to its source evidence message."""
        try:
            async with get_db() as conn:
                is_pg = hasattr(conn, 'execute') and hasattr(conn, 'fetch')
                p1, p2 = ("$1", "$2") if is_pg else ("?", "?")
                
                sql = f"""
                INSERT INTO ef_event_evidence_links (event_uuid, message_id)
                VALUES ({p1}, {p2}) ON CONFLICT DO NOTHING
                """
                if is_pg:
                    await conn.execute(sql, event_uuid, message_id)
                else:
                    await conn.execute(sql, (event_uuid, message_id))
                    await conn.commit()
        except Exception as exc:
            logger.error("[EventRepo] Link evidence failed: %s", exc)
