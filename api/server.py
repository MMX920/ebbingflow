import sys
import os
import asyncio
import time
import json
import secrets
import gc
import shutil
import tempfile
import zipfile
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import FileResponse
import uvicorn
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import webbrowser
import threading
import logging
from starlette.websockets import WebSocketState

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
logger = logging.getLogger(__name__)

from core.chat_engine import get_standard_engine
from core.session import ChatSession
from config import server_config, neo4j_config, identity_config, memory_config, llm_config
from memory.identity.state_reducer import reduce_identity_state
from memory.identity.conflict_resolver import ConflictResolver, ConflictCandidate
from memory.identity.manager import PersonaManager
from memory.knowledge_engine import KnowledgeBaseEngine
from memory.integration.crm_sync import CRMChange, CRMUpsertRequest, CRMReplayRequest, normalize_crm_payload, build_conflict_candidates, make_idempotency_key
from memory.integration.cdc_checkpoint import CDCCheckpointManager
from neo4j import AsyncGraphDatabase
from bridge.llm import LLMBridge

# Global Instances
engine = None
session = None
global_db_driver = None 
checkpoint_manager = None
crm_audit_cache = []
crm_replay_stats = {"processed": 0, "skipped": 0, "failed": 0}
restore_demo_lock = asyncio.Lock()
runtime_restore_in_progress = False

DEMO_BACKUP_REQUIRED_FILES = [
    "neo4j_snapshot.json",
    os.path.join(".data_fs", "ef_history.db"),
    os.path.join(".data_fs", "cdc_outbox.db"),
    os.path.join(".data_fs", "cdc_checkpoint.db"),
    os.path.join(".data_fs", "chroma", "chroma.sqlite3"),
]

# Temporal Audit Configuration
INCLUDE_INVALIDATED = False
INFERENCE_MIN_EVIDENCE = int(os.getenv("INFERENCE_MIN_EVIDENCE", "3"))
INFERENCE_AUTO_INTERVAL = max(1, int(os.getenv("INFERENCE_AUTO_INTERVAL", "6")))


def _quote_cypher_name(name: str) -> str:
    """Quote a label/type from trusted backup data for dynamic Cypher."""
    clean = str(name or "").replace("`", "``")
    return f"`{clean}`"


def _missing_demo_backup_files(backup_dir: str) -> list[str]:
    return [
        os.path.join(backup_dir, rel_path)
        for rel_path in DEMO_BACKUP_REQUIRED_FILES
        if not os.path.exists(os.path.join(backup_dir, rel_path))
    ]


def _safe_extract_zip(zip_path: str, destination: str):
    dest_abs = os.path.abspath(destination)
    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            target = os.path.abspath(os.path.join(destination, member.filename))
            if target != dest_abs and not target.startswith(dest_abs + os.sep):
                raise ValueError(f"Unsafe path in demo backup zip: {member.filename}")
        archive.extractall(destination)


def _find_demo_backup_dir(root: str) -> str:
    if not _missing_demo_backup_files(root):
        return root

    for current, dirs, _files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in {"__pycache__"}]
        if not _missing_demo_backup_files(current):
            return current

    raise FileNotFoundError("Demo backup zip does not contain the required snapshot layout.")


async def _close_runtime_handles_for_restore():
    """Release in-process handles before replacing Windows-locked SQLite files."""
    global engine, session, global_db_driver, checkpoint_manager

    old_session = session
    session = None
    engine = None

    if old_session is not None:
        history_repo = getattr(old_session, "history_repo", None)
        storer = getattr(history_repo, "_storer", None)
        client = getattr(storer, "client", None)
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                logger.debug("[DemoRestore] Chroma client close skipped: %s", exc)

    if checkpoint_manager is not None:
        try:
            checkpoint_manager.close()
        except Exception as exc:
            logger.debug("[DemoRestore] checkpoint close skipped: %s", exc)
        checkpoint_manager = None

    if global_db_driver is not None:
        try:
            await global_db_driver.close()
        except Exception as exc:
            logger.debug("[DemoRestore] Neo4j driver close skipped: %s", exc)
        global_db_driver = None

    try:
        from memory.sql.pool import close_pool
        await close_pool()
    except Exception as exc:
        logger.debug("[DemoRestore] SQL pool close skipped: %s", exc)

    try:
        from chromadb.api.shared_system_client import SharedSystemClient
        systems = list(SharedSystemClient._identifier_to_system.values())
        for chroma_system in systems:
            try:
                chroma_system.stop()
            except Exception as exc:
                logger.debug("[DemoRestore] Chroma shared system stop skipped: %s", exc)
        SharedSystemClient.clear_system_cache()
    except Exception as exc:
        logger.debug("[DemoRestore] Chroma shared cache clear skipped: %s", exc)

    gc.collect()
    await asyncio.sleep(0.25)


async def _close_active_websockets_for_restore():
    connections = list(globals().get("active_connections", set()))
    for ws in connections:
        try:
            if (
                ws.client_state == WebSocketState.CONNECTED
                and ws.application_state == WebSocketState.CONNECTED
            ):
                await ws.close(code=1012, reason="demo data restore")
        except Exception as exc:
            logger.debug("[DemoRestore] websocket close skipped: %s", exc)
        finally:
            _drop_connection(ws)


async def _restore_neo4j_snapshot(backup_dir: str) -> tuple[int, int]:
    snapshot_path = os.path.join(backup_dir, "neo4j_snapshot.json")
    if not os.path.exists(snapshot_path):
        raise FileNotFoundError(f"Missing Neo4j snapshot: {snapshot_path}")

    with open(snapshot_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri,
        auth=(neo4j_config.username, neo4j_config.password),
    )
    try:
        async with driver.session(database=neo4j_config.database) as db:
            delete_result = await db.run("MATCH (n) DETACH DELETE n")
            await delete_result.consume()

            id_map = {}
            nodes_by_labels = {}
            for raw_node in data.get("nodes", []):
                node = dict(raw_node)
                old_eid = node.pop("__eid__", None)
                labels = tuple(sorted(node.pop("__labels__", ["Entity"]) or ["Entity"]))
                nodes_by_labels.setdefault(labels, []).append({"old_eid": old_eid, "props": node})

            for labels, node_list in nodes_by_labels.items():
                label_str = ":".join(_quote_cypher_name(label) for label in labels)
                query = (
                    f"UNWIND $batch AS item "
                    f"CREATE (n:{label_str}) SET n = item.props "
                    f"RETURN item.old_eid AS old, elementId(n) AS new"
                )
                res = await db.run(query, batch=node_list)
                async for record in res:
                    if record["old"]:
                        id_map[record["old"]] = record["new"]

            rels_by_type = {}
            for rel in data.get("rels", []):
                a_new = id_map.get(rel.get("a_eid") or rel.get("a_id"))
                b_new = id_map.get(rel.get("b_eid") or rel.get("b_id"))
                rtype = rel.get("rel_type")
                if a_new and b_new and rtype:
                    rels_by_type.setdefault(rtype, []).append({
                        "a": a_new,
                        "b": b_new,
                        "p": rel.get("rel_props") or {},
                    })

            for rtype, rel_list in rels_by_type.items():
                quoted_type = _quote_cypher_name(rtype)
                for i in range(0, len(rel_list), 5000):
                    batch = rel_list[i:i + 5000]
                    rel_result = await db.run(
                        f"""
                        UNWIND $batch AS r
                        MATCH (a), (b)
                        WHERE elementId(a) = r.a AND elementId(b) = r.b
                        CREATE (a)-[rel:{quoted_type}]->(b)
                        SET rel = r.p
                        """,
                        batch=batch,
                    )
                    await rel_result.consume()
        return len(data.get("nodes", [])), len(data.get("rels", []))
    finally:
        await driver.close()


def _restore_demo_data_files(backup_dir: str):
    src = os.path.join(backup_dir, ".data_fs")
    dst = os.path.join(BASE_DIR, ".data")
    if not os.path.isdir(src):
        raise FileNotFoundError(f"Missing local data snapshot: {src}")

    for attempt in range(1, 6):
        try:
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            return
        except PermissionError:
            gc.collect()
            time.sleep(0.4 * attempt)
            if attempt == 5:
                raise


async def _initialize_runtime_after_restore():
    global engine, session, global_db_driver, checkpoint_manager

    engine = get_standard_engine()
    backend = identity_config.chat_history_backend
    if backend == "sql":
        from scripts.setup_db import setup_db
        await setup_db()
        from memory.history.repository import SqlHistoryRepository
        history_repo = SqlHistoryRepository()
    else:
        from memory.history.repository import ChromaHistoryRepository
        history_repo = ChromaHistoryRepository()

    session = ChatSession(
        session_id="master_session",
        user_id=identity_config.user_id,
        history_repo=history_repo,
    )
    await session.restore_from_repo()

    global_db_driver = AsyncGraphDatabase.driver(
        neo4j_config.uri,
        auth=(neo4j_config.username, neo4j_config.password),
    )
    checkpoint_manager = CDCCheckpointManager()

    try:
        from memory.vector.storer import VectorStorer
        v_storer = VectorStorer()
        session.context_canvas["vector_status"] = "active"
        session.context_canvas["vector_chat_count"] = v_storer.get_chat_count()
        close = getattr(getattr(v_storer, "client", None), "close", None)
        if callable(close):
            close()
    except Exception as exc:
        logger.warning("[DemoRestore] Vector reinitialization degraded: %s", exc)
        session.context_canvas["vector_status"] = "degraded"

def clean_neo4j_data(obj):
    if isinstance(obj, list):
        return [clean_neo4j_data(i) for i in obj]
    if isinstance(obj, dict):
        return {k: clean_neo4j_data(v) for k, v in obj.items()}
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "__str__") and "DateTime" in str(type(obj)):
        return str(obj)
    return obj


def _extract_ws_token(websocket: WebSocket) -> str:
    auth_header = str(websocket.headers.get("authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        if token:
            return token

    header_token = str(websocket.headers.get("x-ws-token") or "").strip()
    if header_token:
        return header_token

    query_key = str(getattr(server_config, "ws_auth_query_param", "ws_token") or "ws_token").strip()
    query_token = str(websocket.query_params.get(query_key) or websocket.query_params.get("token") or "").strip()
    return query_token


def _is_ws_authorized(websocket: WebSocket) -> bool:
    if not getattr(server_config, "ws_auth_required", False):
        return True

    expected = str(getattr(server_config, "ws_auth_token", "") or "").strip()
    if not expected:
        logger.error("[WS_AUTH] WS_AUTH_REQUIRED=true but WS_AUTH_TOKEN is empty. Denying connection.")
        return False

    presented = _extract_ws_token(websocket)
    if not presented:
        return False
    return secrets.compare_digest(presented, expected)


def _extract_http_token(request: Request) -> str:
    auth_header = str(request.headers.get("authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        if token:
            return token

    for key in ("x-maintenance-token", "x-ws-token", "x-api-key"):
        token = str(request.headers.get(key) or "").strip()
        if token:
            return token

    query_key = str(getattr(server_config, "ws_auth_query_param", "ws_token") or "ws_token").strip()
    query_token = str(request.query_params.get(query_key) or request.query_params.get("token") or "").strip()
    return query_token


def _is_maintenance_authorized(request: Request) -> bool:
    expected = str(getattr(server_config, "maintenance_token", "") or "").strip()
    if not expected:
        expected = str(getattr(server_config, "ws_auth_token", "") or "").strip()
    if not expected:
        client_host = str(getattr(getattr(request, "client", None), "host", "") or "").strip()
        if (
            not getattr(server_config, "ws_auth_required", False)
            and client_host in {"127.0.0.1", "::1", "localhost"}
        ):
            logger.warning("[MAINTENANCE_AUTH] No token configured; allowing loopback request because WS_AUTH_REQUIRED=false.")
            return True
        logger.error("[MAINTENANCE_AUTH] Missing MAINTENANCE_TOKEN/WS_AUTH_TOKEN. Denying request.")
        return False

    presented = _extract_http_token(request)
    if not presented:
        return False
    return secrets.compare_digest(presented, expected)

async def run_identity_reinference(uid: str, force: bool = False, source: str = "auto") -> dict:
    """Run personality re-inference with optional evidence threshold gating."""
    manager = None
    try:
        manager = PersonaManager()
        evidence_count = 0
        fact_count = 0
        async with manager._driver.session(database=manager.database) as neo_session:
            count_res = await neo_session.run(
                """
                MATCH (:Entity {entity_id:$uid})-[:HAS_FACT]->(f:PersonalityEvidence)
                RETURN count(f) AS pe
                """,
                uid=uid,
            )
            count_record = await count_res.single()
            evidence_count = int((count_record or {}).get("pe", 0) or 0)
            fact_res = await neo_session.run(
                """
                MATCH (:Entity {entity_id:$uid})-[:HAS_FACT]->(f:Fact)
                RETURN count(f) AS fc
                """,
                uid=uid,
            )
            fact_record = await fact_res.single()
            fact_count = int((fact_record or {}).get("fc", 0) or 0)

        if not force and evidence_count < INFERENCE_MIN_EVIDENCE:
            # Auto fallback path: graph personality evidence is sparse,
            # but we can still infer from SQL dialogue history.
            fallback = await _fallback_infer_from_recent_dialogue(uid)
            if fallback.get("status") == "success":
                fallback["source"] = source
                fallback["fallback_used"] = True
                fallback["evidence_count"] = evidence_count
                fallback["fact_count"] = fact_count
                fallback["min_required"] = INFERENCE_MIN_EVIDENCE
                return fallback
            return {
                "status": "skipped",
                "reason": "insufficient_evidence",
                "source": source,
                "evidence_count": evidence_count,
                "fact_count": fact_count,
                "min_required": INFERENCE_MIN_EVIDENCE,
            }

        settlement = await manager.settle_personality_evidence(uid, lookback_days=30)
        selected_refs = list(settlement.get("selected_refs") or [])
        changed = await manager.re_infer_identity(
            uid,
            evidence_refs=selected_refs,
            audit_summary=settlement,
        )

        # Fallback: when user forces inference but graph evidence is empty,
        # infer from recent dialogue text to avoid a dead-end UX.
        if force and not changed and evidence_count == 0:
            fallback = await _fallback_infer_from_recent_dialogue(uid)
            if fallback.get("status") == "success":
                fallback["source"] = source
                fallback["fallback_used"] = True
                fallback["evidence_count"] = evidence_count
                fallback["fact_count"] = fact_count
                fallback["min_required"] = INFERENCE_MIN_EVIDENCE
                return fallback

        profile = await manager.get_user_profile_struct(uid)
        return {
            "status": "success" if changed else "no_change",
            "source": source,
            "evidence_count": evidence_count,
            "fact_count": fact_count,
            "min_required": INFERENCE_MIN_EVIDENCE,
            "changed": bool(changed),
            "audit_summary": settlement,
            "mbti_label": profile.get("mbti_label") or "",
            "big_five": profile.get("big_five") or {},
            "inference_reasoning": profile.get("inference_reasoning") or "",
        }
    except Exception as e:
        logger.error("[IdentityInference] failed (%s): %s", source, e)
        return {"status": "error", "source": source, "message": str(e)}
    finally:
        if manager is not None:
            try:
                await manager.close()
            except Exception:
                pass

async def _fallback_infer_from_recent_dialogue(uid: str) -> dict:
    try:
        recent_user_msgs = await _load_recent_user_dialogue_from_sql(uid, max_turns=12, max_scan=120)
        if not recent_user_msgs and session is not None and hasattr(session, "history"):
            for msg in reversed(session.history[-40:]):
                role = str(getattr(msg, "role", "") or "").strip().lower()
                content = str(getattr(msg, "content", "") or "").strip()
                if role == "user" and content:
                    recent_user_msgs.append(content)
                if len(recent_user_msgs) >= 12:
                    break
            recent_user_msgs = list(reversed(recent_user_msgs))
        if not recent_user_msgs:
            return {"status": "no_change", "reason": "no_recent_user_dialogue"}

        bridge = LLMBridge(llm_config, category="persona_inference_fallback")
        dialogue = "\n".join([f"- {line}" for line in recent_user_msgs])
        prompt = f"""
你是人格分析助手。基于用户最近对话，给出一个“可回溯但保守”的人格估计。
只输出 JSON，不要解释，不要 Markdown。
要求字段：
{{
  "mbti_label": "INTJ",
  "big_five": {{
    "openness": 0.0,
    "conscientiousness": 0.0,
    "extraversion": 0.0,
    "agreeableness": 0.0,
    "neuroticism": 0.0
  }},
  "reasoning": "一句到两句理由"
}}
最近用户对话：
{dialogue}
"""
        raw = await bridge.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        if not raw:
            return {"status": "no_change", "reason": "fallback_llm_empty"}
        data = json.loads(raw)

        mbti = str((data or {}).get("mbti_label") or "").strip().upper()
        bf = (data or {}).get("big_five") or {}
        reasoning = str((data or {}).get("reasoning") or "").strip()
        normalized_bf = {}
        for key in ["openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism"]:
            try:
                val = bf.get(key)
                if val is None:
                    continue
                normalized_bf[key] = max(0.0, min(1.0, float(val)))
            except Exception:
                continue
        if not normalized_bf:
            ext = 0.65 if mbti.startswith("E") else 0.35
            neu = 0.40 if mbti.startswith("T") else 0.55
            normalized_bf = {
                "openness": 0.58,
                "conscientiousness": 0.56,
                "extraversion": ext,
                "agreeableness": 0.52,
                "neuroticism": neu,
            }

        manager = PersonaManager()
        try:
            changed = await manager.apply_long_term_persona_hints(
                uid,
                mbti_label=mbti or None,
                big_five=normalized_bf or None,
            )
            # If momentum update is too small to pass threshold, force-write fallback vector.
            profile_after_hint = await manager.get_user_profile_struct(uid)
            if not (profile_after_hint.get("big_five") or {}):
                now = time.strftime("%Y-%m-%dT%H:%M:%S")
                async with manager._driver.session(database=manager.database) as neo_session:
                    await neo_session.run(
                        """
                        MATCH (u:Entity {entity_id:$uid})
                        SET u.big_five_openness = $o,
                            u.big_five_conscientiousness = $c,
                            u.big_five_extraversion = $e,
                            u.big_five_agreeableness = $a,
                            u.big_five_neuroticism = $n,
                            u.persona_updated_at = $now
                        """,
                        uid=uid,
                        o=float(normalized_bf.get("openness", 0.55)),
                        c=float(normalized_bf.get("conscientiousness", 0.55)),
                        e=float(normalized_bf.get("extraversion", 0.50)),
                        a=float(normalized_bf.get("agreeableness", 0.50)),
                        n=float(normalized_bf.get("neuroticism", 0.50)),
                        now=now,
                    )
                changed = True
            if reasoning:
                now = time.strftime("%Y-%m-%dT%H:%M:%S")
                async with manager._driver.session(database=manager.database) as neo_session:
                    await neo_session.run(
                        """
                        MATCH (u:Entity {entity_id:$uid})
                        SET u.personality_inference_reasoning = $reasoning,
                            u.persona_updated_at = $now
                        """,
                        uid=uid,
                        reasoning=reasoning,
                        now=now,
                    )
            profile = await manager.get_user_profile_struct(uid)
            return {
                "status": "success" if changed else "no_change",
                "source": "manual_dialogue_fallback",
                "changed": bool(changed),
                "mbti_label": profile.get("mbti_label") or "",
                "big_five": profile.get("big_five") or {},
                "inference_reasoning": profile.get("inference_reasoning") or "",
            }
        finally:
            await manager.close()
    except Exception as e:
        logger.error("[IdentityInference] fallback failed: %s", e)
        return {"status": "error", "source": "manual_dialogue_fallback", "message": str(e)}

async def _load_recent_user_dialogue_from_sql(uid: str, max_turns: int = 12, max_scan: int = 120) -> list[str]:
    """Load recent user utterances from SQL history, stable across server restarts."""
    try:
        from memory.sql.pool import get_db
        async with get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            if is_sqlite:
                sql = """
                SELECT m.content AS content
                FROM ef_chat_messages m
                JOIN ef_chat_sessions s ON s.session_id = m.session_id
                WHERE s.user_id = ? AND m.role = 'user'
                ORDER BY m.id DESC
                LIMIT ?
                """
                cursor = await conn.execute(sql, (uid, max_scan))
                rows = await cursor.fetchall()
            else:
                sql = """
                SELECT m.content AS content
                FROM ef_chat_messages m
                JOIN ef_chat_sessions s ON s.session_id = m.session_id
                WHERE s.user_id = $1 AND m.role = 'user'
                ORDER BY m.id DESC
                LIMIT $2
                """
                rows = await conn.fetch(sql, uid, max_scan)

        msgs = []
        for row in rows or []:
            text = str((row["content"] if isinstance(row, dict) else row["content"]) or "").strip()
            if text:
                msgs.append(text)
            if len(msgs) >= max_turns:
                break
        return list(reversed(msgs))
    except Exception as e:
        logger.debug("[IdentityInference] SQL dialogue load skipped: %s", e)
        return []


async def _load_monitor_dialogue_stream(uid: str, limit: int = 1200) -> list[dict]:
    """Load monitor dialogue history directly from SQL across sessions."""
    try:
        from memory.sql.pool import get_db
        async with get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            if is_sqlite:
                sql = """
                SELECT m.id, m.role, m.speaker AS name, m.content, m.timestamp, m.session_id
                FROM ef_chat_messages m
                JOIN ef_chat_sessions s ON s.session_id = m.session_id
                WHERE s.user_id = ?
                ORDER BY m.id DESC
                LIMIT ?
                """
                cursor = await conn.execute(sql, (uid, limit))
                rows = await cursor.fetchall()
            else:
                sql = """
                SELECT m.id, m.role, m.speaker AS name, m.content, m.timestamp, m.session_id
                FROM ef_chat_messages m
                JOIN ef_chat_sessions s ON s.session_id = m.session_id
                WHERE s.user_id = $1
                ORDER BY m.id DESC
                LIMIT $2
                """
                rows = await conn.fetch(sql, uid, limit)
        history = []
        for row in reversed(rows or []):
            item = dict(row)
            history.append({
                "role": item.get("role"),
                "content": item.get("content"),
                "timestamp": item.get("timestamp"),
                "name": item.get("name"),
                "msg_id": item.get("id"),
                "session_id": item.get("session_id"),
            })
        return history
    except Exception as e:
        logger.debug("[Monitor] SQL dialogue stream load skipped: %s", e)
        return []


def _session_history_payload() -> list[dict]:
    return [
        msg.to_dict()
        for msg in session.history
        if not (
            getattr(msg, "role", None) == "assistant"
            and _is_transient_ai_error_message(getattr(msg, "content", ""))
        )
    ]

@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, session, global_db_driver
    engine = get_standard_engine()
    backend = identity_config.chat_history_backend
    if backend == "sql":
        try:
            from scripts.setup_db import setup_db
            await setup_db()
            from scripts.initialize_neo4j import initialize_neo4j
            await initialize_neo4j()
        except Exception as e:
            print(f"[Bootstrap] Auto DB Setup failed (continuing anyway): {e}")

        from memory.history.repository import SqlHistoryRepository
        history_repo = SqlHistoryRepository()
        print(f"[Bootstrap] Using SQL History Backend (Primary)")
    else:
        from memory.history.repository import ChromaHistoryRepository
        history_repo = ChromaHistoryRepository()
        print(f"[Bootstrap] Using Chroma History Backend (Legacy/RAG Only)")

    session = ChatSession(session_id="master_session", user_id=identity_config.user_id, history_repo=history_repo)
    await session.restore_from_repo()
    
    global_db_driver = AsyncGraphDatabase.driver(
        neo4j_config.uri, 
        auth=(neo4j_config.username, neo4j_config.password)
    )
    
    global checkpoint_manager
    checkpoint_manager = CDCCheckpointManager()

    try:
        from memory.identity.manager import PersonaManager
        p_manager = PersonaManager()
        await p_manager.bootstrap_genesis_identities(session.user_id)
        await p_manager.close()
    except Exception as ge:
        logger.error(f"[GENESIS_ERROR] bootstrap_genesis_identities failed: {ge}")

    if len(session.history) == 0:
        asst_name = identity_config.default_asst_name
        try:
            async with global_db_driver.session(database=neo4j_config.database) as db_session:
                res = await db_session.run(
                    "MATCH (a:Entity {entity_id: $aid, owner_id: $uid}) RETURN a.name AS name",
                    aid=identity_config.assistant_id,
                    uid=session.user_id
                )
                record = await res.single()
                if record and record["name"]:
                    session.identity_state = reduce_identity_state(session.identity_state, {
                        "asst_name": record["name"],
                        "source": "default"
                    })
                    asst_name = session.identity_state.get("asst_name")
                    welcome_msg = f"Hello, I am {asst_name}. Nice to see you again."
                else:
                    asst_name = identity_config.default_asst_name
                    welcome_msg = f"Hello, I am {asst_name}. The system is online. How should I address you?"
        except Exception as se:
            logger.debug(f"Welcome message name fetch failed, fallback default: {se}")
            welcome_msg = f"Hello, I am {asst_name}. The system is online. How should I address you?"
        session.add_assistant_message(welcome_msg)

    from config import embed_config
    try:
        from memory.vector.storer import VectorStorer
        v_storer = VectorStorer()
        print(f"[Bootstrap] Vector Engine: {embed_config.type} | Model: {embed_config.model}")
        print(f"[Stats] Total Chat Records in RAG: {v_storer.get_chat_count()}")
    except Exception as e:
        logger.exception("[VECTOR_BOOTSTRAP_FAILED] Vector storer self-check failed: %s", e)
        session.context_canvas["vector_status"] = "degraded"
        
    print(f"[Session] Restored {len(session.history)} messages for {session.user_id}")
    print("EbbingFlow Standard Engine Initialized (Global Driver Active)")
    
    if os.environ.get("EBBINGFLOW_BROWSER_POPPED") != "1":
        def launch_browser():
            time.sleep(2.0)  # keep startup banner visible before opening browser tabs
            webbrowser.open("http://localhost:8000")
            webbrowser.open("http://localhost:8000/monitor")
            print("[System] UI Components Launched (First Run).")
        
        threading.Thread(target=launch_browser, daemon=True).start()
        os.environ["EBBINGFLOW_BROWSER_POPPED"] = "1"

    print("\n" + "="*50)
    print("ALL SYSTEMS GO! You can access Andrew here:")
    print("Interaction Hub:  http://localhost:8000")
    print("Data Monitor:    http://localhost:8000/monitor")
    print("="*50 + "\n")

    from config import postgres_config
    if postgres_config.is_configured():
        from memory.sql.pool import get_pool
        sql_pool = await get_pool()
        if sql_pool:
            print(f"[SQL] PostgreSQL pool ready (tenant={postgres_config.tenant_id or 'not set'})")
        else:
            print("[SQL] PostgreSQL configured but pool creation failed ??CRM queries disabled")
    else:
        print("[SQL] PostgreSQL not fully configured ??CRM queries disabled (set POSTGRES_DSN or POSTGRES_PASSWORD)")

    yield
    print("EbbingFlow Brain Shutting Down")
    if global_db_driver:
        await global_db_driver.close()
    if checkpoint_manager:
        checkpoint_manager.close()
    from memory.sql.pool import close_pool
    await close_pool()

app = FastAPI(lifespan=lifespan)
active_connections: set[WebSocket] = set()

def _is_ws_disconnect_error(exc: Exception) -> bool:
    """Return True for expected websocket disconnect-style errors."""
    if isinstance(exc, WebSocketDisconnect):
        return True
    msg = str(exc).lower().strip()
    if not msg:
        return isinstance(exc, (RuntimeError, ConnectionError, BrokenPipeError))
    return any(k in msg for k in [
        "not connected", "accept", "disconnected", 
        "websocket is not connected", "connection closed"
    ])

def _drop_connection(ws: WebSocket):
    """Safely remove a websocket connection from active set."""
    if ws in active_connections:
        active_connections.remove(ws)


def _runtime_missing_components() -> list[str]:
    missing = []
    if engine is None:
        missing.append("engine")
    if session is None:
        missing.append("session")
    if global_db_driver is None:
        missing.append("neo4j_driver")
    if checkpoint_manager is None:
        missing.append("checkpoint_manager")
    return missing

async def broadcast(message: dict):
    """Broadcast a JSON-safe message to all active websocket clients."""
    clean_message = clean_neo4j_data(message)
    disconnected = []
    for connection in active_connections:
        try:
            if (
                connection.client_state != WebSocketState.CONNECTED
                or connection.application_state != WebSocketState.CONNECTED
            ):
                disconnected.append(connection)
                continue
            await connection.send_json(clean_message)
        except Exception as e:
            if _is_ws_disconnect_error(e):
                disconnected.append(connection)
            else:
                logger.debug("Broadcast encountered unexpected error: %r", e)
                disconnected.append(connection)
    
    for connection in disconnected:
        _drop_connection(connection)

@app.get("/")
async def get_chat():
    return FileResponse(os.path.join(FRONTEND_DIR, "chat_interaction.html"))

@app.get("/monitor")
async def get_monitor():
    return FileResponse(os.path.join(FRONTEND_DIR, "data_monitor.html"))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    if not _is_ws_authorized(websocket):
        await websocket.close(code=1008, reason="unauthorized")
        return
    await websocket.accept()
    active_connections.add(websocket)
    global engine, session, global_db_driver
    missing = _runtime_missing_components()
    if missing:
        await websocket.send_json({
            "type": "status",
            "step": "00",
            "status": "error",
            "reason": "runtime_not_initialized:" + ",".join(missing),
        })
        await websocket.close(code=1011, reason="runtime not initialized")
        _drop_connection(websocket)
        return
    
    async def sync_all():
        if runtime_restore_in_progress:
            if (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                await websocket.send_json({
                    "type": "maintenance",
                    "status": "restoring_demo_data",
                    "message": "Demo data restore in progress.",
                })
            return
        if not global_db_driver: return
        if (
            websocket.client_state != WebSocketState.CONNECTED
            or websocket.application_state != WebSocketState.CONNECTED
        ):
            return
        try:
            async with global_db_driver.session(database=neo4j_config.database) as neosession:
                query = (
                    "MATCH (s:Entity)-[:ACTOR_IN]->(e:Event {owner_id: $uid}) "
                    "WHERE (e.status = 'active' OR e.status IS NULL) AND e.invalid_at IS NULL "
                    "OPTIONAL MATCH (obj_ent:Entity {owner_id: $uid})-[:OBJECT_OF]->(e) "
                    "OPTIONAL MATCH (src:Entity {owner_id: $uid})-[said:SAID]->(e) "
                    "RETURN s.name AS sub, obj_ent.name AS obj, src.name AS source_entity, "
                    "said.trust_score AS trust, properties(e) AS props "
                    "ORDER BY COALESCE(e.record_time, e.created_at) DESC LIMIT 50"
                )
                evt_res = await neosession.run(query, uid=identity_config.user_id)
                event_facts = []
                async for r in evt_res:
                    try:
                        raw_props = r.get('props') or {}
                        p = dict(raw_props)
                        
                        sub_val = r.get('sub') or p.get('subject') or "Unknown"
                        pre_val = p.get('predicate') or "exists"
                        obj_val = r.get('obj') or p.get('object') or ""
                        
                        time_display = p.get('event_time') or p.get('record_time') or p.get('created_at') or p.get('timestamp_reference') or 'SNAPSHOT'
                        
                        p.update({
                            'subject': sub_val, 'predicate': pre_val, 'object': obj_val,
                            'sub': sub_val, 'pre': pre_val, 'obj': obj_val,
                            'source_entity': r.get('source_entity') or p.get('source_entity', 'system_generated'),
                            'trust_score': r.get('trust') or 1.0,
                            'time': time_display,
                            'line': f"{sub_val} {pre_val}" + (f" -> {obj_val}" if obj_val else "")
                        })
                        event_facts.append(p)
                    except Exception as loop_e:
                        print(f"Sync Protocol Loop Error: {loop_e} | Record: {r}")
                        continue

                # Include non-event cognitive facts (v3 persona/efstb oriented) in the same audit stream.
                fact_query = (
                    "MATCH (u:Entity {entity_id: $uid})-[:HAS_FACT]->(f:Fact {owner_id: $uid}) "
                    "WHERE toLower(coalesce(f.predicate, '')) IN $predicates "
                    "RETURN COALESCE(u.primary_name, u.name, u.entity_id) AS sub, properties(f) AS props "
                    "ORDER BY COALESCE(f.updated_at, f.created_at) DESC LIMIT 50"
                )
                fact_predicates = [
                    "long_term_persona",
                    "short_term_efstb",
                    "mbti",
                    "big_five",
                    "core_values",
                    "state",
                    "role",
                    "occupation",
                    "biography",
                ]
                fact_res = await neosession.run(
                    fact_query,
                    uid=identity_config.user_id,
                    predicates=fact_predicates,
                )
                async for fr in fact_res:
                    try:
                        fp = dict(fr.get("props") or {})
                        sub_val = fr.get("sub") or fp.get("subject") or "Unknown"
                        pre_val = fp.get("predicate") or "fact"
                        obj_val = fp.get("object") or ""
                        time_display = fp.get("updated_at") or fp.get("created_at") or "SN_AUTO_GEN"
                        event_facts.append(
                            {
                                **fp,
                                "subject": sub_val,
                                "predicate": pre_val,
                                "object": obj_val,
                                "sub": sub_val,
                                "pre": pre_val,
                                "obj": obj_val,
                                "source_entity": fp.get("source_entity") or "profile_graph",
                                "trust_score": fp.get("confidence") or 0.85,
                                "time": time_display,
                                "line": f"{sub_val} {pre_val}" + (f" -> {obj_val}" if obj_val else ""),
                            }
                        )
                    except Exception as fact_loop_e:
                        print(f"Sync Protocol Fact Loop Error: {fact_loop_e} | Record: {fr}")
                        continue

                invalidated_facts = []
                if INCLUDE_INVALIDATED:
                    inval_query = (
                        "MATCH (s:Entity)-[:ACTOR_IN]->(e:Event {owner_id: $uid, status: 'invalidated'}) "
                        "RETURN s.name AS sub, properties(e) AS props "
                        "ORDER BY e.invalid_at DESC LIMIT 20"
                    )
                    inval_res = await neosession.run(inval_query, uid=identity_config.user_id)
                    async for ir in inval_res:
                         ip = dict(ir.get('props') or {})
                         ip['subject'] = ir.get('sub') or "Unknown"
                         invalidated_facts.append(ip)

                ent_props = {}
                ent_res_meta = await neosession.run("MATCH (e:Entity {owner_id: $uid}) RETURN e", uid=identity_config.user_id)
                async for row in ent_res_meta:
                    node = row['e']
                    ent_props[node.get('name', 'Unknown')] = dict(node)

                rel_query = (
                    "MATCH (a:Entity)-[r:RELATION]->(b:Entity) "
                    "WHERE r.owner_id = $uid AND (r.status = 'active' OR r.status IS NULL) "
                    "RETURN a.name AS from, r.type AS rel, b.name AS to, properties(r) AS props "
                    "ORDER BY COALESCE(r.record_time, r.created_at) DESC LIMIT 50"
                )
                rel_res = await neosession.run(rel_query, uid=identity_config.user_id)
                rel_facts = []
                async for r in rel_res:
                    rd = r.data()
                    p = rd.pop('props', {})
                    rd.update(p)  # merge persona fields into one normalized observation record
                    rel_facts.append(rd)

                ent_rel_query = (
                    "MATCH (a:Entity {owner_id: $uid})-[r:RELATION]->(b:Entity {owner_id: $uid}) "
                    "WHERE r.status = 'active' OR r.status IS NULL "
                    "RETURN a.name as sub, r.type as rel, b.name as obj"
                )
                ent_rel_res = await neosession.run(ent_rel_query, uid=identity_config.user_id)
                async for row in ent_rel_res:
                    s_name, r_type, o_name = row['sub'], row['rel'], row['obj']
                    long_id = f"{s_name} -> {r_type} -> {o_name}"
                    event_facts.append({
                        'subject': s_name, 'predicate': r_type, 'object': o_name,
                        'sub': s_name, 'pre': long_id, 'obj': o_name,
                        'source_entity': 'system_core', 'trust_score': 1.0,
                        'time': 'GENESIS', 'line': f"-> {r_type} ->",
                        'props': {
                            'description': f'Relationship seeded from bootstrap: {r_type}',
                            'source': 'EbbingFlow_Kernel',
                            'logic_chain': long_id
                        }
                    })

                ent_res = await neosession.run("MATCH (e:Entity {owner_id: $uid}) RETURN e.name as name", uid=identity_config.user_id)
                async for er in ent_res:
                    e_name = er['name']
                    if not any(f.get('sub') == e_name or f.get('obj') == e_name for f in event_facts):
                        event_facts.append({
                            'subject': e_name, 'predicate': 'active', 'object': '',
                            'sub': e_name, 'pre': 'active', 'obj': '',
                            'source_entity': 'system_core', 'trust_score': 1.0,
                            'time': 'INIT', 'line': f'{e_name} status active'
                        })
                
                prof_query = (
                    "MATCH (u:Entity {entity_id: $uid, owner_id: $uid}) "
                    "MATCH (a:Entity {entity_id: $aid, owner_id: $uid}) "
                    "RETURN "
                    "COALESCE(u.primary_name, u.name) AS u_name, u.role AS u_role, u.aliases AS u_aliases, u.current_state AS u_state, "
                    "u.age AS u_age, u.gender AS u_gender, "
                    "COALESCE(a.primary_name, a.name) AS a_name, "
                    "COALESCE(a.persona, a.personality_summary) AS a_persona, "
                    "COALESCE(a.role, a.current_role) AS a_role, "
                    "a.age AS a_age, a.gender AS a_gender"
                )
                prof_res = await neosession.run(prof_query, uid=identity_config.user_id, aid=identity_config.assistant_id)
                prof_record = await prof_res.single()
                
                obs_facts = []

            user_name = prof_record['u_name'] if (prof_record and prof_record['u_name']) else 'User'
            asst_name = prof_record['a_name'] if (prof_record and prof_record['a_name']) else "Andrew"
            asst_persona = prof_record['a_persona'] if (prof_record and prof_record['a_persona']) else 'A calm and reliable assistant'
            user_profile_str = ""
            user_profile_struct = {}
            assistant_profile_struct = {}

            up = None
            ident = {}
            p_manager = None
            try:
                from memory.identity.manager import PersonaManager
                p_manager = PersonaManager()
                up = await p_manager.get_user_profile(identity_config.user_id)
                ident = up.identity or {}
                
                user_profile_struct = p_manager.serialize_user_profile(up)
                
                assistant_profile_struct = await p_manager.get_assistant_profile_struct(identity_config.user_id)

                # Deprecated observation categories (trait/preference/goal/...) are removed.
                # Sync payload observations are rebuilt later with v3-only categories.
                
                if not user_profile_struct.get("name"):
                    user_profile_struct["name"] = session.context_canvas.get("user_real_name") or user_name
                if not user_profile_struct.get("primary_name"):
                    user_profile_struct["primary_name"] = user_profile_struct.get("name") or session.context_canvas.get("user_real_name") or user_name
                if not user_profile_struct.get("aliases"):
                    user_profile_struct["aliases"] = list(session.context_canvas.get("user_aliases") or [])
                if not user_profile_struct.get("age") and prof_record:
                    user_profile_struct["age"] = prof_record.get("u_age") or ""
                if not user_profile_struct.get("age"):
                    user_profile_struct["age"] = session.context_canvas.get("user_age") or ""
                if not user_profile_struct.get("gender") and prof_record:
                    user_profile_struct["gender"] = prof_record.get("u_gender") or ""
                if not user_profile_struct.get("gender"):
                    user_profile_struct["gender"] = session.context_canvas.get("user_gender") or ""
                if not user_profile_struct.get("role"):
                    user_profile_struct["role"] = session.context_canvas.get("user_role") or ""

                if not user_profile_struct.get("mbti_label"):
                    user_profile_struct["mbti_label"] = session.context_canvas.get("mbti_label") or ""
                if not user_profile_struct.get("big_five"):
                    _canvas_bf = {
                        "openness":          session.context_canvas.get("big_five_openness"),
                        "conscientiousness": session.context_canvas.get("big_five_conscientiousness"),
                        "extraversion":      session.context_canvas.get("big_five_extraversion"),
                        "agreeableness":     session.context_canvas.get("big_five_agreeableness"),
                        "neuroticism":       session.context_canvas.get("big_five_neuroticism"),
                    }
                    if any(v is not None for v in _canvas_bf.values()):
                        user_profile_struct["big_five"] = {
                            k: round(float(v), 3) for k, v in _canvas_bf.items() if v is not None
                        }
                if not user_profile_struct.get("values"):
                    user_profile_struct["values"] = list(session.context_canvas.get("user_core_values") or [])

                if not assistant_profile_struct.get("name"):
                    assistant_profile_struct["name"] = session.context_canvas.get("assistant_real_name") or asst_name
                if not assistant_profile_struct.get("persona"):
                    assistant_profile_struct["persona"] = (
                        session.context_canvas.get("assistant_profile")
                        or session.context_canvas.get("assistant_prompt_profile")
                        or asst_persona
                    )
                if not assistant_profile_struct.get("role") and prof_record:
                    assistant_profile_struct["role"] = prof_record.get("a_role") or ""
                if not assistant_profile_struct.get("age") and prof_record:
                    assistant_profile_struct["age"] = prof_record.get("a_age") or ""
                if not assistant_profile_struct.get("gender") and prof_record:
                    assistant_profile_struct["gender"] = prof_record.get("a_gender") or ""
                if not assistant_profile_struct.get("role"):
                    assistant_profile_struct["role"] = session.context_canvas.get("assistant_current_role") or ""
                if not assistant_profile_struct.get("age"):
                    assistant_profile_struct["age"] = session.context_canvas.get("assistant_age") or ""
                if not assistant_profile_struct.get("gender"):
                    assistant_profile_struct["gender"] = session.context_canvas.get("assistant_gender") or ""
                if not assistant_profile_struct.get("relationship_to_user"):
                    assistant_profile_struct["relationship_to_user"] = session.context_canvas.get("assistant_relationship_to_user") or ""
                
                parts = []
                UNKNOWN = 'unknown'
                u_age = ident.get("age") or (prof_record.get("u_age") if prof_record else UNKNOWN) or UNKNOWN
                parts.append(f"AGE: {u_age}")
                u_gender = ident.get("gender") or (prof_record.get("u_gender") if prof_record else UNKNOWN) or UNKNOWN
                parts.append(f"GENDER: {u_gender}")
                
                parts.append(f"Role: {ident.get('role') or 'unknown'}")
                if ident.get("aliases"):
                    parts.append(f"Aliases: {', '.join(ident['aliases'])}")
                
                if user_profile_struct.get("mbti_label"):
                    parts.append(f"MBTI: {user_profile_struct.get('mbti_label')}")
                if user_profile_struct.get("big_five"):
                    bf = user_profile_struct.get("big_five") or {}
                    bf_parts = []
                    for dim, score in bf.items():
                        if score is None:
                            continue
                        try:
                            bf_parts.append(f"{dim}={round(float(score), 3)}")
                        except (TypeError, ValueError):
                            continue
                    if bf_parts:
                        parts.append("BigFive: " + ", ".join(bf_parts))
                efstb_tags = session.context_canvas.get("latest_efstb_tags") or {}
                if efstb_tags:
                    parts.append(
                "EFSTB: "
                        + ", ".join(
                            [
                                f"urgency={efstb_tags.get('urgency_level')}",
                                f"granularity={efstb_tags.get('granularity_preference')}",
                                f"instruction={efstb_tags.get('instruction_compliance')}",
                                f"logic={efstb_tags.get('logic_vs_emotion')}",
                            ]
                        )
                    )
                constraints = user_profile_struct.get("constraints") or []
                if constraints:
                    cons_str = ' '.join(f'!!{c}!!' for c in constraints)
                    parts.append(f"Constraints: {cons_str}")
                
                mood = user_profile_struct.get("state") or "Normal"
                parts.append(f"Mood: {mood}")
                
                bio = ident.get("biography") or ident.get("personality_summary") or ident.get("summary") or ident.get("description") or UNKNOWN
                parts.append(f"Bio: {bio}")
                
                user_profile_str = " | ".join(parts)
            except Exception as pe:
                print(f"Sync Persona Error: {pe}")
                fallback_age = prof_record.get('u_age') if (prof_record and prof_record.get('u_age')) else 'unknown'
                fallback_gender = prof_record.get('u_gender') if (prof_record and prof_record.get('u_gender')) else 'unknown'
                user_profile_str = f"AGE: {fallback_age} | GENDER: {fallback_gender} | Role: {user_name} | Mood: Normal"
            finally:
                if p_manager:
                    await p_manager.close()

            def _normalize_rag_item(item):
                if isinstance(item, dict):
                    normalized = dict(item)
                elif hasattr(item, "__dict__"):
                    normalized = {
                        key: value for key, value in vars(item).items()
                        if not key.startswith("_")
                    }
                elif isinstance(item, str):
                    normalized = {"content": item}
                else:
                    return None

                content = normalized.get("content") or normalized.get("text") or normalized.get("summary") or ""
                if not str(content).strip():
                    return None
                normalized["content"] = content
                normalized["source_name"] = (
                    normalized.get("source_name")
                    or normalized.get("source")
                    or normalized.get("source_type")
                    or "unknown_source"
                )
                score = normalized.get("score")
                if score is None:
                    score = normalized.get("final_score")
                if score is None:
                    score = normalized.get("semantic_score")
                if score is not None:
                    normalized["score"] = score
                if "in_prompt" not in normalized:
                    normalized["in_prompt"] = False
                return normalized

            def _is_transient_ai_error_message(text) -> bool:
                raw = str(text or "").strip()
                if not raw:
                    return False
                return (
                    raw.startswith("[AI ERROR:")
                    and (
                        "AllocationQuota.FreeTierOnly" in raw
                        or "free tier of the model has been exhausted" in raw
                        or "Error code: 403" in raw
                    )
                )

            processed_rag = []
            raw_rag = session.context_canvas.get("rag_context_structured", [])
            for item in raw_rag:
                normalized_item = _normalize_rag_item(item)
                if normalized_item:
                    processed_rag.append(normalized_item)

            _STABLE_KEYS = {
                "name", "primary_name", "persona", "age", "gender", "role", "aliases",
                "state", "values", "bio", "mbti_label", "big_five",
                "relationship_to_user"
            }

            def _stabilize(p: dict):
                p = p or {}
                out = {}
                for k in _STABLE_KEYS:
                    if k == "big_five":
                        out[k] = p.get(k) if isinstance(p.get(k), dict) else {}
                    elif k in {"aliases", "values"}:
                        v = p.get(k)
                        out[k] = v if isinstance(v, list) else []
                    else:
                        out[k] = p.get(k, "")
                return out

            user_profile_struct = _stabilize(user_profile_struct)
            assistant_profile_struct = _stabilize(assistant_profile_struct)

            def _clean_scalar(value):
                return str(value or "").strip()

            def _clean_list(values):
                out = []
                for value in values or []:
                    text = _clean_scalar(value)
                    if text and text not in out:
                        out.append(text)
                return out

            session_user_name = _clean_scalar(session.context_canvas.get("user_real_name"))
            session_user_aliases = _clean_list(session.context_canvas.get("user_aliases") or [])
            session_user_age = _clean_scalar(session.context_canvas.get("user_age"))
            session_user_gender = _clean_scalar(session.context_canvas.get("user_gender"))
            session_user_role = _clean_scalar(session.context_canvas.get("user_role"))

            session_asst_name = _clean_scalar(session.context_canvas.get("assistant_real_name"))
            session_asst_aliases = _clean_list(session.context_canvas.get("assistant_aliases") or [])
            session_asst_age = _clean_scalar(session.context_canvas.get("assistant_age"))
            session_asst_gender = _clean_scalar(session.context_canvas.get("assistant_gender"))
            session_asst_role = _clean_scalar(session.context_canvas.get("assistant_current_role"))
            session_asst_persona = _clean_scalar(session.context_canvas.get("assistant_prompt_profile"))
            session_asst_relationship = _clean_scalar(session.context_canvas.get("assistant_relationship_to_user"))

            user_profile_struct["name"] = (
                session_user_name
                or user_profile_struct.get("primary_name")
                or user_profile_struct.get("name")
                or user_name
            )
            assistant_profile_struct["name"] = (
                session_asst_name
                or assistant_profile_struct.get("primary_name")
                or assistant_profile_struct.get("name")
                or asst_name
            )
            if session_user_name:
                user_profile_struct["primary_name"] = session_user_name
            elif not user_profile_struct.get("primary_name"):
                user_profile_struct["primary_name"] = user_profile_struct["name"]
            if session_asst_name:
                assistant_profile_struct["primary_name"] = session_asst_name
            elif not assistant_profile_struct.get("primary_name"):
                assistant_profile_struct["primary_name"] = assistant_profile_struct["name"]

            user_name = user_profile_struct.get("name") or user_name
            if session_user_aliases:
                user_profile_struct["aliases"] = _clean_list(
                    [
                        user_profile_struct.get("primary_name"),
                        *user_profile_struct.get("aliases", []),
                        *session_user_aliases,
                    ]
                )
            elif not user_profile_struct.get("aliases") and user_profile_struct.get("primary_name"):
                user_profile_struct["aliases"] = [user_profile_struct["primary_name"]]

            if session_asst_aliases:
                assistant_profile_struct["aliases"] = _clean_list(
                    [
                        assistant_profile_struct.get("primary_name"),
                        *assistant_profile_struct.get("aliases", []),
                        *session_asst_aliases,
                    ]
                )

            user_profile_struct["age"] = (
                session_user_age
                or user_profile_struct.get("age")
                or (prof_record.get("u_age") if prof_record else "")
                or ""
            )
            user_profile_struct["gender"] = (
                session_user_gender
                or user_profile_struct.get("gender")
                or (prof_record.get("u_gender") if prof_record else "")
                or ""
            )
            if session_user_role:
                user_profile_struct["role"] = session_user_role

            assistant_profile_struct["age"] = (
                session_asst_age
                or assistant_profile_struct.get("age")
                or (prof_record.get("a_age") if prof_record else "")
                or ""
            )
            assistant_profile_struct["gender"] = (
                session_asst_gender
                or assistant_profile_struct.get("gender")
                or (prof_record.get("a_gender") if prof_record else "")
                or ""
            )
            if session_asst_role:
                assistant_profile_struct["role"] = session_asst_role
            if session_asst_persona:
                assistant_profile_struct["persona"] = session_asst_persona
            if session_asst_relationship:
                assistant_profile_struct["relationship_to_user"] = session_asst_relationship

            state_text = (user_profile_struct.get("state") or "").strip()
            if (
                ("??" in state_text or "??" in state_text)
                and not any(
                    token in state_text
                    for token in ["??", "??", "??", "??", "??", "Normal", "Active"]
                )
            ):
                user_profile_struct["state"] = ""

            obs_facts = []

            def _append_final_obs(target, category, values, confidence=0.85, source="profile_graph"):
                target_text = _clean_scalar(target)
                if not target_text:
                    return
                for value in values or []:
                    text = _clean_scalar(value)
                    if not text:
                        continue
                    obs_facts.append(
                        {
                            "target": target_text,
                            "category": category,
                            "content": text,
                            "confidence": confidence,
                            "source": source,
                        }
                    )

            final_user_target = user_profile_struct.get("primary_name") or user_profile_struct.get("name") or user_name
            final_asst_target = assistant_profile_struct.get("primary_name") or assistant_profile_struct.get("name") or asst_name

            def _serialize_big_five(bf: dict) -> str:
                if not isinstance(bf, dict):
                    return ""
                parts = []
                for dim, score in bf.items():
                    if score is None:
                        continue
                    try:
                        parts.append(f"{dim}={round(float(score), 3)}")
                    except (TypeError, ValueError):
                        continue
                return ", ".join(parts)

            user_lt_parts = []
            mbti_label = _clean_scalar(user_profile_struct.get("mbti_label"))
            if mbti_label:
                user_lt_parts.append(f"MBTI={mbti_label}")
            bf_text = _serialize_big_five(user_profile_struct.get("big_five") or {})
            if bf_text:
                user_lt_parts.append(f"BigFive({bf_text})")
            values = _clean_list(user_profile_struct.get("values") or [])
            if values:
                user_lt_parts.append("CoreValues=" + ", ".join(values[:8]))
            if user_lt_parts:
                _append_final_obs(
                    final_user_target,
                    "long_term_persona",
                    [" | ".join(user_lt_parts)],
                    confidence=0.82,
                    source="profile_graph",
                )

            efstb_tags = session.context_canvas.get("latest_efstb_tags") or {}
            if isinstance(efstb_tags, dict) and efstb_tags:
                efstb_line = ", ".join(
                    [
                        f"urgency={efstb_tags.get('urgency_level')}",
                        f"granularity={efstb_tags.get('granularity_preference')}",
                        f"instruction={efstb_tags.get('instruction_compliance')}",
                        f"logic={efstb_tags.get('logic_vs_emotion')}",
                    ]
                )
                _append_final_obs(
                    final_user_target,
                    "short_term_efstb",
                    [efstb_line],
                    confidence=0.8,
                    source="episode_rollup",
                )

            asst_lt_parts = []
            asst_persona = _clean_scalar(assistant_profile_struct.get("persona") or assistant_profile_struct.get("bio"))
            if asst_persona:
                asst_lt_parts.append(f"persona={asst_persona}")
            asst_role = _clean_scalar(assistant_profile_struct.get("role"))
            if asst_role:
                asst_lt_parts.append(f"role={asst_role}")
            if asst_lt_parts:
                _append_final_obs(
                    final_asst_target,
                    "long_term_persona",
                    [" | ".join(asst_lt_parts)],
                    confidence=0.78,
                    source="profile_graph",
                )
            monitor_history = await _load_monitor_dialogue_stream(identity_config.user_id)
            payload = clean_neo4j_data({
                "type": "global_sync", 
                "profile_protocol_version": 2, 
                "user_name": user_name,
                "user_root_profile_struct": user_profile_struct,
                "user_profile_struct": user_profile_struct,           
                "assistant_root_profile_struct": assistant_profile_struct,
                "assistant_profile_struct": assistant_profile_struct, 
                
                # --- Legacy compatibility fields (DO NOT EXTEND) ---
                "user_profile": user_profile_str,     
                "asst_name": asst_name,               
                "asst_persona": asst_persona,         
                "user_info": {                        
                    "role": user_profile_struct.get("role") or 'unknown',
                    "aliases": user_profile_struct.get("aliases") or [],
                    "state": user_profile_struct.get("state") or "Normal",
                    "age": user_profile_struct.get('age', 'unknown'),
                    "gender": user_profile_struct.get('gender', 'unknown')
                },
                "asst_info": {                        # Legacy aggregate
                    "name": assistant_profile_struct.get("name") or asst_name,
                    "persona": assistant_profile_struct.get("persona") or assistant_profile_struct.get("bio") or asst_persona,
                    "role": assistant_profile_struct.get("role") or (prof_record.get("a_role") if prof_record else "assistant"),
                    "age": assistant_profile_struct.get("age") or (prof_record.get("a_age") if prof_record else "28"),
                    "gender": assistant_profile_struct.get("gender") or (prof_record.get("a_gender") if prof_record else "unknown"),
                    "relationship_to_user": assistant_profile_struct.get("relationship_to_user") or "",
                },
                "graph_all": event_facts,
                "graph_invalidated": invalidated_facts, # New field for temporal audit
                "ent_meta": ent_props,
                "rel_all": rel_facts or [],
                "obs_all": obs_facts or [],
                "rag_all": processed_rag,
                "retrieval_audit": session.context_canvas.get("retrieval_audit") or {
                    "mode": "hybrid_baseline",
                    "bm25_enabled": KnowledgeBaseEngine().is_bm25_enabled,
                    "sources": {"graph": 0, "episode": 0, "saga": 0, "vector": 0, "bm25": 0},
                    "result_count": 0
                },
                "time_window_audit": (session.context_canvas.get("retrieval_audit") or {}).get("latency_ms", {}).get("time_window", {"enabled": False}),
                "resolution_audit": session.context_canvas.get("resolution_audit", []),
                "history": monitor_history or _session_history_payload(),
                "conflict_audit": session.identity_state.get("conflict_trace", {}),
                "identity_inference_status": session.context_canvas.get("identity_inference_status") or {},
                "crm_sync_status": "enabled" if identity_config.enable_crm_sync else "disabled",
                "crm_conflict_audit": crm_audit_cache,
                "crm_replay_stats": crm_replay_stats
            })
            if (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                await websocket.send_json(payload)
        except Exception as e:
            if _is_ws_disconnect_error(e):
                _drop_connection(websocket)
                return
            logger.error("Sync Protocol Error: %r", e)

    await sync_all()
    
    try:
        while True:
            data_raw = await websocket.receive_text()
            msg = json.loads(data_raw)
            
            if msg.get("type") == "request_sync":
                await sync_all()
                continue
                
            user_input = msg.get("text", "")
            if not user_input: continue

            await broadcast({"type": "user_input", "text": user_input})

            async def ws_status_callback(step, status, time_ms=None, tokens=None, prompt=None, path=None, reason=None):
                payload = {
                    "type": "status", 
                    "step": step, 
                    "status": status,
                    "time_ms": time_ms,
                    "path": path,
                    "reason": reason
                }
                if tokens:
                    payload["tokens_in"] = tokens.get("input", 0)
                    payload["tokens_out"] = tokens.get("output", 0)
                    payload["tokens_total"] = tokens.get(
                        "total",
                        (tokens.get("input", 0) or 0) + (tokens.get("output", 0) or 0),
                    )
                if prompt:
                    payload["prompt"] = prompt
                await broadcast(payload)

                if step == "01" and status == "done":
                    assistant_struct = session.context_canvas.get("assistant_profile_struct", {})
                    user_name_hint = session.context_canvas.get("user_real_name")
                    await broadcast({
                        "type": "memory_sync",
                        "persona_status": session.context_canvas.get("persona_status", "active"),
                        "vector_status": session.context_canvas.get("vector_status", "active"),
                        "asst_name": session.context_canvas.get("assistant_real_name"),
                        "asst_persona": session.context_canvas.get("assistant_profile"),
                        "assistant_root_profile_struct": assistant_struct,
                        "assistant_profile_struct": assistant_struct,
                        "user_name": user_name_hint,
                        "user_root_profile_struct": {
                            "name": user_name_hint or "",
                            "primary_name": user_name_hint or "",
                            "aliases": list(session.context_canvas.get("user_aliases") or []),
                            "age": session.context_canvas.get("user_age") or "",
                            "gender": session.context_canvas.get("user_gender") or "",
                            "role": session.context_canvas.get("user_role") or "",
                        },
                        "user_profile": session.context_canvas.get("user_profile", "(no profile)")
                    })

                if step == "06" and status == "done":
                    assistant_struct = session.context_canvas.get("assistant_profile_struct", {})
                    user_name_hint = session.context_canvas.get("user_real_name")
                    await broadcast({
                        "type": "memory_sync",
                        "assistant_root_profile_struct": assistant_struct,
                        "assistant_profile_struct": assistant_struct,
                        "user_root_profile_struct": {
                            "name": user_name_hint or "",
                            "primary_name": user_name_hint or "",
                            "aliases": list(session.context_canvas.get("user_aliases") or []),
                            "age": session.context_canvas.get("user_age") or "",
                            "gender": session.context_canvas.get("user_gender") or "",
                            "role": session.context_canvas.get("user_role") or "",
                        },
                        "rag_content": [_normalize_rag_item(item) for item in session.context_canvas.get("rag_context_structured", []) if _normalize_rag_item(item)],
                        "retrieval_audit": session.context_canvas.get("retrieval_audit", {})
                    })

            full_ai_response = ""
            async for chunk in engine.chat_stream(user_input, session, status_callback=ws_status_callback):
                full_ai_response += chunk
                await broadcast({"type": "chunk", "text": chunk})

            session.context_canvas["vector_status"] = "active"

            # Auto mode: run personality re-inference every N conversation turns.
            turn_count = int(session.context_canvas.get("inference_turn_count", 0) or 0) + 1
            session.context_canvas["inference_turn_count"] = turn_count
            if (turn_count % INFERENCE_AUTO_INTERVAL) == 0:
                import time
                infer_start = time.perf_counter()
                await ws_status_callback("14", "doing", reason="auto_turn")
                auto_infer = await run_identity_reinference(
                    identity_config.user_id,
                    force=False,
                    source="auto_turn",
                )
                infer_ms = int((time.perf_counter() - infer_start) * 1000)
                infer_status = str(auto_infer.get("status") or "").lower()
                infer_reason = auto_infer.get("reason") or auto_infer.get("source") or ""
                if infer_status in {"success", "no_change"}:
                    await ws_status_callback("14", "done", time_ms=infer_ms, reason=infer_reason)
                elif infer_status == "skipped":
                    await ws_status_callback("14", "skip", time_ms=infer_ms, reason=infer_reason or "skipped")
                else:
                    await ws_status_callback("14", "error", time_ms=infer_ms, reason=infer_reason or infer_status or "infer_failed")
            else:
                auto_infer = {
                    "status": "skipped",
                    "reason": "interval_pending",
                    "source": "auto_turn",
                    "turn_count": turn_count,
                    "interval": INFERENCE_AUTO_INTERVAL,
                    "rounds_left": INFERENCE_AUTO_INTERVAL - (turn_count % INFERENCE_AUTO_INTERVAL),
                }
                await ws_status_callback(
                    "14",
                    "skip",
                    time_ms=0,
                    reason=f"interval_pending:{auto_infer['rounds_left']}",
                )
            session.context_canvas["identity_inference_status"] = auto_infer

            await asyncio.sleep(0.3)
            await sync_all()
            print("??Monitor Audit Log: Graph synchronization flushed after conversation end.")

    except WebSocketDisconnect:
        _drop_connection(websocket)
    except Exception as e:
        if _is_ws_disconnect_error(e):
            _drop_connection(websocket)
            return
        logger.error("WS Main Loop Error: %r", e)

@app.get("/cdc/changes")
async def get_cdc_changes(since: int = 0, limit: int = 200, uid: str = None):
    """Fetch CDC incremental changes since a version."""
    from memory.integration.cdc_outbox import outbox
    owner_id = uid or identity_config.user_id
    
    changes = outbox.list_changes_since(owner_id, since, limit)
    latest = outbox.get_latest_version(owner_id)
    
    return {
        "owner_id": owner_id,
        "since": since,
        "limit": limit,
        "latest_version": latest,
        "count": len(changes),
        "changes": changes
    }

@app.get("/maintenance/compaction-report")
async def get_compaction_report():
    """Read the latest graph compaction report."""
    report_path = ".data/compaction_report.json"
    if not os.path.exists(report_path):
        return {"status": "no_report", "message": "compaction report not found"}
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/cdc/ack")
async def ack_cdc_version(consumer_id: str, owner_id: str, version: int):
    """L2: Update CDC consumer checkpoint."""
    new_v = checkpoint_manager.ack_checkpoint(consumer_id, owner_id, version)
    return {"status": "success", "current_version": new_v}

@app.post("/identity/reinfer")
async def identity_reinfer(force: bool = False):
    """Manual trigger for personality re-inference."""
    result = await run_identity_reinference(
        identity_config.user_id,
        force=force,
        source="manual_api",
    )
    return result

@app.post("/crm/upsert")
async def crm_upsert(req: CRMUpsertRequest):
    """L3: Inbound upsert with conflict arbitration."""
    if not identity_config.enable_crm_sync:
        return {"status": "disabled", "message": "CRM sync disabled"}
    
    ALLOWED_SLOTS = {
        "name": "name",
        "role": "role",
        "state": "current_state",
        "aliases": "aliases"
    }
    
    results = []
    async with global_db_driver.session(database=neo4j_config.database) as neosession:
        for change in req.changes:
            if change.slot not in ALLOWED_SLOTS:
                results.append({"id": change.external_event_id, "status": "ignored", "reason": "invalid_slot"})
                continue
            
            db_slot = ALLOWED_SLOTS[change.slot]
            
            curr_res = await neosession.run(
                f"MATCH (e:Entity {{entity_id: $eid, owner_id: $oid}}) RETURN properties(e)[$slot] AS val, properties(e)[$slot + '_source'] AS src, properties(e)[$slot + '_confidence'] AS conf",
                eid=change.target_id, oid=change.owner_id, slot=db_slot
            )
            curr = await curr_res.single()
            
            candidates = []
            if curr and curr["val"]:
                candidates.append(ConflictCandidate(
                    value=str(curr["val"]),
                    source=curr["src"] or "system",
                    confidence=curr["conf"] or 1.0,
                    record_time="2000-01-01"
                ))
            
            crm_cands = build_conflict_candidates(change, identity_config.crm_source_weight)
            for cand in crm_cands:
                candidates.append(cand)
            
            arb = ConflictResolver.resolve_conflict(change.slot, candidates)
            
            audit_entry = {
                "external_id": change.external_event_id, "slot": change.slot,
                "winner": arb.winner, "reason": arb.winner_reason, "timestamp": time.strftime("%H:%M:%S")
            }
            crm_audit_cache.append(audit_entry)
            if len(crm_audit_cache) > 20: crm_audit_cache.pop(0)

            if arb.winner == change.value:
                await neosession.run(
                    f"MATCH (e:Entity {{entity_id: $eid, owner_id: $oid}}) "
                    f"SET e.{db_slot} = $val, e.{db_slot}_source = 'crm', e.{db_slot}_confidence = $conf, e.updated_at = $now",
                    eid=change.target_id, oid=change.owner_id, val=change.value, conf=change.confidence, now=change.timestamp
                )
                results.append({"id": change.external_event_id, "status": "applied"})
            else:
                results.append({"id": change.external_event_id, "status": "ignored", "reason": "arbitration_lost"})
    
    return {"status": "success", "results": results}

@app.post("/crm/events/replay")
async def crm_replay(req: CRMReplayRequest):
    """L4: Replay historical CRM events with idempotency checks."""
    processed = 0
    skipped = 0
    failed = 0
    for change in req.changes:
        key = make_idempotency_key(change.owner_id, change.external_event_id, change.slot, change.value)
        if checkpoint_manager.is_replayed(key):
            skipped += 1
            crm_replay_stats["skipped"] += 1
            continue
        
        try:
            resp = await crm_upsert(CRMUpsertRequest(changes=[change]))
            if resp.get("status") == "success":
                checkpoint_manager.mark_replayed(key)
                processed += 1
                crm_replay_stats["processed"] += 1
            else:
                failed += 1
                crm_replay_stats["failed"] += 1
        except Exception as e:
            logger.error(f"Replay item failure: {e}")
            failed += 1
            crm_replay_stats["failed"] += 1
            
    return {"status": "finished", "processed": processed, "skipped": skipped, "failed": failed}

@app.post("/evolution/rollback/{event_id}")
async def rollback_evolution(event_id: str):
    """Rollback a recent identity evolution event."""
    async with global_db_driver.session(database=neo4j_config.database) as db:
        tx = await db.begin_transaction()
        try:
            res = await tx.run("MATCH (e:Event {event_id: $eid}) RETURN e", eid=event_id)
            evt = await res.single()
            if not evt: 
                await tx.rollback()
                return {"status": "error", "message": "Event not found"}
            meta_str = evt["e"].get("event_metadata", "{}")
            import json
            try:
                meta = json.loads(meta_str)
            except (json.JSONDecodeError, TypeError):
                meta = {}
            old_val = meta.get("old_value") or meta.get("old_name") or meta.get("old")
            new_val = meta.get("new_value") or meta.get("new_name") or meta.get("new")
            if not old_val or not new_val:
                await tx.rollback()
                return {"status": "error", "message": "Invalid metadata for rollback"}
            
            uid = identity_config.user_id
            await tx.run("""
                MATCH (e:Event {event_id: $eid, owner_id: $uid})
                SET e.status = 'rolled_back', e.invalid_at = timestamp()
                WITH e
                MATCH (n:Entity {name: $new_val, owner_id: $uid})
                OPTIONAL MATCH (old_n:Entity {name: $old_val, owner_id: $uid})-[r:IS_ALIAS_OF]->(n)
                DELETE r
                SET n.name = $old_val
            """, new_val=new_val, old_val=old_val, uid=uid, eid=event_id)
            await tx.commit()
            return {"status": "success", "rolled_back_from": new_val, "restored_to": old_val}
        except Exception as e:
            await tx.rollback()
            return {"status": "error", "message": str(e)}

from pydantic import BaseModel
class WipeRequest(BaseModel):
    # Default to cognitive-only wipe to avoid accidental KB deletion
    items: list[str] = ["cognitive"]

@app.post("/maintenance/wipe-memory")
async def wipe_memory(req: WipeRequest, request: Request):
    """Wipe selected memory dimensions."""
    if not _is_maintenance_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized maintenance request")
    global engine, session, global_db_driver
    uid = identity_config.user_id
    wipe_cognitive = "cognitive" in req.items
    wipe_knowledge = "knowledge" in req.items
    
    try:
        results = []
        if wipe_cognitive:
            async with global_db_driver.session(database=neo4j_config.database) as db_session:
                await db_session.run("MATCH (n {owner_id: $uid}) DETACH DELETE n", uid=uid)
                from memory.identity.manager import PersonaManager
                p_manager = PersonaManager()
                await p_manager.bootstrap_genesis_identities(uid)
                await p_manager.close()
            
            from memory.sql.pool import get_db
            async with get_db() as conn:
                await conn.execute("DELETE FROM ef_chat_messages")
                await conn.execute("DELETE FROM ef_chat_sessions")
                await conn.commit()
            
            session.history = []
            results.append('cognitive memory cleared')

        if wipe_knowledge:
            from memory.vector.storer import VectorStorer
            v_storer = VectorStorer()
            try:
                v_storer.client.delete_collection(v_storer.chat_collection.name)
                v_storer.chat_collection = v_storer.client.get_or_create_collection(
                    name=v_storer._collection_name,
                    embedding_function=v_storer._embedding_fn
                )
            except Exception as ve:
                logger.warning(f"Vector wipe failed partly: {ve}")
            results.append('knowledge base cleared')
        
        session.context_canvas = {"vector_status": "active"}
        return {"status": "success", "message": " | ".join(results) or "no wipe item selected"}
    except Exception as e:
        logger.error(f"Selective wipe failed: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/maintenance/restore-demo-data")
async def restore_demo_data(request: Request):
    """Restore the bundled demo snapshot while the server is running."""
    global runtime_restore_in_progress
    if not _is_maintenance_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized maintenance request")

    backup_zip = os.path.join(BASE_DIR, "backups", "demo_data.zip")
    expanded_backup_dir = os.path.join(BASE_DIR, "backups", "demo_data")
    temp_backup = None

    if os.path.exists(backup_zip):
        temp_backup = tempfile.TemporaryDirectory(prefix="ebbingflow_demo_restore_")
        try:
            _safe_extract_zip(backup_zip, temp_backup.name)
            backup_dir = _find_demo_backup_dir(temp_backup.name)
        except Exception as exc:
            temp_backup.cleanup()
            return {"status": "error", "message": f"invalid demo backup zip: {exc}"}
    else:
        backup_dir = expanded_backup_dir

    missing = _missing_demo_backup_files(backup_dir)
    if missing:
        if temp_backup is not None:
            temp_backup.cleanup()
        return {
            "status": "error",
            "message": "demo backup is incomplete",
            "missing": missing,
        }

    async with restore_demo_lock:
        try:
            runtime_restore_in_progress = True
            await _close_active_websockets_for_restore()
            await asyncio.sleep(0.25)
            await _close_runtime_handles_for_restore()
            _restore_demo_data_files(backup_dir)
            node_count, rel_count = await _restore_neo4j_snapshot(backup_dir)
            await _initialize_runtime_after_restore()
            return {
                "status": "success",
                "message": "demo data restored",
                "nodes": node_count,
                "relationships": rel_count,
                "history_messages": len(session.history) if session else 0,
            }
        except Exception as e:
            logger.exception("[DemoRestore] Restore failed: %s", e)
            try:
                if session is None or global_db_driver is None or checkpoint_manager is None:
                    await _initialize_runtime_after_restore()
            except Exception as init_exc:
                logger.exception("[DemoRestore] Runtime reinitialization failed: %s", init_exc)
            return {"status": "error", "message": str(e)}
        finally:
            runtime_restore_in_progress = False
            if temp_backup is not None:
                temp_backup.cleanup()

from fastapi import UploadFile, File, Form

@app.get("/kb/list")
async def list_knowledge_base():
    from memory.vector.storer import VectorStorer
    storer = VectorStorer()
    res = storer.doc_collection.get(include=["metadatas"])
    sources = set()
    for meta in res.get("metadatas", []):
        if meta and "source" in meta:
            sources.add(meta["source"])
    return {"status": "success", "files": list(sources)}

@app.post("/kb/upload")
async def upload_knowledge_base(file: UploadFile = File(...), chunk_size: int = Form(1000)):
    try:
        content = await file.read()
        try:
            text = content.decode('utf-8')
        except Exception:
            try:
                text = content.decode('gbk')
            except Exception:
                return {"status": "error", "message": "Only UTF-8/GBK text files are supported."}
        
        from memory.vector.devourer import DocumentDevourer
        from memory.identity.resolver import Actor
        overlap = min(chunk_size // 4, 300)
        devourer = DocumentDevourer(chunk_size=chunk_size, overlap=overlap)
        actor = Actor(speaker_id="user", speaker_name="User", target_id="assistant", target_name="AI")
        
        result = await devourer.devour(text, source_name=file.filename, actor=actor, extract_graph=False)
        chunks = result.get("chunks_stored", "?")
        return {"status": "success", "message": f"Uploaded {file.filename} in {chunks} chunks (chunk_size={chunk_size})."}
    except Exception as e:
        import traceback
        logger.error(f"KB Upload Crash: {e}\n{traceback.format_exc()}")
        return {"status": "error", "message": f"KB upload failed: {str(e)}"}

class KBDeleteRequest(BaseModel):
    filename: str

@app.post("/kb/delete")
async def delete_kb_file(req: KBDeleteRequest):
    from memory.vector.storer import VectorStorer
    storer = VectorStorer()
    try:
        storer.doc_collection.delete(where={"source": req.filename})
        return {"status": "success", "message": f"{req.filename} deleted"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/kb/clear")
async def clear_knowledge_base():
    from memory.vector.storer import VectorStorer
    storer = VectorStorer()
    try:
        res = storer.doc_collection.get(include=[])
        if res and res["ids"]:
            storer.doc_collection.delete(ids=res["ids"])
        return {"status": "success", "message": "knowledge base cleared"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/monitor/stats")
async def get_monitor_stats():
    """Get monitor metrics for identity evolution."""
    if global_db_driver is None:
        return {
            "status": "degraded",
            "message": "neo4j driver not initialized",
            "conflict_count": 0,
            "merge_success_rate": "0.0%",
            "rollback_count": 0,
        }
    async with global_db_driver.session(database=neo4j_config.database) as db:
        uid = identity_config.user_id
        res = await db.run(
            "MATCH (e:Event {predicate: 'IDENTITY_EVOLVED', owner_id: $uid}) "
            "WITH count(e) as total_conflicts, "
            "sum(case when e.status = 'rolled_back' then 1 else 0 end) as rollbacks "
            "RETURN total_conflicts, rollbacks",
            uid=uid,
        )
        record = await res.single()
        total_conflicts = record["total_conflicts"] if record else 0
        rollbacks = record["rollbacks"] if record else 0
        
        success = total_conflicts - rollbacks
        rate = f"{(success/total_conflicts*100):.1f}%" if total_conflicts > 0 else "100.0%"
        
        return {
            "conflict_count": total_conflicts,
            "merge_success_rate": rate,
            "rollback_count": rollbacks
        }

if __name__ == "__main__":
    uvicorn.run("api.server:app", host="0.0.0.0", port=8000, reload=True, reload_dirs=["api", "core", "memory", "bridge"])



