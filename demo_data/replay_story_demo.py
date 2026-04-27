import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import identity_config
from core.chat_engine import get_standard_engine
from core.session import ChatSession
from memory.history.repository import SqlHistoryRepository


DEFAULT_SOURCE = ROOT / "demo_data" / "zhuge_hegemony_final.jsonl"
PROGRESS_FILE = ROOT / ".replay_progress.json"


def load_progress(key: str) -> int:
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            return data.get(key, 0)
        except Exception:
            pass
    return 0


def save_progress(key: str, index: int):
    data = {}
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    data[key] = index
    PROGRESS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def read_source(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


async def initialize_runtime():
    try:
        from scripts.setup_db import setup_db
        await setup_db()
        print("[init] SQL schema ready")
    except Exception as exc:
        print(f"[init][warn] SQL setup skipped/failed: {exc}")

    try:
        from scripts.initialize_neo4j import initialize_neo4j
        await initialize_neo4j()
        print("[init] Neo4j schema ready")
    except Exception as exc:
        print(f"[init][warn] Neo4j setup skipped/failed: {exc}")


async def replay(args):
    rows = read_source(args.source)
    
    progress_key = f"{args.source.name}_{args.session_id}"
    start_idx = args.start_index
    
    # Auto-resume logic
    if start_idx == 0 and not args.reset_progress:
        saved_idx = load_progress(progress_key)
        if saved_idx > 0:
            print(f"[progress] Found previous progress. Resuming from index {saved_idx}")
            start_idx = saved_idx

    selected = rows[start_idx : start_idx + args.limit]
    if not selected:
        print(f"[done] No more rows to process (start_index={start_idx}, total={len(rows)})")
        return

    await initialize_runtime()

    engine = get_standard_engine()
    history_repo = SqlHistoryRepository()
    session = ChatSession(
        session_id=args.session_id,
        user_id=identity_config.user_id,
        history_repo=history_repo,
    )
    await session.restore_from_repo()

    print(f"[replay] session_id={args.session_id}")
    print(f"[replay] selected={len(selected)} start_index={start_idx} total_source={len(rows)}")

    for offset, row in enumerate(selected, start=1):
        source_index = start_idx + offset - 1
        simulated_at = row["simulated_at"]
        user_text = row["user"]

        print("")
        print("=" * 80)
        print(f"[{offset}/{len(selected)} | {source_index + 1}/{len(rows)}] source_index={source_index} day={row.get('story_day')} time={simulated_at}")
        print(f"USER: {user_text}")
        print("Andrew: ", end="", flush=True)

        full_reply = ""

        async def status_callback(step, status, **kwargs):
            if args.verbose_status:
                reason = kwargs.get("reason") or kwargs.get("detail") or ""
                print(f"\n  [status] step={step} status={status} {reason}".rstrip())

        async for chunk in engine.chat_stream(
            user_text,
            session,
            status_callback=status_callback,
            simulated_at=simulated_at,
        ):
            full_reply += chunk
            print(chunk, end="", flush=True)
        print("")

        user_msg = next((m for m in reversed(session.history) if m.role == "user"), None)
        assistant_msg = next((m for m in reversed(session.history) if m.role == "assistant"), None)
        print(
            f"[stored] user_msg_id={getattr(user_msg, 'msg_id', None)} "
            f"user_ts={getattr(user_msg, 'timestamp', None)} "
            f"assistant_msg_id={getattr(assistant_msg, 'msg_id', None)} "
            f"assistant_ts={getattr(assistant_msg, 'timestamp', None)}"
        )

        # Save progress after successful turn
        save_progress(progress_key, source_index + 1)

        if args.sleep > 0:
            await asyncio.sleep(args.sleep)

    print("")
    print("[done] replay complete")


def parse_args():
    parser = argparse.ArgumentParser(description="Replay demo dialogue through the real EbbingFlow SOP.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--session-id", default="demo_probe_3")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--verbose-status", action="store_true")
    parser.add_argument("--reset-progress", action="store_true", help="Ignore saved progress and start from start-index")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(replay(parse_args()))
