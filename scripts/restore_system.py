import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

from neo4j import AsyncGraphDatabase

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import neo4j_config

REQUIRED_FILES = [
    Path("neo4j_snapshot.json"),
    Path(".data_fs") / "ef_history.db",
    Path(".data_fs") / "cdc_outbox.db",
    Path(".data_fs") / "cdc_checkpoint.db",
    Path(".data_fs") / "chroma" / "chroma.sqlite3",
]


def quote_cypher_name(name: str) -> str:
    return f"`{str(name or '').replace('`', '``')}`"


def missing_backup_files(backup_dir: Path) -> list[Path]:
    return [backup_dir / rel for rel in REQUIRED_FILES if not (backup_dir / rel).exists()]


def safe_extract_zip(zip_path: Path, destination: Path):
    dest_abs = destination.resolve()
    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            target = (destination / member.filename).resolve()
            if target != dest_abs and dest_abs not in target.parents:
                raise ValueError(f"Unsafe path in zip: {member.filename}")
        archive.extractall(destination)


def find_backup_dir(root: Path) -> Path:
    if not missing_backup_files(root):
        return root

    for current, dirs, _files in os.walk(root):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        candidate = Path(current)
        if not missing_backup_files(candidate):
            return candidate

    raise FileNotFoundError("Backup does not contain neo4j_snapshot.json and .data_fs.")


async def restore_neo4j(backup_dir: Path):
    snapshot_path = backup_dir / "neo4j_snapshot.json"
    with snapshot_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri,
        auth=(neo4j_config.username, neo4j_config.password),
    )
    try:
        async with driver.session(database=neo4j_config.database) as session:
            print("[*] Clearing current Neo4j graph...")
            result = await session.run("MATCH (n) DETACH DELETE n")
            await result.consume()

            print(f"[*] Restoring {len(data.get('nodes', []))} Neo4j nodes...")
            id_map = {}
            nodes_by_labels = {}
            for raw_node in data.get("nodes", []):
                node = dict(raw_node)
                old_eid = node.pop("__eid__", None)
                labels = tuple(sorted(node.pop("__labels__", ["Entity"]) or ["Entity"]))
                nodes_by_labels.setdefault(labels, []).append({"old_eid": old_eid, "props": node})

            for labels, node_list in nodes_by_labels.items():
                label_str = ":".join(quote_cypher_name(label) for label in labels)
                query = (
                    f"UNWIND $batch AS item "
                    f"CREATE (n:{label_str}) SET n = item.props "
                    f"RETURN item.old_eid AS old, elementId(n) AS new"
                )
                res = await session.run(query, batch=node_list)
                async for record in res:
                    if record["old"]:
                        id_map[record["old"]] = record["new"]

            print(f"[*] Restoring {len(data.get('rels', []))} Neo4j relationships...")
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
                quoted_type = quote_cypher_name(rtype)
                for i in range(0, len(rel_list), 5000):
                    batch = rel_list[i:i + 5000]
                    result = await session.run(
                        f"""
                        UNWIND $batch AS r
                        MATCH (a), (b)
                        WHERE elementId(a) = r.a AND elementId(b) = r.b
                        CREATE (a)-[rel:{quoted_type}]->(b)
                        SET rel = r.p
                        """,
                        batch=batch,
                    )
                    await result.consume()

        print("[OK] Neo4j graph restored.")
    finally:
        await driver.close()


def restore_local_data(backup_dir: Path):
    src = backup_dir / ".data_fs"
    dst = ROOT / ".data"
    print("[*] Restoring local .data storage...")
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    print("[OK] Local .data storage restored.")


async def restore_from_path(path: Path):
    temp_dir = None
    try:
        if path.is_file() and path.suffix.lower() == ".zip":
            temp_dir = tempfile.TemporaryDirectory(prefix="ebbingflow_restore_")
            extract_root = Path(temp_dir.name)
            safe_extract_zip(path, extract_root)
            backup_dir = find_backup_dir(extract_root)
        else:
            backup_dir = find_backup_dir(path)

        missing = missing_backup_files(backup_dir)
        if missing:
            raise FileNotFoundError("Missing backup files: " + ", ".join(str(p) for p in missing))

        restore_local_data(backup_dir)
        await restore_neo4j(backup_dir)
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


async def main():
    parser = argparse.ArgumentParser(description="Restore EbbingFlow backup data.")
    parser.add_argument(
        "--path",
        default=str(ROOT / "backups" / "demo_data.zip"),
        help="Backup directory or zip path. Defaults to backups/demo_data.zip.",
    )
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    args = parser.parse_args()

    backup_path = Path(args.path)
    if not backup_path.is_absolute():
        backup_path = ROOT / backup_path

    if not backup_path.exists():
        print(f"[ERROR] Backup path not found: {backup_path}")
        return

    print(f"=== EbbingFlow Restore [source: {backup_path}] ===")
    if not args.yes:
        confirm = input("This will overwrite current memory data. Continue? (y/n): ").strip().lower()
        if confirm != "y":
            print("Restore cancelled.")
            return

    await restore_from_path(backup_path)
    print("\n[DONE] Restore completed.")


if __name__ == "__main__":
    asyncio.run(main())
