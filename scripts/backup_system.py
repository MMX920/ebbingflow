import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

from neo4j import AsyncGraphDatabase

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import neo4j_config


def quote_cypher_name(name: str) -> str:
    return f"`{str(name or '').replace('`', '``')}`"


def add_dir_to_zip(zip_file: zipfile.ZipFile, source_dir: Path, base_dir: Path):
    for path in source_dir.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            zip_file.write(path, path.relative_to(base_dir))


async def backup_neo4j(backup_dir: Path):
    print("[*] Backing up Neo4j graph data...")
    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri,
        auth=(neo4j_config.username, neo4j_config.password),
    )
    try:
        async with driver.session(database=neo4j_config.database) as session:
            nodes_res = await session.run(
                "MATCH (n) RETURN n, labels(n) AS labels, elementId(n) AS eid"
            )
            nodes = []
            for record in await nodes_res.data():
                node_data = dict(record["n"])
                node_data["__labels__"] = record["labels"]
                node_data["__eid__"] = record["eid"]
                nodes.append(node_data)

            rels_res = await session.run(
                """
                MATCH (a)-[r]->(b)
                RETURN elementId(a) AS a_eid,
                       type(r) AS rel_type,
                       properties(r) AS rel_props,
                       elementId(b) AS b_eid
                """
            )
            rels = await rels_res.data()

            snapshot_path = backup_dir / "neo4j_snapshot.json"
            with snapshot_path.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "nodes": nodes,
                        "rels": rels,
                        "timestamp": datetime.now().isoformat(),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            print(f"[OK] Neo4j snapshot saved: {snapshot_path}")
    finally:
        await driver.close()


def backup_local_data(backup_dir: Path):
    print("[*] Backing up local .data directory...")
    src = ROOT / ".data"
    dst = backup_dir / ".data_fs"
    if not src.exists():
        print("[!] .data directory not found; skipping local data backup.")
        return

    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__"))
    print(f"[OK] Local data copied: {dst}")


def write_backup_zip(snapshot_dir: Path, zip_path: Path):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        add_dir_to_zip(archive, snapshot_dir, snapshot_dir)
    print(f"[OK] Backup zip written: {zip_path}")


async def create_backup_zip(output: Path | None = None) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = output or (ROOT / "backups" / f"backup_{timestamp}.zip")

    print(f"=== EbbingFlow backup started [ID: {timestamp}] ===")
    with tempfile.TemporaryDirectory(prefix="ebbingflow_backup_") as temp:
        snapshot_dir = Path(temp) / f"backup_{timestamp}"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        await backup_neo4j(snapshot_dir)
        backup_local_data(snapshot_dir)
        write_backup_zip(snapshot_dir, output)

    print(f"\n[DONE] Full backup completed: {output}")
    return output


async def main():
    parser = argparse.ArgumentParser(description="Create an EbbingFlow backup zip.")
    parser.add_argument(
        "--output",
        help="Output zip path. Defaults to backups/backup_YYYYMMDD_HHMMSS.zip.",
    )
    args = parser.parse_args()

    output = Path(args.output) if args.output else None
    if output is not None and not output.is_absolute():
        output = ROOT / output

    await create_backup_zip(output)


if __name__ == "__main__":
    asyncio.run(main())
