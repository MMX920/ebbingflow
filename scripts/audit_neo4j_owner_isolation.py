"""Lightweight audit for Neo4j tenant/owner isolation in Cypher strings.

The script is intentionally conservative: it flags common MATCH/MERGE patterns
that touch shared graph labels without an owner_id guard nearby.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = ["api", "core", "memory", "scripts"]
SKIP_FILES = {Path("scripts/audit_neo4j_owner_isolation.py")}

PATTERNS = [
    ("entity_without_owner", re.compile(r"(?:MATCH|MERGE)\s*\([^)]*:Entity\s*\{[^}]*entity_id\s*:\s*\$[^,}]+[^}]*\}", re.I)),
    ("event_uuid_without_owner", re.compile(r"(?:MATCH|MERGE|OPTIONAL MATCH)\s*\([^)]*:Event\s*\{[^}]*uuid\s*:\s*[^,}]+[^}]*\}", re.I)),
    ("episode_without_owner", re.compile(r"(?:MATCH|MERGE|OPTIONAL MATCH)\s*\([^)]*:Episode\s*\{[^}]*episode_id\s*:\s*[^,}]+[^}]*\}", re.I)),
    ("relation_without_endpoint_owner", re.compile(r"MATCH\s*\([^)]*:Entity\)\s*-\s*\[[^]]*:RELATION[^]]*\]\s*->\s*\([^)]*:Entity\)", re.I)),
    ("fact_element_without_owner", re.compile(r"MATCH\s*\([^)]*:Fact\)\s*WHERE\s*elementId", re.I)),
]


def _clean_line(line: str) -> str:
    return line.strip().replace("\\n", " ")


def _has_owner_guard(line: str) -> bool:
    return "owner_id" in line


def main() -> int:
    findings: list[tuple[str, Path, int, str]] = []
    for scan_dir in SCAN_DIRS:
        base = ROOT / scan_dir
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            rel = path.relative_to(ROOT)
            if rel in SKIP_FILES or any(part in {"venv", "__pycache__"} for part in rel.parts):
                continue
            try:
                lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            except OSError:
                continue
            for lineno, line in enumerate(lines, start=1):
                clean = _clean_line(line)
                for name, pattern in PATTERNS:
                    if pattern.search(clean) and not _has_owner_guard(clean):
                        findings.append((name, rel, lineno, clean))

    if findings:
        print("Neo4j owner isolation audit found possible gaps:")
        for name, rel, lineno, line in findings:
            print(f"- {name}: {rel}:{lineno}: {line}")
        return 1

    print("Neo4j owner isolation audit passed: no obvious unguarded shared-label Cypher patterns found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
