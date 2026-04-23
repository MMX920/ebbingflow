from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = ("api", "core", "memory", "scripts", "tests")
BARE_EXCEPT_RE = re.compile(r"^\s*except\s*:\s*(#.*)?$")


def _iter_py_files():
    for rel_dir in SCAN_DIRS:
        base = ROOT / rel_dir
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            yield path


def test_no_bare_except_in_project_code():
    offenders: list[str] = []
    for py_file in _iter_py_files():
        for lineno, line in enumerate(py_file.read_text(encoding="utf-8").splitlines(), start=1):
            if BARE_EXCEPT_RE.match(line):
                offenders.append(f"{py_file.relative_to(ROOT)}:{lineno}")
    assert not offenders, "Bare except is forbidden:\n" + "\n".join(offenders)


def test_requirements_are_pinned():
    req_path = ROOT / "requirements.txt"
    assert req_path.exists(), "requirements.txt is missing"

    invalid: list[str] = []
    for raw in req_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Keep current policy simple and strict for runtime dependencies.
        if "==" not in line:
            invalid.append(line)
    assert not invalid, "All runtime dependencies must be pinned with '==':\n" + "\n".join(invalid)


def test_pyproject_exists_and_declares_project():
    pyproject = ROOT / "pyproject.toml"
    assert pyproject.exists(), "pyproject.toml is missing"
    text = pyproject.read_text(encoding="utf-8")
    assert "[build-system]" in text, "pyproject.toml must define [build-system]"
    assert "[project]" in text, "pyproject.toml must define [project]"
