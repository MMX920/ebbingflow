# Demo Data Import Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the monitor personality re-inference button with a reliable in-app demo data import flow. The default demo is: **"Reborn as Zhuge Liang: Building an Empire with EbbingFlow"** («重生成为诸葛亮，系统（Ebbingflow）助我成就霸业»).

**Architecture:** Add a maintenance endpoint that runs inside the FastAPI process so it can close live database handles before replacing `.data` from `backups/demo_data`. The endpoint restores Neo4j, restores local SQLite/Chroma files, then rebuilds the runtime session/driver/checkpoint state. The frontend reuses the existing wipe modal and wait overlay styling for confirmation and progress.

**Tech Stack:** FastAPI, Neo4j async driver, SQLite file snapshots, Chroma persistent SQLite storage, vanilla HTML/CSS/JS.

---

### Task 1: Runtime Restore Endpoint

**Files:**
- Modify: `api/server.py`

- [x] Add helpers to close the current runtime, restore Neo4j from `backups/demo_data/neo4j_snapshot.json`, replace `.data` from `backups/demo_data/.data_fs`, and rebuild runtime objects.
- [x] Add `POST /maintenance/restore-demo-data` protected by the existing maintenance auth.
- [x] Make restoration fail closed with a clear message if backup files are missing.

### Task 2: Monitor UI Flow

**Files:**
- Modify: `frontend/data_monitor.html`

- [x] Replace `人格重判` with `导入演示数据`.
- [x] Add a demo import confirmation modal using the same classes as the wipe modal.
- [x] Add `executeDemoImport()` that shows the existing wait overlay, calls the new endpoint, handles maintenance token retry, and reloads `/monitor` when done.

### Task 3: Verification

**Files:**
- Test command: `python -m pytest tests/test_ws_auth.py -q`
- Syntax command: `python -m py_compile api/server.py`

- [x] Run backend syntax check.
- [x] Run a focused existing test.
- [x] Review diff for unrelated churn.
