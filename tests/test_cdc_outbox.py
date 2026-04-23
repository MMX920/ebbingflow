import os
import sys
import time

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.integration.cdc_outbox import CDCOutbox


def _safe_remove(path: str, retries: int = 8, delay: float = 0.05):
    if not os.path.exists(path):
        return
    last_err = None
    for _ in range(retries):
        try:
            os.remove(path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(delay)
    # Windows may keep SQLite handles briefly; avoid hard-failing cleanup.
    if last_err:
        print(f"[WARN] cleanup skipped for locked file: {path} ({last_err})")


def test_cdc_outbox():
    print("Start CDC Outbox Test...")

    test_db = ".data/cdc_test.db"
    _safe_remove(test_db)

    cdc = CDCOutbox()
    old_db_path = cdc.db_path
    cdc.db_path = test_db
    cdc._init_db()

    owner = "test_user"

    # 1. 验证 append 后 version 递增
    v1 = cdc.append_change(owner, "upsert", "event", "evt_1", {"msg": "hello"})
    v2 = cdc.append_change(owner, "upsert", "event", "evt_2", {"msg": "world"})
    assert v1 > 0, "Version should be positive"
    assert v2 == v1 + 1, "Version should increment monotonically"
    print("Case 1: Version Incrementation Pass")

    # 2. 验证 since 拉取正确
    changes = cdc.list_changes_since(owner, v1)
    assert len(changes) == 1, f"Should pull 1 change since v1, got {len(changes)}"
    assert changes[0]["entity_id"] == "evt_2", "Pushed change should match"
    print("Case 2: Incremental Fetching Pass")

    # 3. 验证 latest version
    latest = cdc.get_latest_version(owner)
    assert latest == v2, f"Latest version should be {v2}, got {latest}"
    print("Case 3: Latest Version Tracking Pass")

    # 4. 容错测试（模拟 DB 路径失效）
    print("[EXPECTED_FAULT_INJECTION] Testing database connection failure...")
    cdc.db_path = "/non_existent/path/no.db"
    v3 = cdc.append_change(owner, "upsert", "event", "evt_3", {})
    assert v3 == -1, "Should fail gracefully with -1"
    print("Case 4: Graceful Failure Pass (Result: -1)")

    # 清理与回滚
    cdc.db_path = old_db_path
    _safe_remove(test_db)
    print("\nAll CDC outbox tests passed!")


if __name__ == "__main__":
    try:
        test_cdc_outbox()
    except Exception as e:
        print(f"Test Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
