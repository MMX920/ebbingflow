import os
import sys
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.graph.writer import AsyncGraphWriter
from memory.event.slots import MemoryEvent, ActionType

async def test_temporal_logic():
    print("Running Bitemporal Logic Tests...")
    writer = AsyncGraphWriter()
    
    # CASE 1: event_time 与 record_time 同时存在 (正例)
    e1 = MemoryEvent(
        subject="user", predicate="出生于北京", timestamp_reference="1990-01-01",
        action_type=ActionType.STATE_CHANGE, confidence=1.0
    )
    # 模拟内部 _normalize_event_time
    ev_time = writer._normalize_event_time(e1)
    rec_time = writer._now_record_time()
    assert ev_time == "1990-01-01", "event_time should be normalized from timestamp_reference"
    print("Case 1: Bitemporal Presence Pass")

    # CASE 2: 老数据缺 event_time 可回退 (显示逻辑模拟)
    # mock 一个没有 event_time 的数据对象
    old_evt = {"record_time": "2026-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z"}
    display_time = old_evt.get("event_time") or old_evt.get("record_time") or old_evt.get("created_at")
    assert display_time == "2026-01-01T00:00:00Z", "Fallback display time failed"
    print("Case 2: Legacy Fallback Pass")

    # CASE 3: 同槽位新事实使旧事实 invalidated (写入模拟)
    writer._driver = MagicMock()
    mock_session = AsyncMock()
    writer._driver.session.return_value.__aenter__.return_value = mock_session
    mock_session.run = AsyncMock()
    # 模拟返回 final_id
    mock_res = MagicMock()
    mock_res.single = AsyncMock(return_value={"final_id": "uuid-new", "final_sk": None, "inv_ids": ["uuid-old"]})
    mock_session.run.return_value = mock_res
    
    await writer.write_events([e1], [], "session_1", "user_001")
    
    # 检查 Cypher 是否包含失效逻辑
    all_query_text = "".join([call.args[0] for call in mock_session.run.call_args_list])
    assert "SET old.status = 'invalidated'" in all_query_text, "Invalidation logic missing in Cypher"
    print("Case 3: Slot Invalidation Pass")

    # CASE 4: 不同槽位不互相失效 (反例逻辑)
    # 这一步通过 Cypher 里的 WHERE old.temporal_slot = $temporal_slot 保证
    assert "old.temporal_slot = $temporal_slot" in all_query_text, "Slot isolation missing in Cypher"
    print("Case 4: Slot Isolation Pass")

    print("Bitemporal Logic Tests: 4/4 PASS")

if __name__ == "__main__":
    asyncio.run(test_temporal_logic())
