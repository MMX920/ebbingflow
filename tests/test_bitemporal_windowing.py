import os
import sys
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.graph.writer import AsyncGraphWriter
from memory.knowledge_engine import KnowledgeBaseEngine
from memory.event.slots import MemoryEvent

async def test_windowing():
    print("Running Bitemporal Windowing Tests...")
    engine = KnowledgeBaseEngine()
    
    # CASE 1: 昨天 命中昨天窗口
    q1 = "昨天我干了什么？"
    tw_start, tw_end, source = engine._infer_time_window(q1)
    yesterday = (datetime.utcnow() - timedelta(days=1)).date().isoformat()
    assert yesterday in tw_start, f"Yesterday window start mismatch: {tw_start}"
    assert source == "nlp_inferred"
    print("Case 1: Yesterday NLP Inference Pass")

    # CASE 2: 今天 只召回今日事件 (逻辑验证)
    q2 = "今天有什么计划？"
    tw_start, tw_end, source = engine._infer_time_window(q2)
    today = datetime.utcnow().date().isoformat()
    assert today in tw_start, f"Today window start mismatch: {tw_start}"
    print("Case 2: Today NLP Inference Pass")

    # CASE 3: 无时间词时不启用窗口
    q3 = "介绍一下你自己"
    tw_start, tw_end, source = engine._infer_time_window(q3)
    assert tw_start is None and tw_end is None
    assert source == "none"
    print("Case 3: No Window Pass")

    # CASE 4: event_time 缺失时回退 record_time (Cypher 逻辑审计)
    # 我们检查 _retrieve_graph_events 中的 Cypher 字符串
    # 模拟调用以捕获 Cypher
    engine._driver = MagicMock()
    mock_session = AsyncMock()
    engine._driver.session.return_value.__aenter__.return_value = mock_session
    mock_session.run = AsyncMock()
    mock_res = MagicMock()
    mock_res.data = AsyncMock(return_value=[])
    mock_session.run.return_value = mock_res
    
    await engine._retrieve_graph_events("测试", "user_001", "2026-01-01T00:00:00Z", "2026-01-01T23:59:59Z")
    
    all_query_text = "".join([call.args[0] for call in mock_session.run.call_args_list])
    assert "evt.event_time IS NULL AND (evt.record_time >= $start OR evt.created_at >= $start)" in all_query_text, "Bitemporal fallback missing in Cypher"
    print("Case 4: Bitemporal Fallback Pass")

    # CASE 5: 解析失败不崩溃且回退正常 (Writer 侧)
    from memory.event.slots import ActionType
    writer = AsyncGraphWriter()
    e5 = MemoryEvent(subject="user", predicate="测试", timestamp_reference="未知时刻", action_type=ActionType.STATE_CHANGE)
    ev_time = writer._normalize_event_time(e5)
    assert ev_time is not None, "Normalized event_time should never be None"
    print("Case 5: Robust Parsing Pass")

    print("\nBitemporal Windowing Tests: 5/5 PASS")

if __name__ == "__main__":
    asyncio.run(test_windowing())
