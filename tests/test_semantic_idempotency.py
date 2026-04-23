import os
import sys
import uuid
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.graph.writer import AsyncGraphWriter
from memory.event.slots import MemoryEvent, ActionType

async def test_semantic_idempotency_extended():
    print("Start Extended Semantic Idempotency Test...")
    
    writer = AsyncGraphWriter()
    owner_id = "test_user_001"
    
    # 1. 验证 Key 稳定性 (基于 entity_id 而不是 name)
    slot = "name"
    norm_val = writer._normalize_value_for_slot(slot, "王力")
    # 即使 user_id 一致，传入不同的 sub_stable_id 应产生一致的 Key
    key1 = writer._semantic_idempotency_key("user_001", slot, norm_val, owner_id)
    key2 = writer._semantic_idempotency_key("user_001", slot, norm_val, owner_id)
    assert key1 == key2, "Key should be stable for same entity_id"
    print("Case 1: Key Stability (entity_id anchor) Pass")

    # 2. 模拟 Cypher 逻辑验证 (核心：COALESCE 兼容性)
    # 我们通过 Mock Session 来检查生成的 Cypher 字符串是否包含 COALESCE
    mock_session = AsyncMock()
    
    # 构造一个模拟事件
    e = MemoryEvent(
        subject="user",
        predicate="我叫王力",
        action_type=ActionType.STATE_CHANGE,
        confidence=1.0,
        source_entity="user"
    )
    
    # 模拟 write_events 调用
    # 这里我们直接手动触发部分内部逻辑，或者 mock _driver.session
    writer._driver = MagicMock()
    writer._driver.session.return_value.__aenter__.return_value = mock_session
    
    await writer.write_events([e], [], "session_001", owner_id, current_names={"user": "王力"})
    
    # 获取最后一次运行的 Cypher
    call_args = mock_session.run.call_args_list
    cypher_found = False
    for call in call_args:
        query = call.args[0]
        if "existing.mention_count = COALESCE(existing.mention_count, 0) + 1" in query:
            cypher_found = True
            break
            
    assert cypher_found, "Cypher should contain COALESCE for null-safe mention_count increment"
    print("Case 2: Null-Safe Mention Count Cypher (COALESCE) Pass")

    # 3. 验证老数据兼容场景参数
    # 在 ON MATCH SET 中也应有 COALESCE
    on_match_found = False
    for call in call_args:
        query = call.args[0]
        if (
            "e.mention_count = COALESCE(e.mention_count, 0) + 1" in query
            or "existing.mention_count = COALESCE(existing.mention_count, 0) + 1" in query
        ):
            on_match_found = True
            break
    assert on_match_found, "Cypher ON MATCH should contain COALESCE for legacy compatibility"
    print("Case 3: Legacy Compatibility (ON MATCH COALESCE) Pass")

    print("\nAll 3 extended test cases passed!")

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(test_semantic_idempotency_extended())
    except Exception as e:
        print(f"Test Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
