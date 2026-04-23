import os
import sys
import asyncio
from unittest.mock import AsyncMock, MagicMock

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.graph.relation_reasoner import RelationReasoner

async def test_reasoning():
    print("Running Relation Reasoner Tests...")
    
    # 模拟 Neo4j 驱动
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    
    reasoner = RelationReasoner(driver=mock_driver)
    
    # CASE 1: R4 对称推演 (FRIEND_OF)
    rels1 = [{"from": "Alice", "type": "FRIEND_OF", "to": "Bob", "from_id": "alice_pk", "to_id": "bob_pk"}]
    inf1 = await reasoner.reason(rels1, "user_001")
    assert any(i["type"] == "FRIEND_OF" and i["from"] == "Bob" and i["inference_rule"] == "R4" for i in inf1)
    print("Case 1: R4 Symmetry Pass")

    # CASE 2: R3 可逆推演 (SERVES -> OWNS)
    rels2 = [{"from": "Andrew", "type": "SERVES", "to": "Master", "from_id": "asst_001", "to_id": "user_001"}]
    inf2 = await reasoner.reason(rels2, "user_001")
    assert any(i["type"] == "OWNS" and i["from"] == "Master" and i["inference_rule"] == "R3" for i in inf2)
    print("Case 2: R3 Inversion Pass")

    # CASE 3: R1 传递性 (SAME_AS)
    # 模拟图谱中已有 B SAME_AS C
    mock_res = MagicMock()
    mock_res.data = AsyncMock(return_value=[{"c_name": "Charlie", "c_id": "charlie_pk"}])
    mock_session.run.return_value = mock_res
    
    rels3 = [{"from": "Alice", "type": "SAME_AS", "to": "Bob", "from_id": "alice_pk", "to_id": "bob_pk"}]
    inf3 = await reasoner.reason(rels3, "user_001")
    assert any(i["type"] == "SAME_AS" and i["to"] == "Charlie" and i["inference_rule"] == "R1" for i in inf3)
    print("Case 3: R1 Transitivity Pass")

    print("\nRelation Reasoner Tests: 3/3 Pass (Core Rules)")

if __name__ == "__main__":
    asyncio.run(test_reasoning())
