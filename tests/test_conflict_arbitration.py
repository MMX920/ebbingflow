import os
import sys

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.identity.conflict_resolver import ConflictResolver, ConflictCandidate
from datetime import datetime, timedelta

def test_conflict_arbitration():
    print("Start Identity Conflict Arbitration Unit Test...")
    
    # 用例 1: 高 source_weight 胜低 confidence
    c1 = ConflictCandidate(value="UserChosen", source="user", confidence=0.5, record_time="2026-04-13T10:00:00Z")
    c2 = ConflictCandidate(value="SystemGuess", source="system", confidence=1.0, record_time="2026-04-13T10:00:01Z")
    res1 = ConflictResolver.resolve_conflict("name", [c1, c2])
    assert res1.winner == "UserChosen", f"Weight test failed: {res1.winner}"
    print("Case 1: Source Weight (User > System) Pass")

    # 用例 2: 同 source 下高 confidence 胜
    c3 = ConflictCandidate(value="ConfLow", source="assistant", confidence=0.6, record_time="2026-04-13T10:00:00Z")
    c4 = ConflictCandidate(value="ConfHigh", source="assistant", confidence=0.9, record_time="2026-04-13T10:00:00Z")
    res2 = ConflictResolver.resolve_conflict("name", [c3, c4])
    assert res2.winner == "ConfHigh", f"Confidence test failed: {res2.winner}"
    print("Case 2: Confidence (0.9 > 0.6) Pass")

    # 用例 3: 同 source+confidence 下新时间胜
    c5 = ConflictCandidate(value="OldTime", source="history", confidence=0.8, record_time="2026-04-13T10:00:00Z")
    c6 = ConflictCandidate(value="NewTime", source="history", confidence=0.8, record_time="2026-04-13T10:05:00Z")
    res3 = ConflictResolver.resolve_conflict("role", [c5, c6])
    assert res3.winner == "NewTime", f"Recency test failed: {res3.winner}"
    print("Case 3: Recency (10:05 > 10:00) Pass")

    # 用例 4: 全同分时稳定排序一致 (字典序)
    c7 = ConflictCandidate(value="Alice", source="explicit", confidence=1.0, record_time="2026-04-13T10:00:00Z")
    c8 = ConflictCandidate(value="Bob", source="explicit", confidence=1.0, record_time="2026-04-13T10:00:00Z")
    res4 = ConflictResolver.resolve_conflict("name", [c7, c8])
    # Bob > Alice (字典序倒序)
    assert res4.winner == "Bob", f"Tie-break test failed: {res4.winner}"
    print("Case 4: Tie-break (Lexicographical Bob > Alice) Pass")

    # 用例 5: 获胜理由包含 Source
    assert "Source: user" in res1.winner_reason, "Reasoning missing source info"
    print("Case 5: Reasoning Trace Pass")

    print("\nAll 5 test cases passed!")

if __name__ == "__main__":
    try:
        test_conflict_arbitration()
    except AssertionError as e:
        print(f"Test Failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Runtime Exception: {e}")
        sys.exit(1)
