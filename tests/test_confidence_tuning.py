import os
import sys
import json
import unittest
from typing import List

# 项目根目录
sys.path.append(os.getcwd())

from memory.identity.conflict_resolver import ConflictResolver, ConflictCandidate

class ConfidenceTuner:
    def __init__(self):
        self.test_cases = [
            {
                "name": "User_High_vs_CRM_Normal",
                "candidates": [
                    ConflictCandidate(value="A", source="user", confidence=1.0, record_time="2024-01-01"),
                    ConflictCandidate(value="B", source="crm", confidence=1.0, record_time="2024-01-02")
                ],
                "expected_winner": "A" # User should win even if older
            },
            {
                "name": "CRM_High_vs_User_Low",
                "candidates": [
                    ConflictCandidate(value="B", source="crm", confidence=0.9, record_time="2024-01-02"),
                    ConflictCandidate(value="A", source="user", confidence=0.3, record_time="2024-01-01")
                ],
                "expected_winner": "B" # CRM should win if User confidence is very low
            },
            {
                "name": "Both_Normal_Recency",
                "candidates": [
                    ConflictCandidate(value="A", source="crm", confidence=0.8, record_time="2024-01-01"),
                    ConflictCandidate(value="B", source="crm", confidence=0.8, record_time="2024-01-02")
                ],
                "expected_winner": "B" # Recency win
            }
        ]

    def run_grid_search(self):
        # 模拟参数调优
        weights = [0.5, 0.6, 0.65, 0.7, 0.8]
        results = []

        for w in weights:
            score = 0
            # 这里我们通过 mock 修改 resolver 的内部权重逻辑
            # 或者简单地在 resolver 中发现 crm 的权重是基于外部配置的
            # 实际上 ConflictResolver 目前 hardcode 了 user=1.0, crm=0.65 (如果是通过 api 传进来的话)
            # 在这里我们模拟不同 crm_weight 下的表现
            
            passed = 0
            for case in self.test_cases:
                # 调整权重进行模拟
                winner = self._simulate_resolve(case["candidates"], crm_weight=w)
                if winner == case["expected_winner"]:
                    passed += 1
            
            acc = passed / len(self.test_cases)
            results.append({"crm_weight": w, "accuracy": acc})

        best = max(results, key=lambda x: x["accuracy"])
        
        report = {
            "grid_results": results,
            "best_params": best,
            "recommendation": {
                "CRM_SOURCE_WEIGHT": best["crm_weight"],
                "LOW_CONFIDENCE_THRESHOLD": 0.4
            }
        }

        os.makedirs(".data", exist_ok=True)
        with open(".data/tuning_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        
        print(f"Confidence Tuning Best Weight: {best['crm_weight']} (Acc: {best['accuracy']:.2%})")

    def _simulate_resolve(self, candidates: List[ConflictCandidate], crm_weight: float):
        # 简单的权重加权模拟
        scores = {}
        for c in candidates:
            weight = 1.0 if c.source == "user" else crm_weight
            # 惩罚极低置信度
            conf = c.confidence if c.confidence >= 0.4 else 0.1
            score = weight * conf
            scores[c.value] = max(scores.get(c.value, 0), score)
        
        return max(scores, key=scores.get)

if __name__ == "__main__":
    tuner = ConfidenceTuner()
    tuner.run_grid_search()
