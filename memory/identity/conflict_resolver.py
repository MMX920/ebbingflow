from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class ConflictCandidate:
    value: str
    source: str  # explicit | user | assistant | history | system
    confidence: float = 1.0
    record_time: str = ""

@dataclass
class ArbitrationResult:
    winner: str
    winner_reason: str
    ranked_candidates: List[ConflictCandidate]

class ConflictResolver:
    """身份冲突仲裁器 (Identity Conflict Arbitrator)"""
    
    SOURCE_WEIGHTS = {
        "explicit":   1.00,
        "fast_track": 1.00, # 快路径更新权重等同于显式指令
        "user":       0.90,
        "assistant":  0.70,
        "crm":        0.65, # Phase-2.5: CRM Sync Source
        "history":    0.60,
        "system":     0.50
    }

    @classmethod
    def resolve_conflict(cls, slot: str, candidates: List[ConflictCandidate]) -> ArbitrationResult:
        if not candidates:
            return ArbitrationResult("", "No candidates provided", [])
        
        if len(candidates) == 1:
            return ArbitrationResult(candidates[0].value, "Single candidate skip-through", candidates)

        # 1. 评分与排序逻辑
        def score_candidate(c: ConflictCandidate):
            # 获取层级权重
            s_weight = cls.SOURCE_WEIGHTS.get(c.source, 0.40)
            # 处理时间 (ISO8601 可直接字符串比较或转 float)
            # 为简单起见，且 record_time 是 ISO，字符串比较能代表先后
            return (s_weight, c.confidence, c.record_time, c.value)

        # 稳定排序：倒序排列得分因子
        ranked = sorted(candidates, key=score_candidate, reverse=True)
        winner = ranked[0]
        
        # 2. 构造获胜理由
        reason = f"Source: {winner.source} (W: {cls.SOURCE_WEIGHTS.get(winner.source)}) | Confidence: {winner.confidence}"
        if len(ranked) > 1:
            competitor = ranked[1]
            if cls.SOURCE_WEIGHTS.get(winner.source) == cls.SOURCE_WEIGHTS.get(competitor.source):
                reason += f" | Recency won over {competitor.source} @ {competitor.record_time}"

        return ArbitrationResult(
            winner=winner.value,
            winner_reason=reason,
            ranked_candidates=ranked
        )
