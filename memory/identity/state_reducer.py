"""
身份状态权重缩减器 (Identity State Reducer)
---------------------------------------
核心逻辑：explicit (显式命令) > history (历史记录) > default (系统默认)
"""
from datetime import datetime

# 优先级定义
PRIORITY = {
    "explicit": 3,
    "fast_track": 3,  # 等价于显式命令
    "history": 2,
    "default": 1
}

from memory.identity.conflict_resolver import ConflictResolver, ConflictCandidate

def reduce_identity_state(current: dict, incoming: dict) -> dict:
    """
    基于 ConflictResolver 仲裁器决定身份状态更新。
    支持对关键槽位 (asst_name, user_name_logic) 实施冲突判定。
    """
    if not incoming:
        return current
        
    slots_to_resolve = ["asst_name", "user_name_logic"]
    update_copy = current.copy()
    
    curr_src = current.get("source", "default")
    in_src = incoming.get("source", "default")
    
    # 记录冲突审计信息
    traces = {}
    last_reason = ""

    for slot in slots_to_resolve:
        if slot in incoming or slot in current:
            candidates = []
            # 添加当前值
            if slot in current and current[slot]:
                candidates.append(ConflictCandidate(
                    value=current[slot],
                    source=curr_src,
                    record_time=current.get("updated_at", "")
                ))
            # 添加新值
            if slot in incoming:
                candidates.append(ConflictCandidate(
                    value=incoming[slot],
                    source=in_src,
                    record_time=datetime.now().isoformat()
                ))
            
            # 执行仲裁
            if len(candidates) > 0:
                result = ConflictResolver.resolve_conflict(slot, candidates)
                update_copy[slot] = result.winner
                last_reason = result.winner_reason
                # 记录前 3 名候补 (Audit Trace)
                traces[slot] = [vars(c) for c in result.ranked_candidates[:3]]

    # 汇总更新
    update_copy.update({k: v for k, v in incoming.items() if k not in slots_to_resolve})
    update_copy["winner_reason"] = last_reason
    update_copy["conflict_trace"] = traces
    update_copy["updated_at"] = datetime.now().isoformat()
    return update_copy
