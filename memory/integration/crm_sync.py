"""
CRM 外部系统同步适配器 (CRM Sync Adapter)
负责外部 CRM 数据的解析、归一化、以及与内部冲突仲裁器的对接。
"""
import hashlib
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

class CRMChange(BaseModel):
    external_event_id: str
    owner_id: str
    target_id: str  # user_001 | assistant_001
    slot: str       # name | role | state | alias
    value: str
    confidence: float = 1.0
    timestamp: str

class CRMUpsertRequest(BaseModel):
    changes: List[CRMChange]

class CRMReplayRequest(BaseModel):
    batch_id: str
    changes: List[CRMChange]

def normalize_crm_payload(change: CRMChange) -> Dict[str, Any]:
    """把 CRM 字段映射到内部槽位名称"""
    # 简单的透传映射，未来可扩展别名转换
    return {
        "slot": change.slot,
        "value": change.value,
        "confidence": change.confidence,
        "source": "crm"
    }

def build_conflict_candidates(change: CRMChange, weight: float) -> List[Any]:
    """
    包装成 ConflictCandidate 列表供仲裁器使用。
    由于 ConflictResolver 期望的是一组 Candidate，这里我们把 CRM 数据包成具有固定权重的对象。
    """
    from memory.identity.conflict_resolver import ConflictCandidate
    return [
        ConflictCandidate(
            value=change.value,
            source="crm",
            confidence=change.confidence,
            record_time=change.timestamp
        )
    ]

def make_idempotency_key(owner_id: str, external_event_id: str, slot: str, value: str) -> str:
    """生成回放幂等键"""
    raw = f"{owner_id}:{external_event_id}:{slot}:{value}"
    return hashlib.sha256(raw.encode()).hexdigest()
