"""
跨会话实体消歧引擎 (Entity Resolution Engine)
负责将提取到的实体名称映射到稳定的图谱根 ID (Root Identity)，
支持：RootID直连、Alias别名命中、Canonical归一化命中、语义相似度命中。
"""
import logging
import difflib
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

from config import identity_config

logger = logging.getLogger(__name__)

@dataclass
class ResolutionResult:
    resolved_root_id: Optional[str] = None
    resolved_name: Optional[str] = None
    confidence: float = 0.0
    reason: str = "none" # root_id | alias | canonical | similarity | none

class EntityResolver:
    def __init__(self, similarity_threshold: float = 0.92):
        self.similarity_threshold = similarity_threshold
        # 核心根域定义
        self.user_root = identity_config.user_id # user_001
        self.asst_root = identity_config.assistant_id # assistant_001

    def resolve(self, name: str, owner_id: str, session_ctx: Dict[str, Any] = None) -> ResolutionResult:
        """执行多级消歧检索"""
        if not name:
            return ResolutionResult()

        n = name.strip()
        ctx = session_ctx or {}
        
        # 1. Root ID 直命中 (硬匹配)
        if n == self.user_root:
            return ResolutionResult(self.user_root, ctx.get("user_real_name") or "User", 1.0, "root_id")
        if n == self.asst_root:
            return ResolutionResult(self.asst_root, ctx.get("assistant_real_name") or "Assistant", 1.0, "root_id")

        # 2. Alias 别名命中 (来自 Session 上下文或配置)
        # 这里的别名通常在 MemoryRetriever 中已同步到 context_canvas
        user_aliases = ["我", "主人", "机主", "管理员", "用户"]
        if ctx.get("user_real_name"): user_aliases.append(ctx["user_real_name"])
        
        asst_aliases = ["助手", "Andrew", "Hong", "你", "您", "AI", "管家"]
        if ctx.get("assistant_real_name"): asst_aliases.append(ctx["assistant_real_name"])

        if n in user_aliases:
            return ResolutionResult(self.user_root, ctx.get("user_real_name") or "User", 0.95, "alias")
        if n in asst_aliases:
            return ResolutionResult(self.asst_root, ctx.get("assistant_real_name") or "Assistant", 0.95, "alias")

        # 3. Canonical 命中 (利用 canonical.py 规则)
        try:
            from memory.identity.canonical import canonicalize_entity
            cn = canonicalize_entity(n)
            if cn == "user":
                return ResolutionResult(self.user_root, ctx.get("user_real_name") or "User", 0.90, "canonical")
            if cn == "andrew":
                return ResolutionResult(self.asst_root, ctx.get("assistant_real_name") or "Assistant", 0.90, "canonical")
        except Exception as exc:
            logger.debug("[EntityResolver] canonicalize failed for '%s': %s", n, exc)

        # 4. 相似度命中 (模糊匹配)
        # 对用户和助手现有的真实名称进行相似度比查
        best_match = None
        best_score = 0.0
        
        targets = [
            (self.user_root, ctx.get("user_real_name")),
            (self.asst_root, ctx.get("assistant_real_name"))
        ]
        
        for root_id, real_name in targets:
            if not real_name: continue
            score = difflib.SequenceMatcher(None, n.lower(), real_name.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = root_id
        
        if best_score >= self.similarity_threshold:
            # 映射置信度 0.75 ~ 0.89 (基于分值区间)
            mapped_conf = 0.75 + (best_score - self.similarity_threshold) * (0.14 / (1.0 - self.similarity_threshold))
            return ResolutionResult(best_match, None, round(mapped_conf, 2), "similarity")

        return ResolutionResult(reason="none")

    def filter_resolution(self, res: ResolutionResult, min_confidence: float = 0.95) -> Optional[str]:
        """只有高置信度才允许自动应用 (Merge)"""
        # 硬规则：永远不允许 user 与 assistant 交叉应用
        # Resolve 逻辑中已经通过 alias/root_id 隔离，此处确保置信度门槛
        if res.confidence >= min_confidence and res.resolved_root_id:
            return res.resolved_root_id
        return None
