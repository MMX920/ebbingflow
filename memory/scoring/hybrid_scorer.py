"""
复合评分引擎 (HybridScorer v1.0) — 阶段七核心
================================================
实现了 Qwen 红队分析中建议的两大核心算法：

1. RRF (Reciprocal Rank Fusion) 多路召回融合
   - 来自 信息检索领域的工业级融合算法
   - 不依赖绝对分数，只依赖"排名"，天然解决向量分数和图谱分数量纲不统一的问题
   - 公式：RRF_score = Σ 1 / (k + rank_i)，其中 k=60 是平滑系数

2. 艾宾浩斯时间衰减 (Ebbinghaus Forgetting Curve Time Decay)
   - 1 年前的记忆和 1 小时前的记忆不应该权重一样
   - 基于指数衰减模拟人类遗忘曲线：decay = exp(-0.693 * days / half_life)
   - 半衰期 (half_life) 默认 7 天——即 7 天前的记忆权重降为 50%

最终 Final Score 公式：
  final_score = (
      0.40 * semantic_similarity_normalized   # 语义相似度（向量分）
    + 0.25 * graph_hop_score                  # 图谱跳数关联度
    + 0.20 * time_decay_score                 # 时间衰减权重
    + 0.15 * impact_score_normalized          # 事件重要性（来自 Neo4j impact_score）
  )
  
  再通过 RRF 进行多路召回融合，得到最终排序。
"""
import math
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Dict

# 默认参数：从 config 读取，支持通过环境变量全局调整
try:
    from config import memory_config as _mem_cfg
    _DEFAULT_HALF_LIFE = _mem_cfg.time_decay_half_life_days
except Exception:
    _DEFAULT_HALF_LIFE = 7


logger = logging.getLogger(__name__)

# ============================================================
# RRF 平滑参数（工业推荐 k=60，来自 Cormack et al. 2009）
# ============================================================
RRF_K = 60

# ============================================================
# 时间衰减参数
# ============================================================
HALF_LIFE_DAYS = _DEFAULT_HALF_LIFE  # 从 config.py / .env 读取，当前 45 天
MIN_TIME_DECAY = 0.25     # 最低时间权重 (长记忆保底)
LOW_CONFIDENCE_THRESHOLD = 0.7  # 低置信度阈值 (低于此值不吃保底)

# ============================================================
# 各维度权重（四项加总必须 = 1.0）
# ============================================================
WEIGHT_SEMANTIC    = 0.40   # 语义相似度
WEIGHT_GRAPH_HOP   = 0.25   # 图谱关联度
WEIGHT_TIME_DECAY  = 0.20   # 时间衰减
WEIGHT_IMPACT      = 0.15   # 事件重要性


@dataclass
class ScoredCandidate:
    """
    带有多维评分信息的候选记忆条目。
    每个维度的分数均在 [0, 1] 之间。
    """
    content: str
    speaker: str
    timestamp: str             # ISO8601 字符串
    source_type: str           # "vector_chat", "vector_doc", "graph"
    source_name: str           # 来源 (session_id / 文档名)

    # 各维度原始分
    semantic_score: float = 0.0    # 向量相似度（原始）
    graph_hop_score: float = 0.0   # 图谱关联度 (1.0=有直接关联, 0.5=2跳, 0.0=无)
    impact_score: float = 5.0      # 事件重要性 (1~10)，文档类默认 5
    time_decay_score: float = 1.0  # 时间衰减值 (0~1)
    confidence: Optional[float] = None  # 置信度 (0~1)，缺失则视为高置信度
    source_msg_id: Optional[int] = None  # [M1] SQL 原始消息 ID，用于证据链回溯
    evidence_context: Optional[str] = None  # [M2] SQL evidence window for prompt grounding

    # 最终融合分
    final_score: float = 0.0
    graph_validated: bool = False
    in_prompt: bool = False

@dataclass
class UnifiedMemoryResult:
    """最终展现给引擎的统一格式"""
    content: str
    final_score: float
    speaker: str
    source_type: str
    timestamp: str
    graph_validated: bool
    source_name: str
    semantic_score: float
    time_decay_score: float
    graph_hop_score: float
    impact_score: float
    source_msg_id: Optional[int] = None # [M1]
    evidence_context: Optional[str] = None # [M2]
    in_prompt: bool = False


class TimeDecayCalculator:
    """
    艾宾浩斯遗忘曲线时间衰减计算器。
    
    原始公式：R = e^(-t/S)
    Ours:     decay = max(MIN_DECAY, exp(-0.693 * days / half_life))
    
    0.693 = ln(2)，使得当 t = half_life 时，decay 精确等于 0.5。
    """
    
    def __init__(self, half_life_days: float = HALF_LIFE_DAYS, min_decay: float = MIN_TIME_DECAY):
        self.half_life = half_life_days
        self.min_decay = min_decay
    
    def calculate(self, timestamp_str: str, confidence: Optional[float] = None) -> float:
        """
        根据时间戳字符串和置信度计算时间衰减权重。
        
        Args:
            timestamp_str: ISO8601 格式的时间戳字符串
            confidence: [0, 1] 之间的置信度分数
            
        Returns:
            float: [0.0, 1.0] 之间的衰减权重
        """
        if not timestamp_str:
            return 0.5  # 无时间戳时给中性权重
        
        try:
            # 解析时间戳
            ts = timestamp_str.replace("Z", "+00:00")
            # 兼容带时区和不带时区的格式
            try:
                parsed_time = datetime.fromisoformat(ts)
                if parsed_time.tzinfo is None:
                    # 无时区信息时假定为本地时间
                    parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            except ValueError:
                # 尝试解析不带时区的格式
                parsed_time = datetime.fromisoformat(timestamp_str[:19])
                parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)
            delta = now - parsed_time
            days_ago = delta.total_seconds() / 86400.0  # 转为天数
            
            if days_ago < 0:
                days_ago = 0.0  # 防止时钟漂移导致负值
            
            # 艾宾浩斯指数衰减 (raw_decay)
            raw_decay = math.exp(-0.693 * days_ago / self.half_life)
            
            # --- 修正逻辑：低置信度护栏 ---
            # 如果 confidence < 阈值，则不享受 MIN_TIME_DECAY 保底
            # 如果 confidence 为 None，按可享受保底处理 (兼容旧数据)
            is_low_confidence = (confidence is not None and confidence < LOW_CONFIDENCE_THRESHOLD)
            
            if is_low_confidence:
                final_decay = raw_decay
            else:
                final_decay = max(self.min_decay, raw_decay)
            
            # 保证 final_decay 始终在 [0, 1]
            return max(0.0, min(1.0, final_decay))
            
        except Exception as e:
            logger.debug(f"[TimeDecay] 时间戳解析失败（给中性权重）: {timestamp_str} | {e}")
            return 0.5


class HybridScorer:
    """
    多路召回融合评分引擎。
    
    工作流程：
    1. 接收来自向量检索 + 图谱检索的候选列表（可能有重叠）
    2. 对每个候选计算 4 个维度的归一化分数
    3. 通过加权求和得到 composite_score
    4. 通过 RRF 融合多路排名，得到 rrf_score
    5. 最终 final_score = 0.7 * composite_score + 0.3 * rrf_score
       (RRF 作为补充，防止某路召回独大)
    """
    
    def __init__(
        self,
        half_life_days: float = HALF_LIFE_DAYS,
        weights: Optional[Dict[str, float]] = None
    ):
        self.time_decay = TimeDecayCalculator(half_life_days=half_life_days)
        
        # 支持外部自定义权重（用于 A/B 测试或个性化）
        if weights:
            self.w_semantic = weights.get("semantic", WEIGHT_SEMANTIC)
            self.w_graph    = weights.get("graph_hop", WEIGHT_GRAPH_HOP)
            self.w_time     = weights.get("time_decay", WEIGHT_TIME_DECAY)
            self.w_impact   = weights.get("impact", WEIGHT_IMPACT)
        else:
            self.w_semantic = WEIGHT_SEMANTIC
            self.w_graph    = WEIGHT_GRAPH_HOP
            self.w_time     = WEIGHT_TIME_DECAY
            self.w_impact   = WEIGHT_IMPACT
    
    def _normalize_impact(self, impact_score: float) -> float:
        """将 impact_score (1~10) 归一化到 (0~1)"""
        return max(0.0, min(1.0, (impact_score - 1) / 9.0))
    
    def _compute_composite_score(self, candidate: ScoredCandidate) -> float:
        """
        计算四维加权复合分数。
        所有输入维度均已在 [0, 1]，加权输出也在 [0, 1]。
        """
        impact_norm = self._normalize_impact(candidate.impact_score)
        
        score = (
            self.w_semantic * candidate.semantic_score
            + self.w_graph  * candidate.graph_hop_score
            + self.w_time   * candidate.time_decay_score
            + self.w_impact * impact_norm
        )
        return score
    
    @staticmethod
    def _rrf_fusion(ranked_lists: List[List[ScoredCandidate]], k: int = RRF_K) -> Dict[str, float]:
        """
        Reciprocal Rank Fusion 算法。
        
        将多个独立排序的列表融合成一个统一的 RRF 分数字典。
        
        Args:
            ranked_lists: 多个已按各自维度排序的候选列表
            k: 平滑系数，防止排名靠前的条目过度主导（默认 60）
        
        Returns:
            Dict[content_hash -> rrf_score]
        """
        rrf_scores: Dict[str, float] = {}
        
        for ranked_list in ranked_lists:
            for rank, candidate in enumerate(ranked_list, start=1):
                # 唯一键：使用内容摘要以支持不同召回路的同一条目合并
                content_key = candidate.content.strip()
                rrf_scores[content_key] = rrf_scores.get(content_key, 0.0) + (1.0 / (k + rank))
        
        return rrf_scores

    def _mmr_deduplicate(self, candidates: List[ScoredCandidate], threshold: float = 0.85) -> List[ScoredCandidate]:
        """
        MMR (Maximal Marginal Relevance) 风格去重。
        在保留高分的同时，剔除语义高度重复的片段。
        """
        if not candidates: return []
        
        selected = [candidates[0]]
        for i in range(1, len(candidates)):
            cand = candidates[i]
            is_redundant = False
            for s in selected:
                # 极简 Jaccard 相似度计算 (字符级)
                set_cand = set(cand.content)
                set_s = set(s.content)
                denom = len(set_cand | set_s)
                similarity = len(set_cand & set_s) / denom if denom > 0 else 0
                
                if similarity > threshold:
                    is_redundant = True
                    break
            if not is_redundant:
                selected.append(cand)
        return selected
    
    def score(self, candidates: List[ScoredCandidate], top_k: int = 5) -> List[ScoredCandidate]:
        """
        主评分入口：对候选列表进行多维评分和 RRF 融合。
        
        Args:
            candidates: 来自各路召回的候选记忆列表（已填充各维度原始分）
            top_k: 最终返回的条数
        
        Returns:
            按 final_score 降序排列的 top_k 条记忆
        """
        if not candidates:
            return []
        
        # ── Step 1: 给每个候选计算时间衰减分 ──────────────────────
        for c in candidates:
            # [M2.3] Episode 相比原子事件具有更缓慢的遗忘速度（半衰期 * 2）
            h_life = HALF_LIFE_DAYS
            if c.source_type == "episode":
                h_life *= 2
            
            # 使用临时的衰减器计算
            calc = self.time_decay if h_life == HALF_LIFE_DAYS else TimeDecayCalculator(half_life_days=h_life)
            c.time_decay_score = calc.calculate(c.timestamp, confidence=c.confidence)
        
        # ── Step 2: 计算四维复合分 ─────────────────────────────────
        for c in candidates:
            if c.source_type == "episode":
                # Episode 默认具备更高的初始影响力 (Impact)
                c.impact_score = max(c.impact_score, 7.5)
            c.final_score = self._compute_composite_score(c)
        
        # ── Step 3: 准备 RRF 的多路排序列表 (五路融合) ───────────
        # 轨道 A：按嵌入语义相似度排序 (Vector Only)
        by_semantic = sorted(candidates, key=lambda c: c.semantic_score if c.source_type == "vector" else -1, reverse=True)
        
        # 轨道 B：按图谱关联度排序 (Graph Only)
        by_graph = sorted(candidates, key=lambda c: c.graph_hop_score if c.source_type == "graph" else -1, reverse=True)
        
        # 轨道 C：按时间新鲜度排序 (Recency)
        by_time = sorted(candidates, key=lambda c: c.time_decay_score, reverse=True)
        
        # 轨道 D：按重要性排序 (Impact)
        by_impact = sorted(candidates, key=lambda c: c.impact_score, reverse=True)

        # 轨道 E: 按 BM25 关键词评分排序 (BM25 Only)
        by_bm25 = sorted(candidates, key=lambda c: c.semantic_score if c.source_type == "bm25" else -1, reverse=True)
        
        # 轨道 F: 按 Episode 评分排序 (Episode Only) [M2.3]
        by_episode = sorted(candidates, key=lambda c: c.final_score if c.source_type == "episode" else -1, reverse=True)

        # ── Step 4: RRF 融合 ─────────────────────────────────────
        rrf_map = self._rrf_fusion([by_semantic, by_graph, by_time, by_impact, by_bm25, by_episode])
        
        # 归一化 RRF 分数到 [0, 1]
        max_rrf = max(rrf_map.values()) if rrf_map else 1.0
        
        # ── Step 5: 计算最终融合分 ────────────────────────────────
        for c in candidates:
            content_key = c.content.strip()
            rrf_score_norm = rrf_map.get(content_key, 0.0) / max_rrf if max_rrf > 0 else 0.0
            
            # 最终分 = 70% 多维复合分 + 30% RRF 排名融合分
            c.final_score = 0.70 * c.final_score + 0.30 * rrf_score_norm
        
        # ── Step 6: 最终排序与配额动态过滤 (Budget Control) ──────
        candidates.sort(key=lambda c: c.final_score, reverse=True)
        
        # 配额配置 (来自 config.py)
        budgets = {
            "graph": getattr(_mem_cfg, "budget_graph", 3),
            "vector": getattr(_mem_cfg, "budget_vector", 2),
            "episode": getattr(_mem_cfg, "budget_episode", 2), # 用户指定基线为 2
            "saga": 1, 
            "bm25": getattr(_mem_cfg, "budget_bm25", 1)
        }
        
        assigned = []
        rejected = []
        counts = {"graph": 0, "vector": 0, "saga": 0, "episode": 0, "bm25": 0}
        
        # ── 第一轮：严格按配额录用 (保证多样性) ───────────────────
        for c in candidates:
            s_type = c.source_type if c.source_type in budgets else "vector"
            if counts[s_type] < budgets[s_type]:
                assigned.append(c)
                counts[s_type] += 1
            else:
                rejected.append(c)
            
            if len(assigned) >= top_k:
                break
        
        # ── 第二轮：配额补位 (保证召回总量) ───────────────────────
        # 如果第一轮后没装满，从落选者中按分数高低补齐
        if len(assigned) < top_k and rejected:
            for r in rejected:
                assigned.append(r)
                if len(assigned) >= top_k:
                    break
        
        # 应用 MMR 去重 (在总量确定后)
        filtered_candidates = self._mmr_deduplicate(assigned, threshold=0.85)
        
        top = filtered_candidates
        logger.debug(
            f"[HybridScorer] 评分完成: {len(candidates)} -> top {top_k} | "
            f"最高分: {top[0].final_score:.4f} | 内容: {top[0].content[:30]}"
            if top else "[HybridScorer] 无候选记忆"
        )
        
        return top
