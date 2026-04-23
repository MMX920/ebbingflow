"""
用户画像字段契约 (Profile Field Contract)
------------------------------------------
只保留必须结构化存储在 Entity 节点上的5类核心字段（白名单）：
  1. name       - 姓名（主键级字段，有 alias 管理）
  2. age        - 年龄
  3. gender     - 性别
  4. mbti_label - MBTI 人格标签
  5. big_five_* - 五大人格维度（openness/conscientiousness/extraversion/agreeableness/neuroticism）

其余一切事实信息（职业、地点、学历、传记、角色、偏好等）均以 Fact 节点的形式
直接写入 Neo4j，不再经过本文件过滤。
"""
import logging

logger = logging.getLogger(__name__)

# ── 白名单：仅这5类字段允许写入 Entity 节点属性 ──────────────────────────────
ALLOWED_PROFILE_FIELDS = {
    # 基础身份
    "name",
    "age",
    "gender",
}

# 别名映射（仅限上述核心字段）
ALIAS_MAP = {
    "userage": "age",
    "sex":     "gender",
    "mbti":    "mbti_label",
}


def normalize_profile_updates(raw: dict) -> tuple[dict, dict]:
    """
    规约画像更新字典。
    1. 执行别名替换。
    2. 将命中白名单的字段放入 clean_updates（写 Entity 节点属性）。
    3. 其余字段放入 dropped_updates（由调用方写入 Fact 节点）。
    4. 移除空值。

    返回: (clean_updates, dropped_updates)
    """
    clean: dict = {}
    dropped: dict = {}

    if not raw:
        return clean, dropped

    for k, v in raw.items():
        norm_k = k.lower().strip()
        final_k = ALIAS_MAP.get(norm_k, norm_k)

        # 过滤无效值
        if v in (None, "", "未知", "unknown"):
            continue

        if final_k in ALLOWED_PROFILE_FIELDS:
            clean[final_k] = v
        else:
            # 保留原始键名，供 Fact 节点写入
            dropped[k] = v

    if dropped:
        logger.debug(
            "📌 [ProfileContract] %d field(s) will be stored as Facts: %s",
            len(dropped),
            list(dropped.keys()),
        )

    return clean, dropped
