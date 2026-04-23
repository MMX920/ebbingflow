"""
实体归一化中心 (Canonical Resolver)
----------------------------------
强制将各种随机、模糊的代词和别名对齐到系统核心实体 ID。
"""

CANONICAL_MAP = {
    # 助手侧归一化
    "你": "andrew",
    "您": "andrew",
    "助手": "andrew",
    "助理": "andrew",
    "管家": "andrew",
    "ai": "andrew",
    "assistant": "andrew",
    "andrew": "andrew",
    "ping": "andrew",
    "机器人": "andrew",
    
    # 用户侧归一化
    "我": "user",
    "主人": "user",
    "用户": "user",
    "user": "user",
    "master": "user"
}

# 严禁进入图谱的噪音词
ISOLATE_PRONOUNS = {"他", "她", "它", "他们", "她们", "它们", "谁", "这", "那"}

def canonicalize_entity(name: str) -> str:
    """
    执行实体名强制归一。
    返回: 归一化后的字符串，如果无法识别且为噪音点则返回空字符串。
    """
    if not name:
        return ""
    
    clean_name = name.strip().lower()
    
    # 1. 查表映射
    if clean_name in CANONICAL_MAP:
        return CANONICAL_MAP[clean_name]
    
    # 2. 噪音拦截
    if clean_name in ISOLATE_PRONOUNS:
        return ""
    
    # 3. 原样保留 (如果是具体的人名或物体名)
    return name
