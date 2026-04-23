"""
CRM SQL 检索模块
-----------------
从共享业务 PostgreSQL 中读取结构化 CRM 数据，
供 ebbingflow 回答相关的财务与销售跟进类问题。

对应表 (来自 schema.sql):
  events        — 从对话中提取的关键事件
  stage_history — 成交阶段变更记录
  tasks         — 销售代办任务
  deal_signals  — 购买意向信号
  risks         — 风险标记
  conversations — 对话元信息（用于按客户名过滤）
  messages      — 原始消息（证据链）
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 关键词路由：判断查询类型
# ---------------------------------------------------------------------------

_TASK_KEYWORDS = re.compile(
    r"代办|任务|待办|todo|跟进|follow.?up|提醒|reminder|截止|due", re.IGNORECASE
)
_STAGE_KEYWORDS = re.compile(
    r"阶段|stage|进展|进度|成交|商机|deal|pipeline|推进", re.IGNORECASE
)
_SIGNAL_KEYWORDS = re.compile(
    r"意向|signal|意愿|兴趣|interested|购买|买|决策|预算|budget", re.IGNORECASE
)
_RISK_KEYWORDS = re.compile(
    r"风险|risk|危险|问题|障碍|objection|卡点|竞品|competitor", re.IGNORECASE
)
_EVENT_KEYWORDS = re.compile(
    r"发生|事件|记录|说了|提到|event|聊了|情况|进展|跟进|信息", re.IGNORECASE
)


def classify_query(query: str) -> List[str]:
    """
    返回命中的查询类别列表，可多命中。
    可能的值: 'tasks', 'stage', 'signals', 'risks', 'events'
    """
    types = []
    if _TASK_KEYWORDS.search(query):
        types.append("tasks")
    if _STAGE_KEYWORDS.search(query):
        types.append("stage")
    if _SIGNAL_KEYWORDS.search(query):
        types.append("signals")
    if _RISK_KEYWORDS.search(query):
        types.append("risks")
    if _EVENT_KEYWORDS.search(query) or not types:
        types.append("events")  # events 作为兜底
    return types


def is_crm_query(query: str) -> bool:
    """
    启发式判断：该查询是否需要 SQL CRM 数据补充。
    真阳性条件：包含客户/销售/商机等 CRM 语义词汇。
    """
    crm_pattern = re.compile(
        r"客户|销售|跟进|商机|客户名|follow.?up|deal|pipeline|员工|业务员"
        r"|代办|任务|todo|意向|风险|阶段|stage|成交|进展|情况如何|进度",
        re.IGNORECASE,
    )
    return bool(crm_pattern.search(query))


# ---------------------------------------------------------------------------
# 辅助：从查询中提取可能的客户名 / 销售名
# ---------------------------------------------------------------------------

def extract_name_hint(query: str) -> Optional[str]:
    """
    简单启发式：提取「XX客户」「客户XX」「叫XX的」等模式中的名字。
    返回第一个候选词（2-8 字），或 None。
    """
    patterns = [
        r"客户[：:「『\s]*([\w\u4e00-\u9fff]{2,8})",
        r"([\w\u4e00-\u9fff]{2,8})[：:」』\s]*客户",
        r"叫\s*([\w\u4e00-\u9fff]{2,8})",
        r"名叫\s*([\w\u4e00-\u9fff]{2,8})",
        r"关于\s*([\w\u4e00-\u9fff]{2,8})",
    ]
    for pat in patterns:
        m = re.search(pat, query)
        if m:
            return m.group(1).strip()
    return None


# ---------------------------------------------------------------------------
# 核心查询函数
# ---------------------------------------------------------------------------

async def query_crm_context(
    query: str,
    tenant_id: str,
    name_hint: Optional[str] = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    主入口：根据查询类型分派 SQL 查询，返回结构化行列表。
    每行包含 type (来源表) + 其余字段。
    """
    from .pool import get_pool

    pool = await get_pool()
    if pool is None:
        return []

    name_hint = name_hint or extract_name_hint(query)
    query_types = classify_query(query)
    results: List[Dict[str, Any]] = []

    for qt in query_types:
        try:
            rows = await _dispatch(pool, qt, tenant_id, name_hint, limit)
            results.extend(rows)
        except Exception as exc:
            logger.warning("[SQL CRM] query_type=%s error: %s", qt, exc)

    return results


async def _dispatch(pool, query_type: str, tenant_id: str, name_hint: Optional[str], limit: int) -> List[Dict[str, Any]]:
    if query_type == "tasks":
        return await _query_tasks(pool, tenant_id, name_hint, limit)
    elif query_type == "stage":
        return await _query_stage_history(pool, tenant_id, name_hint, limit)
    elif query_type == "signals":
        return await _query_deal_signals(pool, tenant_id, name_hint, limit)
    elif query_type == "risks":
        return await _query_risks(pool, tenant_id, name_hint, limit)
    else:
        return await _query_events(pool, tenant_id, name_hint, limit)


# ---------------------------------------------------------------------------
# 各表查询实现
# ---------------------------------------------------------------------------

async def _query_events(pool, tenant_id: str, name_hint: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """从 events 表查询最近的关键事件，可按客户名过滤。"""
    if name_hint:
        rows = await pool.fetch(
            """
            SELECT e.id, e.title, e.description, e.occurred_at,
                   e.source, e.confidence, e.conversation_id, e.agent_uuid,
                   c.sim_id
            FROM events e
            JOIN conversations c ON c.id = e.conversation_id
            WHERE e.tenant_id = $1
              AND (e.agent_uuid ILIKE $2 OR c.sim_id ILIKE $2)
            ORDER BY COALESCE(e.occurred_at, e.created_at) DESC
            LIMIT $3
            """,
            tenant_id, f"%{name_hint}%", limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT e.id, e.title, e.description, e.occurred_at,
                   e.source, e.confidence, e.conversation_id, e.agent_uuid
            FROM events e
            WHERE e.tenant_id = $1
            ORDER BY COALESCE(e.occurred_at, e.created_at) DESC
            LIMIT $2
            """,
            tenant_id, limit,
        )
    return [{"type": "event", **dict(r)} for r in rows]


async def _query_tasks(pool, tenant_id: str, name_hint: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """从 tasks 表查询代办任务，优先返回未完成的。"""
    if name_hint:
        rows = await pool.fetch(
            """
            SELECT t.id, t.title, t.description, t.due_date, t.status,
                   t.priority, t.source, t.confidence, t.conversation_id, t.agent_uuid,
                   c.sim_id
            FROM tasks t
            JOIN conversations c ON c.id = t.conversation_id
            WHERE t.tenant_id = $1
              AND (t.agent_uuid ILIKE $2 OR c.sim_id ILIKE $2)
            ORDER BY CASE WHEN t.status IN ('draft', 'confirmed') THEN 0 ELSE 1 END,
                     t.due_date ASC NULLS LAST
            LIMIT $3
            """,
            tenant_id, f"%{name_hint}%", limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT t.id, t.title, t.description, t.due_date, t.status,
                   t.priority, t.source, t.confidence, t.conversation_id, t.agent_uuid
            FROM tasks t
            WHERE t.tenant_id = $1
            ORDER BY CASE WHEN t.status IN ('draft', 'confirmed') THEN 0 ELSE 1 END,
                     t.due_date ASC NULLS LAST
            LIMIT $2
            """,
            tenant_id, limit,
        )
    return [{"type": "task", **dict(r)} for r in rows]


async def _query_stage_history(pool, tenant_id: str, name_hint: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """从 stage_history 查询商机阶段变化。"""
    if name_hint:
        rows = await pool.fetch(
            """
            SELECT sh.id, sh.stage_value, sh.changed_at,
                   sh.source, sh.confidence, sh.conversation_id, sh.agent_uuid,
                   c.sim_id
            FROM stage_history sh
            JOIN conversations c ON c.id = sh.conversation_id
            WHERE sh.tenant_id = $1
              AND (sh.agent_uuid ILIKE $2 OR c.sim_id ILIKE $2)
            ORDER BY sh.changed_at DESC NULLS LAST
            LIMIT $3
            """,
            tenant_id, f"%{name_hint}%", limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT sh.id, sh.stage_value, sh.changed_at,
                   sh.source, sh.confidence, sh.conversation_id, sh.agent_uuid
            FROM stage_history sh
            WHERE sh.tenant_id = $1
            ORDER BY sh.changed_at DESC NULLS LAST
            LIMIT $2
            """,
            tenant_id, limit,
        )
    return [{"type": "stage", **dict(r)} for r in rows]


async def _query_deal_signals(pool, tenant_id: str, name_hint: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """从 deal_signals 查询购买意向信号。"""
    if name_hint:
        rows = await pool.fetch(
            """
            SELECT ds.id, ds.signal_value, ds.description,
                   ds.created_at, ds.source, ds.confidence, ds.conversation_id, ds.agent_uuid,
                   c.sim_id
            FROM deal_signals ds
            JOIN conversations c ON c.id = ds.conversation_id
            WHERE ds.tenant_id = $1
              AND (ds.agent_uuid ILIKE $2 OR c.sim_id ILIKE $2)
            ORDER BY ds.created_at DESC NULLS LAST
            LIMIT $3
            """,
            tenant_id, f"%{name_hint}%", limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT ds.id, ds.signal_value, ds.description,
                   ds.created_at, ds.source, ds.confidence, ds.conversation_id, ds.agent_uuid
            FROM deal_signals ds
            WHERE ds.tenant_id = $1
            ORDER BY ds.created_at DESC NULLS LAST
            LIMIT $2
            """,
            tenant_id, limit,
        )
    return [{"type": "signal", **dict(r)} for r in rows]


async def _query_risks(pool, tenant_id: str, name_hint: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """从 risks 查询风险标记。"""
    if name_hint:
        rows = await pool.fetch(
            """
            SELECT r.id, r.risk_level, r.description,
                   r.created_at, r.source, r.confidence, r.conversation_id, r.agent_uuid,
                   c.sim_id
            FROM risks r
            JOIN conversations c ON c.id = r.conversation_id
            WHERE r.tenant_id = $1
              AND (r.agent_uuid ILIKE $2 OR c.sim_id ILIKE $2)
            ORDER BY r.created_at DESC NULLS LAST
            LIMIT $3
            """,
            tenant_id, f"%{name_hint}%", limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT r.id, r.risk_level, r.description,
                   r.created_at, r.source, r.confidence, r.conversation_id, r.agent_uuid
            FROM risks r
            WHERE r.tenant_id = $1
            ORDER BY r.created_at DESC NULLS LAST
            LIMIT $2
            """,
            tenant_id, limit,
        )
    return [{"type": "risk", **dict(r)} for r in rows]


# ---------------------------------------------------------------------------
# 格式化函数：把行列表转换成 LLM 友好文本
# ---------------------------------------------------------------------------

_TYPE_LABELS = {
    "event": "关键事件",
    "task": "代办任务",
    "stage": "阶段变化",
    "signal": "购买信号",
    "risk": "风险标记",
}


def format_crm_rows(rows: List[Dict[str, Any]]) -> str:
    """将 SQL 结果格式化为 prompt 友好的中文文本块。"""
    if not rows:
        return ""

    lines: List[str] = ["[CRM结构化数据]"]
    for r in rows:
        row_type = r.get("type", "unknown")
        label = _TYPE_LABELS.get(row_type, row_type)

        if row_type == "event":
            ts = _fmt_ts(r.get("occurred_at"))
            title = r.get("title", "")
            desc = r.get("description", "")
            content = f"{title}" + (f": {desc}" if desc else "")
            lines.append(
                f"- {label} {content} "
                f"(置信度:{r.get('confidence','')}, 来源:{r.get('source','')}{ts})"
            )
        elif row_type == "task":
            due = _fmt_ts(r.get("due_date"), date_only=True)
            title = r.get("title", "")
            desc = r.get("description", "")
            content = f"{title}" + (f": {desc}" if desc else "")
            lines.append(
                f"- {label} {content} "
                f"状态:{r.get('status','')} 优先级:{r.get('priority','')}{due}"
            )
        elif row_type == "stage":
            ts = _fmt_ts(r.get("changed_at"))
            lines.append(
                f"- {label}: 阶段转移至 {r.get('stage_value','')} {ts}"
            )
        elif row_type == "signal":
            ts = _fmt_ts(r.get("created_at"))
            desc = r.get("description", "")
            lines.append(
                f"- {label}[{r.get('signal_value','')}] {desc} "
                f"(置信度:{r.get('confidence','')}{ts})"
            )
        elif row_type == "risk":
            ts = _fmt_ts(r.get("created_at"))
            desc = r.get("description", "")
            lines.append(
                f"- {label}[{r.get('risk_level','')}] {desc} "
                f"(置信度:{r.get('confidence','')}{ts})"
            )
        else:
            lines.append(f"- [{label}] {r}")

    return "\n".join(lines)


def _fmt_ts(val: Any, date_only: bool = False) -> str:
    if val is None:
        return ""
    if isinstance(val, datetime):
        return f" 时间:{val.strftime('%Y-%m-%d') if date_only else val.strftime('%Y-%m-%d %H:%M')}"
    s = str(val).strip()
    return f" 时间:{s[:10] if date_only else s[:16]}" if s else ""
