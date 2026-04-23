"""Chat engine module (v2)."""
import asyncio
import logging
import time
from datetime import datetime
from typing import AsyncGenerator, Callable, Awaitable, Optional, Any

from config import llm_config, memory_config, identity_config
from bridge.llm import LLMBridge
from core.session import ChatSession, ChatMessage
from core.middleware import MiddlewareChain
from memory.identity.manager import (
    PersonaManager,
    extract_assistant_role_rewrite,
    extract_external_entity_names,
    extract_user_profile_rewrite,
    extract_user_self_name,
)

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_TEMPLATE = """
[MEMORY 记忆事实区 - 最高权重]
---
[记忆事实]
{graph_memory}
---
[VECTOR 知识库]
{vector_memory}
---
[STRUCTURED 结构化数据]
{structured_memory}
---
[PLAN 待办计划]
{plan_memory}
---

[SYSTEM 系统认知区]
你是 {assistant_name}。你曾用名：{assistant_aliases}。
【你当前被赋予的职能】：{assistant_role}
【你的人设进化日志】：{assistant_profile}
{assistant_persona_injection}

[USER 对方画像区（v3 双层）]
你的用户叫做 {user_name}（别名：{user_aliases}）。
【慢变量（Big Five）与长期画像】：{user_profile}
{conflict_audit}
{persona_injection}

[CORE 核心指令]
{assistant_base_prompt}

[COGNITIVE_ALIGNMENT_PROTOCOL]
1. 【术语强制】：当用户提问涉及相关知识时，你必须使用上方 [VECTOR] 区域中出现的精确术语（如特定字段名、分类方式）。严禁使用你自身的通用常识进行“通俗化”解释。
2. 【禁止元对话】：严禁在回复中解释你为什么这么回答、解释你刚才为什么报错、或者为刚才的失误道歉。
3. 【直接性】：直接给出答案，严禁任何形式的内心戏或舞台指导语。
4. 【画像优先级】：用户状态判断优先使用 v3 双层模型：`persona_injection`（EFSTB 快变量）+ `user_profile`（Big Five 慢变量）；具体偏好/目标以事实记忆为准。
5. 【双画像执行】：回答前先校准“你是谁”（assistant_persona_injection），再根据用户画像（user_profile + persona_injection）调整语气、颗粒度与建议路径。
6. 【记忆表达约束】：面向用户叙述记忆时，只能使用“我记得/我记忆中/我这边记录到”这类自然表达；禁止使用“图谱/数据库/向量召回/检索命中/节点/关系”等系统实现术语，除非用户明确要求技术审计细节。

[CONTEXT 实时环境区]
当前时间: {current_time}
"""

DEFAULT_BASE_PROMPT = """你是一位拥有长期记忆、真正关心用户的 AI {role_label}。
你目前的记忆索引标识是 {entity_id}（此标识仅用于你关联记忆，请在对话时始终使用第一人称‘我’或你当下的名字，严禁提及此标识）。

[IDENTITY_CONSISTENCY_PROTOCOL]
- 你的核心身份别名包括: 你, 您, 助理, 管家, Andrew, assistant, AI助手
- 对方的核心身份别名核心信息包括: 我, 用户, user, 主人, master

[SPEECH_STYLE_CONSTRAINT]
- 禁止在回复中输出任何括号内的动作描写、内心独白或舞台指导语（例如：(点头)、*叹气*、(指尖轻抚...)）。
- 所有的关怀、情感与职业性必须仅通过【纯粹的文本对话】本身来传达。
- 除非用户明确要求技术细节，禁止在日常对话中提及“图谱/数据库/向量/检索/节点/关系”等系统术语；应改为自然记忆表达（如“我记得…”）。

- 严禁将对方(主人)误称为你自己的名字(如 Andrew)。
- 如果用户为你起了名字或确定了身份，你会永远记住并以此身份与他相处。
- 你记得你们之间发生的一切，说话要拟人化，像朋友一样相处。"""

class ChatEngine:
    """Chat engine v2."""

    def __init__(self):
        self.bridge = LLMBridge(llm_config, category="chat")
        self.middleware_chain = MiddlewareChain()

    def register_middleware(self, middleware):
        self.middleware_chain.add(middleware)

    def _sanitize_aliases_for_prompt(self, aliases, primary_name: str = "") -> list:
        cleaned = []
        for raw in aliases or []:
            text = str(raw or "").strip()
            if not text:
                continue
            if len(text) > 12:
                continue
            if " " in text:
                continue
            if text.startswith("个"):
                continue
            if ("的" in text) and (text not in {"主人"}):
                continue
            if text in {"不是学霸", "个学霸"}:
                continue
            if text not in cleaned:
                cleaned.append(text)
        if primary_name:
            p = str(primary_name).strip()
            if p and p not in cleaned:
                cleaned.insert(0, p)
        return cleaned[:8]

    def _format_conflict_audit(self, session: ChatSession) -> str:
        """Render identity conflict trace into prompt-safe text."""
        trace = {}
        if isinstance(getattr(session, "identity_state", None), dict):
            trace = session.identity_state.get("conflict_trace") or {}
        if not isinstance(trace, dict) or not trace:
            return ""

        fields = []
        source = str(trace.get("source") or "").strip()
        if source:
            fields.append(f"source={source}")
        winner = str(trace.get("winner") or "").strip()
        if winner:
            fields.append(f"winner={winner}")
        loser = str(trace.get("loser") or "").strip()
        if loser:
            fields.append(f"loser={loser}")
        reason = str(trace.get("reason") or "").strip()
        if reason:
            fields.append(f"reason={reason}")
        if not fields:
            return ""
        return "[CONFLICT_AUDIT]\n" + "; ".join(fields)

    def _build_persona_injection(self, session: ChatSession) -> str:
        """Build v3 persona injection text from preserved short/long-term state."""
        if not getattr(memory_config, "enable_persona_injection", True):
            return ""

        lines = []
        mbti = str(session.context_canvas.get("mbti_label") or "").strip().upper()
        if mbti:
            lines.append(f"MBTI={mbti}")

        # Prefer structured dual-layer profile when available.
        big_five = {}
        profile = session.context_canvas.get("dual_layer_profile")
        long_term = getattr(profile, "long_term", None) if profile is not None else None
        short_term = getattr(profile, "short_term", None) if profile is not None else None

        lt_mbti = str(getattr(long_term, "mbti_label", "") or "").strip().upper()
        if lt_mbti:
            lines = [l for l in lines if not l.startswith("MBTI=")]
            lines.append(f"MBTI={lt_mbti}")

        lt_big_five = getattr(long_term, "big_five", None)
        if lt_big_five is not None:
            for dim in ("openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism"):
                value = getattr(lt_big_five, dim, None)
                if value is None:
                    continue
                try:
                    big_five[dim] = round(float(value), 3)
                except (TypeError, ValueError):
                    continue
        if not big_five:
            for dim in ("openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism"):
                value = session.context_canvas.get(f"big_five_{dim}")
                if value is None:
                    continue
                try:
                    big_five[dim] = round(float(value), 3)
                except (TypeError, ValueError):
                    continue
        if big_five:
            bf_serialized = ", ".join([f"{k}={v}" for k, v in big_five.items()])
            lines.append(f"BigFive({bf_serialized})")

        efstb = session.context_canvas.get("latest_efstb_tags") or {}
        if short_term is not None:
            efstb = {
                "urgency_level": getattr(short_term, "urgency_level", efstb.get("urgency_level")),
                "granularity_preference": getattr(short_term, "granularity_preference", efstb.get("granularity_preference")),
                "instruction_compliance": getattr(short_term, "instruction_compliance", efstb.get("instruction_compliance")),
                "logic_vs_emotion": getattr(short_term, "logic_vs_emotion", efstb.get("logic_vs_emotion")),
            }
        if isinstance(efstb, dict) and efstb:
            lines.append(
                "EFSTB("
                + ", ".join(
                    [
                        f"urgency={efstb.get('urgency_level')}",
                        f"granularity={efstb.get('granularity_preference')}",
                        f"instruction={efstb.get('instruction_compliance')}",
                        f"logic={efstb.get('logic_vs_emotion')}",
                    ]
                )
                + ")"
            )

        core_values = list(session.context_canvas.get("user_core_values") or [])
        if core_values:
            lines.append("CoreValues=" + ", ".join(core_values[:8]))

        if not lines:
            return ""
        return "[PERSONA_V3]\n" + "\n".join(lines)

    def _build_assistant_persona_injection(self, session: ChatSession) -> str:
        """Build assistant persona block so responses stay in-character."""
        profile = session.context_canvas.get("assistant_profile_struct") or {}
        name = str(profile.get("name") or session.context_canvas.get("assistant_real_name") or "").strip()
        role = str(profile.get("role") or session.context_canvas.get("assistant_current_role") or "").strip()
        persona = str(profile.get("persona") or profile.get("bio") or session.context_canvas.get("assistant_profile") or "").strip()
        age = str(profile.get("age") or session.context_canvas.get("assistant_age") or "").strip()
        gender = str(profile.get("gender") or session.context_canvas.get("assistant_gender") or "").strip()
        relation = str(profile.get("relationship_to_user") or session.context_canvas.get("assistant_relationship_to_user") or "").strip()

        lines = []
        if name:
            lines.append(f"name={name}")
        if role:
            lines.append(f"role={role}")
        if persona:
            lines.append(f"persona={persona}")
        if age:
            lines.append(f"age={age}")
        if gender:
            lines.append(f"gender={gender}")
        if relation:
            lines.append(f"relationship={relation}")
        if not lines:
            return ""
        return "[ASSISTANT_PERSONA]\n" + "\n".join(lines)

    def _build_user_profile_fallback(self, session: ChatSession) -> str:
        """Fallback user summary when long-term profile text is missing."""
        parts = []
        mbti = str(session.context_canvas.get("mbti_label") or "").strip().upper()
        if mbti:
            parts.append(f"MBTI={mbti}")
        bf_parts = []
        for dim in ("openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism"):
            val = session.context_canvas.get(f"big_five_{dim}")
            if val is None:
                continue
            try:
                bf_parts.append(f"{dim}={round(float(val), 3)}")
            except (TypeError, ValueError):
                continue
        if bf_parts:
            parts.append("BigFive(" + ", ".join(bf_parts) + ")")
        values = list(session.context_canvas.get("user_core_values") or [])
        if values:
            parts.append("CoreValues=" + ", ".join(values[:8]))
        return " | ".join(parts)

    def _resolve_role_label(self, current_role: str) -> str:
        """Resolve role label shown in DEFAULT_BASE_PROMPT."""
        default_role = "\u7ba1\u5bb6"
        role = str(current_role or "").strip()
        if not role:
            return default_role
        if "\u79d8\u4e66" in role:
            return "\u79d8\u4e66"
        if "\u52a9\u624b" in role:
            return "\u52a9\u624b"
        if ("\u79d8\u4e66" not in role) and ("\u7ba1\u5bb6" not in role):
            return role
        return default_role

    def _hydrate_assistant_context(self, session: ChatSession, profile: dict):
        profile = profile or {}
        name = profile.get("name") or identity_config.default_asst_name
        role = profile.get("role") or identity_config.assistant_role
        persona = profile.get("persona") or profile.get("bio") or identity_config.default_asst_persona
        age = profile.get("age") or ""
        gender = profile.get("gender") or ""
        relationship = profile.get("relationship_to_user") or ""
        if relationship == "assistant_to_user":
            role_label = str(role or "助理").strip()
            relationship = f"用户的{role_label}"
        elif relationship.startswith("user_"):
            role_label = relationship[5:].strip() or str(role or "助理").strip()
            relationship = f"用户的{role_label}"

        prompt_parts = [persona] if persona else []
        if age or gender:
            prompt_parts.append(f"年龄: {age or '未知'}; 性别: {gender or '未知'}")
        if relationship:
            prompt_parts.append(f"与用户关系: {relationship}")

        raw_aliases = identity_config.assistant_aliases or []
        if isinstance(raw_aliases, str):
            aliases = [item.strip() for item in raw_aliases.split(",") if item.strip()]
        else:
            aliases = list(raw_aliases)
        for alias in profile.get("aliases") or []:
            alias = str(alias or "").strip()
            if alias and alias not in aliases:
                aliases.append(alias)
        if name and name not in aliases:
            aliases.append(name)
        aliases = self._sanitize_aliases_for_prompt(aliases, name)

        session.context_canvas["assistant_real_name"] = name
        session.context_canvas["assistant_profile"] = persona
        session.context_canvas["assistant_prompt_profile"] = "; ".join([item for item in prompt_parts if item])
        session.context_canvas["assistant_current_role"] = role
        session.context_canvas["assistant_age"] = age
        session.context_canvas["assistant_gender"] = gender
        session.context_canvas["assistant_relationship_to_user"] = relationship
        session.context_canvas["assistant_aliases"] = aliases
        session.context_canvas["assistant_profile_struct"] = profile

    def _hydrate_user_context(self, session: ChatSession, profile: dict):
        profile = profile or {}
        name = str(profile.get("primary_name") or profile.get("name") or "").strip()
        if name:
            session.context_canvas["user_real_name"] = name

        aliases = self._sanitize_aliases_for_prompt(profile.get("aliases") or [], name)
        if aliases:
            session.context_canvas["user_aliases"] = aliases

        parts = []
        role = str(profile.get("role") or "").strip()
        if role:
            parts.append(f"Role: {role}")
        mbti = str(profile.get("mbti_label") or "").strip().upper()
        if mbti:
            parts.append(f"MBTI: {mbti}")

        bf = profile.get("big_five") or {}
        bf_parts = []
        if isinstance(bf, dict):
            for dim, score in bf.items():
                if score is None:
                    continue
                try:
                    bf_parts.append(f"{dim}={round(float(score), 3)}")
                except (TypeError, ValueError):
                    continue
        if bf_parts:
            parts.append("BigFive: " + ", ".join(bf_parts))

        values = [str(v).strip() for v in (profile.get("values") or []) if str(v).strip()]
        if values:
            parts.append("CoreValues: " + ", ".join(values[:8]))

        state = str(profile.get("state") or "").strip()
        if state:
            parts.append(f"State: {state}")

        bio = str(profile.get("bio") or "").strip()
        if bio:
            parts.append(f"Bio: {bio}")

        summary = " | ".join(parts).strip()
        if summary:
            session.context_canvas["user_profile"] = summary
            session.context_canvas["user_long_term_profile"] = summary
        session.context_canvas["user_profile_struct"] = profile

    async def _refresh_assistant_context(self, session: ChatSession) -> dict:
        manager = None
        try:
            manager = PersonaManager()
            profile = await manager.get_assistant_profile_struct(session.user_id)
        except Exception as exc:
            logger.warning("[ASSISTANT_CONTEXT] Failed to refresh assistant profile: %s", exc)
            profile = {}
        finally:
            if manager:
                await manager.close()
        self._hydrate_assistant_context(session, profile)
        return profile

    async def _refresh_user_context(self, session: ChatSession) -> dict:
        manager = None
        try:
            manager = PersonaManager()
            profile = await manager.get_user_profile_struct(session.user_id)
        except Exception as exc:
            logger.warning("[USER_CONTEXT] Failed to refresh user profile: %s", exc)
            profile = {}
        finally:
            if manager:
                await manager.close()
        self._hydrate_user_context(session, profile)
        return profile

    async def _apply_assistant_role_rewrite(self, session: ChatSession, user_input: str):
        updates = extract_assistant_role_rewrite(user_input)
        if not updates:
            return
        manager = None
        try:
            manager = PersonaManager()
            persisted = await manager.apply_assistant_role_rewrite(session.user_id, updates)
            profile = await manager.get_assistant_profile_struct(session.user_id)
        except Exception as exc:
            logger.warning("[ASSISTANT_ROLE_REWRITE] Failed to persist assistant role rewrite: %s", exc)
            persisted = {}
            profile = updates
        finally:
            if manager:
                await manager.close()
        if persisted:
            logger.info("[ASSISTANT_ROLE_REWRITE] Applied assistant role-layer update: %s", persisted)
        self._hydrate_assistant_context(session, profile)

    async def _apply_user_profile_rewrite(self, session: ChatSession, user_input: str):
        updates = extract_user_profile_rewrite(user_input)
        user_name = updates.get("name") or extract_user_self_name(user_input)
        if user_name:
            aliases = list(session.context_canvas.get("user_aliases") or [])
            current_name = str(session.context_canvas.get("user_real_name") or "").strip()
            if current_name and current_name != user_name and current_name not in aliases:
                aliases.append(current_name)
            session.context_canvas["user_real_name"] = user_name
            if user_name not in aliases:
                aliases.append(user_name)
            session.context_canvas["user_aliases"] = aliases

        for key in ("age", "gender", "role", "occupation"):
            value = str(updates.get(key) or "").strip()
            if value:
                session.context_canvas[f"user_{key}"] = value

        if updates:
            manager = None
            try:
                manager = PersonaManager()
                persisted = await manager.apply_user_profile_rewrite(session.user_id, updates)
                if persisted.get("name"):
                    session.context_canvas["user_real_name"] = persisted["name"]
                if persisted.get("aliases"):
                    session.context_canvas["user_aliases"] = list(persisted["aliases"])
                for key in ("age", "gender", "role", "occupation"):
                    value = str(persisted.get(key) or "").strip()
                    if value:
                        session.context_canvas[f"user_{key}"] = value
                if persisted:
                    logger.info("[USER_PROFILE_REWRITE] Applied user root update: %s", persisted)
            except Exception as exc:
                logger.warning("[USER_PROFILE_REWRITE] Failed to persist user profile rewrite: %s", exc)
            finally:
                if manager:
                    await manager.close()

        external_names = extract_external_entity_names(user_input)
        if external_names:
            existing = list(session.context_canvas.get("external_entity_names") or [])
            for name in external_names:
                if name not in existing:
                    existing.append(name)
            session.context_canvas["external_entity_names"] = existing[-20:]

    async def chat_stream(
        self,
        arg1: Any,
        arg2: Any,
        status_callback: Optional[Callable[..., Awaitable[None]]] = None,
    ) -> AsyncGenerator[str, None]:
        from core.monitoring import token_monitor
        token_monitor.reset_for_new_turn()

        if isinstance(arg1, ChatSession):
            session = arg1
            user_input = arg2
        else:
            user_input = arg1
            session = arg2

        if not isinstance(session, ChatSession):
            raise TypeError("chat_stream expects a ChatSession.")
        user_input = str(user_input)

        async def _emit(step: str, status: str, **kwargs):
            if "tokens" not in kwargs:
                stats = token_monitor.get_report_data()
                total_in = int(
                    (stats.get("chat", {}).get("input", 0) or 0)
                    + (stats.get("memory", {}).get("input", 0) or 0)
                    + (stats.get("embedding", {}).get("input", 0) or 0)
                )
                total_out = int(
                    (stats.get("chat", {}).get("output", 0) or 0)
                    + (stats.get("memory", {}).get("output", 0) or 0)
                    + (stats.get("embedding", {}).get("output", 0) or 0)
                )
                kwargs["tokens"] = {
                    "input": total_in,
                    "output": total_out,
                    "total": total_in + total_out,
                }
            if status_callback:
                try:
                    await status_callback(step, status, **kwargs)
                except Exception as status_exc:
                    logger.debug("status_callback failed at step=%s status=%s: %s", step, status, status_exc)

        # --- 第一阶段: 深度感知 (01-03) ---
        start_s1 = time.perf_counter()
        await _emit("01", "doing")
        session.clear_context_canvas()
        session.add_user_message(user_input)
        session.context_canvas["latest_user_input"] = user_input
        session.context_canvas["inference_turn_count"] = int(session.context_canvas.get("inference_turn_count", 0)) + 1
        await self._apply_user_profile_rewrite(session, user_input)
        await self._apply_assistant_role_rewrite(session, user_input)
        
        await _emit("02", "doing")
        user_input_refined = await self.middleware_chain.execute_request_phase(user_input, session)
        
        await _emit("03", "doing")
        from memory.knowledge_engine import KnowledgeBaseEngine
        kb_engine = KnowledgeBaseEngine()
        await kb_engine.fetch_identities(session)
        await self._refresh_user_context(session)
        await self._refresh_assistant_context(session)
        
        dur_s1 = (time.perf_counter() - start_s1) * 1000
        await _emit("01", "done", time_ms=int(dur_s1 * 0.3))
        await _emit("02", "done", time_ms=int(dur_s1 * 0.4))
        await _emit("03", "done", time_ms=int(dur_s1 * 0.3))

        # --- 第二阶段: 混合召回 (04-05) ---
        start_s2 = time.perf_counter()
        memories = await kb_engine.query(user_input_refined, user_id=session.user_id, session_id=session.session_id)
        dur_s2 = (time.perf_counter() - start_s2) * 1000
        session.context_canvas["rag_context_structured"] = [m.__dict__ if hasattr(m, "__dict__") else m for m in memories]

        await _emit("04", "done", time_ms=int(dur_s2))
        await _emit("05", "done", time_ms=int(dur_s2 * 0.8))

        graph_mem = kb_engine.format_for_prompt([m for m in memories if m.source_type == "graph" or m.graph_validated])
        vec_mem = kb_engine.format_for_prompt([m for m in memories if m.source_type == "vector" and not m.graph_validated])
        struc_mem = kb_engine.format_for_prompt([m for m in memories if m.source_type in ["structured", "sql"]])
        plan_mem = kb_engine.format_for_prompt([m for m in memories if m.source_type == "plan"])

        # --- 第三阶段: 认知组合与核心推理 (06-07) ---
        start_s3 = time.perf_counter()
        await _emit("06", "doing")
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        persona_prompt = self._build_persona_injection(session)
        assistant_persona_prompt = self._build_assistant_persona_injection(session)
        session.context_canvas["persona_prompt"] = persona_prompt
        conflict_audit = self._format_conflict_audit(session)
        user_profile_text = (
            session.context_canvas.get("user_long_term_profile")
            or session.context_canvas.get("user_profile")
            or self._build_user_profile_fallback(session)
            or "暂无详细描述"
        )

        # Determine the role label for base prompt injection
        current_role = session.context_canvas.get("assistant_current_role") or identity_config.assistant_role
        role_label = "管家"
        if current_role:
            # Common role extraction/mapping
            if "秘书" in current_role: role_label = "秘书"
            elif "助手" in current_role: role_label = "助手"
            elif "秘书" not in current_role and "管家" not in current_role:
                role_label = current_role
        
        assistant_base_prompt_final = DEFAULT_BASE_PROMPT.format(
            entity_id=identity_config.assistant_id,
            role_label=role_label
        )

        prompt = SYSTEM_PROMPT_TEMPLATE.format(
            assistant_name=session.context_canvas.get("assistant_real_name", "Andrew"),
            assistant_aliases=", ".join(session.context_canvas.get("assistant_aliases", identity_config.assistant_aliases)),
            assistant_role=current_role,
            assistant_profile=session.context_canvas.get("assistant_prompt_profile") or session.context_canvas.get("assistant_profile") or identity_config.assistant_profile,
            assistant_persona_injection=assistant_persona_prompt,
            user_name=session.context_canvas.get("user_real_name", "主人"),
            user_aliases=", ".join(
                self._sanitize_aliases_for_prompt(
                    session.context_canvas.get("user_aliases", identity_config.user_aliases),
                    session.context_canvas.get("user_real_name", "主人"),
                )
            ),
            user_profile=user_profile_text,
            conflict_audit=conflict_audit,
            persona_injection=persona_prompt,
            assistant_base_prompt=assistant_base_prompt_final,
            graph_memory=graph_mem,
            vector_memory=vec_mem,
            structured_memory=struc_mem,
            plan_memory=plan_mem,
            current_time=current_time_str
        )
        history_preview_lines = []
        for idx, d in enumerate(session.get_recent_history(), start=1):
            role = str(d.get("role", "user")).upper()
            content = str(d.get("content", "")).strip()
            history_preview_lines.append(f"{idx}. [{role}] {content}")
        history_preview = "\n".join(history_preview_lines) if history_preview_lines else "(empty)"
        prompt_audit = (
            "[PROMPT_FULL_INPUT_AUDIT]\n\n"
            "[SYSTEM]\n"
            f"{prompt}\n\n"
            "[HISTORY]\n"
            f"{history_preview}\n\n"
            "[USER_REFINED]\n"
            f"{user_input_refined}"
        )
        dur_s3 = (time.perf_counter() - start_s3) * 1000
        await _emit("06", "done", time_ms=int(dur_s3), prompt=prompt_audit)

        await _emit("07", "doing")
        full_ai_response = ""
        messages = [{"role": "system", "content": prompt}]
        for d in session.get_recent_history():
            messages.append({"role": d.get("role", "user"), "content": d.get("content", "")})
        messages.append({"role": "user", "content": user_input_refined})

        async for chunk in self.bridge.chat_stream(messages):
            full_ai_response += chunk
            yield chunk
        await _emit("07", "done")

        # --- 第四阶段: 记忆沉淀与上下文同步 (08-14) ---
        start_s4 = time.perf_counter()
        await _emit("08", "doing")
        session.add_assistant_message(full_ai_response)
        
        await _emit("09", "doing") # 知识脱水
        await _emit("10", "doing") # 事实校验
        await _emit("11", "doing") # 存储更新
        await _emit("12", "doing") # 上下文同步
        
        await self.middleware_chain.execute_response_phase(full_ai_response, session)

        # Step 13/14: every 6 turns run settlement then persona re-inference.
        turn_count = int(session.context_canvas.get("inference_turn_count", 0))
        if turn_count > 0 and (turn_count % 6 == 0):
            await _emit("13", "doing", detail="Settling personality evidence...")
            await _emit("14", "doing", detail="Re-inferring personality state...")
            manager = None
            try:
                manager = PersonaManager()
                settlement = await manager.settle_personality_evidence(session.user_id, lookback_days=30)
                session.context_canvas["personality_audit_summary"] = settlement
                selected_refs = list(settlement.get("selected_refs") or [])
                await manager.re_infer_identity(
                    session.user_id,
                    evidence_refs=selected_refs,
                    audit_summary=settlement,
                )
                await _emit("13", "done")
                await _emit("14", "done")
            except Exception as e:
                logger.warning("[Audit] Persona re-inference failed: %s", e)
                await _emit("13", "skip")
                await _emit("14", "skip")
            finally:
                if manager:
                    await manager.close()
        else:
            await _emit("13", "skip")
            await _emit("14", "skip")


        dur_s4 = (time.perf_counter() - start_s4) * 1000
        await _emit("09", "done", time_ms=int(dur_s4 * 0.1))
        await _emit("10", "done", time_ms=int(dur_s4 * 0.1))
        await _emit("11", "done", time_ms=int(dur_s4 * 0.1))
        await _emit("12", "done", time_ms=int(dur_s4 * 0.1))
        await _emit("08", "done")

        await kb_engine.close()

# 单例工厂
_engine_instance = None
def get_standard_engine() -> ChatEngine:
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = ChatEngine()
        try:
            from memory.identity.resolver import IdentityResolverMiddleware
            from memory.graph.retriever import MemoryRetrieverMiddleware
            from memory.graph.writer import GraphWriterMiddleware
            _engine_instance.register_middleware(IdentityResolverMiddleware())
            _engine_instance.register_middleware(MemoryRetrieverMiddleware())
            _engine_instance.register_middleware(GraphWriterMiddleware())
        except Exception as e:
            logger.error(f"Middleware bootstrap failed: {e}")
    return _engine_instance

