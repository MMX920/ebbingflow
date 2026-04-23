"""
身份认知边界解析器 (Identity Resolver)
处于中间件最前置位，用于将相对视角的对话上下文（如：说话人、接收者）锁定为绝对的实体标识。
"""
import logging
from typing import Optional
from pydantic import BaseModel

from core.middleware import BaseMiddleware
from core.session import ChatSession
from config import identity_config

logger = logging.getLogger(__name__)


class Actor(BaseModel):
    """通信对面的实体强绑定模型"""
    speaker_id: str
    speaker_name: str
    target_id: str
    target_name: str
    
    def to_context_string(self) -> str:
        return f"当前发言人: {self.speaker_name} (ID: {self.speaker_id}) -> 倾听者: {self.target_name} (ID: {self.target_id})"


class IdentityResolverMiddleware(BaseMiddleware):
    """
    负责消除代词歧义的主客体绑定中间件。
    将解析出的 Actor 放入 Session 供下游的 Retriever 和 Extractor 使用。
    """
    def __init__(self):
        pass

    async def process_request(self, user_input: str, session: ChatSession) -> str:
        """
        在处理用户的输入之前，动态建立基础 Actor 状态。
        """
        try:
            # 优先从配置中读取 master ID
            current_user_id = identity_config.user_id
            current_asst_id = identity_config.assistant_id

            # 初始名称（后续可能会被 MemoryRetriever 中的图谱查询覆盖）
            current_user_name = session.context_canvas.get("user_real_name") or "用户"
            current_asst_name = session.context_canvas.get("assistant_real_name") or "AI助手"
            
            # 注入本轮对话的主客体 Actor 映射
            actor = Actor(
                speaker_id=current_user_id,
                speaker_name=current_user_name,
                target_id=current_asst_id,
                target_name=current_asst_name
            )
            
            # --- 注入硬规则：规范化别名策略 (Canonical Alias Policy) ---
            session.context_canvas["CANONICAL_ALIAS_POLICY"] = {
                "user_root": "user",
                "assistant_root": "andrew",
                "user_aliases": ["我", "用户", "user", "主人", "master"],
                "assistant_aliases": ["你", "您", "助理", "管家", "andrew", "assistant", "ping", "AI助手"],
                "prohibition": "严禁将用户(user)混淆为助手名, 严禁生成代词孤立点"
            }
            
            # 注入到会话的 Canvas 中
            session.context_canvas["Actor_Identity_Context"] = actor.to_context_string()
            
            # 为了方便其他组件获取强类型，直接挂在 session 对象上
            session.current_actor = actor
            
        except Exception as e:
            logger.warning(f"[IdentityResolver] 建立身份句柄失败: {e}")

        # 可以选做：调用超小参数模型（如 qwen 0.5b）快速把 user_input 里的 "你" 重写为 asst_name，"我" 重写为 user_name
        # 考虑到性能和延迟，最稳妥的是在 EventExtractor 提取时，把 actor 作为 prompt 变量传进去
        
        return user_input

    async def process_response(self, ai_output: str, session: ChatSession) -> str:
        """
        响应阶段无需处理，直接放行
        """
        return ai_output
