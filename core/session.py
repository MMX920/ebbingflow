"""
会话管理模块 (Session Management)
"""
import uuid
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

from config import memory_config

try:
    from memory.history.repository import ChatHistoryRepository
except ImportError:
    ChatHistoryRepository = Any


class ChatMessage:
    """单条聊天消息"""
    def __init__(self, role: str, content: str, name: Optional[str] = None, msg_id: Optional[int] = None):
        self.role = role
        self.content = content
        self.name = name  # 可选：记录确切的 Entity Name
        self.msg_id = msg_id # 从数据库中获取的自增 ID (用于证据链)
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        msg = {
            "role": self.role, 
            "content": self.content,
            "timestamp": self.timestamp
        }
        if self.name:
            msg["name"] = self.name
        if self.msg_id:
            msg["msg_id"] = self.msg_id
        return msg


class ChatSession:
    """
    单次/长期会话管理
    管理全局 Session ID、短期内存窗口（最近 N 轮上下文）
    """
    def __init__(self, session_id: Optional[str] = None, user_id: str = "default_user", history_repo: Optional[ChatHistoryRepository] = None):
        self.session_id: str = session_id or str(uuid.uuid4())
        self.user_id: str = user_id
        self.history_repo = history_repo
        
        # 历史记录列表 (存放所有的短时记忆)
        self.history: List[ChatMessage] = []
        
        # 记忆窗口大小，由环境变量控制，默认通常为 6（即3轮对话问答）
        self.window_size: int = memory_config.window_size

        # 全局上下文画布，中间件可以向这里面注入检索到的信息
        self.context_canvas: Dict[str, Any] = {"vector_status": "active"}

        # 身份权利分级状态 (Identity Precedence State)
        self.identity_state = {
            "asst_name": "Andrew",
            "asst_canonical": "andrew",
            "user_canonical": "user",
            "source": "default",      # 优先级: explicit > history > default
            "updated_at": datetime.now().isoformat()
        }

        # 尝试从仓储恢复会话 (不再直接使用 VectorStorer)
        self._restore_history()

    def _history_message_limit(self) -> int:
        """
        MEMORY_WINDOW_SIZE 按“对话轮数”解释：
        1 轮 = user + assistant = 2 条消息
        """
        if self.window_size <= 0:
            return 0
        return self.window_size * 2

    async def restore_from_repo(self):
        """[M1] 强制异步恢复历史记录，确保在 Async 上下文中正确加载证据链 ID"""
        try:
            if not self.history_repo: return
            if hasattr(self.history_repo, "async_get_recent"):
                data = await self.history_repo.async_get_recent(
                    user_id=self.user_id, session_id=self.session_id, limit=self._history_message_limit()
                )
                self._apply_history_items(data)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Async history restore failed: {e}")

    def _restore_history(self):
        """从仓储接口恢复对话记录 (同步 Fallback)"""
        try:
            if not self.history_repo: return
            history_data = self.history_repo.get_recent(
                user_id=self.user_id, session_id=self.session_id, limit=self._history_message_limit()
            )
            self._apply_history_items(history_data)
        except Exception as e:
            self.context_canvas["vector_status"] = "degraded"
            import logging
            logging.getLogger(__name__).debug(f"Sync history restore skipped or failed: {e}")

    def _apply_history_items(self, history_data: List[Dict]):
        if not history_data: return
        self.history = []
        for item in history_data:
            # 支持从 SQL 或 Chroma 返回的字典中提取 ID
            msg_id = item.get("id") or item.get("msg_id") 
            m = ChatMessage(item["role"], item["content"], item.get("name"), msg_id=msg_id)
            m.timestamp = item.get("timestamp")
            self.history.append(m)
        if self.history:
            print(f"[Session] Restored {len(self.history)} history records from SQL")

    def add_user_message(self, content: str, name: Optional[str] = None) -> ChatMessage:
        msg = ChatMessage(role="user", content=content, name=name or self.user_id)
        if self.history_repo:
            try:
                # [M1] Persist immediately to get SQL ID for Evidence Chain
                msg_id = self.history_repo.append_turn(
                    user_id=self.user_id, session_id=self.session_id,
                    role="user", speaker=name or self.user_id, content=content
                )
                msg.msg_id = msg_id
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"SQL Persistence failed (User): {e}")
        self.history.append(msg)
        return msg

    def add_assistant_message(self, content: str, name: Optional[str] = None) -> ChatMessage:
        msg = ChatMessage(role="assistant", content=content, name=name)
        if self.history_repo:
            try:
                # [M1] Persist immediately to get SQL ID for Evidence Chain
                msg_id = self.history_repo.append_turn(
                    user_id=self.user_id, session_id=self.session_id,
                    role="assistant", speaker="assistant", content=content
                )
                msg.msg_id = msg_id
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"SQL Persistence failed (Asst): {e}")
        self.history.append(msg)
        return msg


    def get_recent_history(self) -> List[Dict[str, str]]:
        """获取最近 N 条消息用于直接投喂 LLM（避免上下文超出或混滑）"""
        limit = self._history_message_limit()
        recent = self.history[-limit:] if limit > 0 else self.history
        return [msg.to_dict() for msg in recent]

    def clear_context_canvas(self):
        """每轮对话开始前清空画布，但保留系统状态位与长期计数器"""
        prev_vector = self.context_canvas.get("vector_status", "active")
        prev_persona = self.context_canvas.get("persona_status", "active")
        un_episoded_turns = self.context_canvas.get("un_episoded_turns", 0)
        ep_events_buffer = self.context_canvas.get("ep_events_buffer", [])
        dual_layer_profile = self.context_canvas.get("dual_layer_profile")
        latest_efstb_tags = self.context_canvas.get("latest_efstb_tags")
        mbti_label = self.context_canvas.get("mbti_label")
        user_core_values = self.context_canvas.get("user_core_values", [])
        big_five_openness = self.context_canvas.get("big_five_openness")
        big_five_conscientiousness = self.context_canvas.get("big_five_conscientiousness")
        big_five_extraversion = self.context_canvas.get("big_five_extraversion")
        big_five_agreeableness = self.context_canvas.get("big_five_agreeableness")
        big_five_neuroticism = self.context_canvas.get("big_five_neuroticism")
        inference_turn_count = self.context_canvas.get("inference_turn_count", 0)
        
        self.context_canvas = {
            "vector_status": prev_vector,
            "persona_status": prev_persona,
            "un_episoded_turns": un_episoded_turns,
            "ep_events_buffer": ep_events_buffer,
            "dual_layer_profile": dual_layer_profile,
            "latest_efstb_tags": latest_efstb_tags,
            "mbti_label": mbti_label,
            "user_core_values": user_core_values,
            "big_five_openness": big_five_openness,
            "big_five_conscientiousness": big_five_conscientiousness,
            "big_five_extraversion": big_five_extraversion,
            "big_five_agreeableness": big_five_agreeableness,
            "big_five_neuroticism": big_five_neuroticism,
            "inference_turn_count": inference_turn_count,
        }
