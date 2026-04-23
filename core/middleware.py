"""
中间件框架模块 (Middleware Framework)
所有对输入输出的拦截、检索、身份识别、后台异步写入都在此处串联。
"""
from typing import Any, List, Dict
import logging

from core.session import ChatSession

logger = logging.getLogger(__name__)


class BaseMiddleware:
    """中间件基类，所有的业务中间件均继承此基类"""
    
    async def process_request(self, user_input: str, session: ChatSession) -> str:
        """
        请求阶段：修改用户输入，或通过检索向 session.context_canvas 中注入知识和背景。
        :param user_input: 用户本轮的文本输入
        :param session: 会话对象
        :return: （可能被修改过的）用户输入文本
        """
        return user_input

    async def process_response(self, ai_output: str, session: ChatSession) -> str:
        """
        响应阶段：通常用于后台异步任务，如提取事件、异步写入图谱等。
        :param ai_output: AI 本轮返回的文本
        :param session: 会话对象
        :return: （可能被修改过的）AI 文本
        """
        return ai_output


class MiddlewareChain:
    """中间件调用链"""
    def __init__(self):
        self.middlewares: List[BaseMiddleware] = []

    def add(self, middleware: BaseMiddleware):
        """添加一个中间件到尾部"""
        self.middlewares.append(middleware)

    async def execute_request_phase(self, user_input: str, session: ChatSession) -> str:
        """从前向后执行 Request 拦截"""
        current_input = user_input
        for mw in self.middlewares:
            try:
                current_input = await mw.process_request(current_input, session)
            except Exception as e:
                logger.error(f"Error in {mw.__class__.__name__}.process_request: {e}")
        return current_input

    async def execute_response_phase(self, ai_output: str, session: ChatSession) -> str:
        """从后向前执行 Response 拦截 (类似洋葱模型)"""
        current_output = ai_output
        for mw in reversed(self.middlewares):
            try:
                current_output = await mw.process_response(current_output, session)
            except Exception as e:
                logger.error(f"[中间件执行链异常] {mw.__class__.__name__}.process_response 崩溃: {e}")
        return current_output

    async def close(self):
        """逆序关闭所有中间件"""
        for mw in reversed(self.middlewares):
            if hasattr(mw, "close") and callable(mw.close):
                try:
                    await mw.close()
                except Exception as e:
                    logger.error(f"Error closing middleware {mw.__class__.__name__}: {e}")
