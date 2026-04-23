import os
import sys
import asyncio
import botpy
import time
from botpy import logging
from botpy.message import C2CMessage, DirectMessage
from dotenv import load_dotenv

#  路径兼容性修复 
# 确保即使在 integrations/ 目录下直接运行，也能正确导入根目录的 core 和 bridge
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 现在可以正常导入了
from core.chat_engine import get_standard_engine
from core.session import ChatSession
from memory.history.repository import SqlHistoryRepository
from config import identity_config
# 

load_dotenv()
logger = logging.get_logger()
# 与 server.py 保持一致，使跨端记忆同步
MY_BRAIN_SESSION_ID = "master_session" 

class EbbingFlowBot(botpy.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = get_standard_engine()
        # Initialize SQL repository to ensure messages have IDs for evidence chaining
        history_repo = SqlHistoryRepository()
        self.session = ChatSession(
            session_id=MY_BRAIN_SESSION_ID,
            user_id=identity_config.user_id,  # 使用主用户 ID
            history_repo=history_repo
        )
        self._msg_seq_counter = int(time.time())
        
        print("\n" + "="*50)
        print(" EbbingFlow QQ 桥接网关已启动")
        print(f" 记忆锚点: {MY_BRAIN_SESSION_ID}")
        print("="*50 + "\n")

    def get_next_seq(self):
        self._msg_seq_counter += 1
        return self._msg_seq_counter

    async def on_c2c_message_create(self, message: C2CMessage):
        user_text = message.content.strip()
        print(f" [QQ] 收到消息: {user_text}")
        response_text = ""
        try:
            async for chunk in self.engine.chat_stream(user_text, self.session):
                response_text += chunk
            await self.api.post_c2c_message(
                openid=message.author.user_openid,
                msg_type=0, 
                msg_id=message.id,
                content=f"{response_text}",
                msg_seq=self.get_next_seq()
            )
        except Exception as e:
            print(f" 运行逻辑出错: {e}")

    async def on_direct_message_create(self, message: DirectMessage):
        user_text = message.content.strip()
        print(f" [频道] 收到消息: {user_text}")
        response_text = ""
        try:
            async for chunk in self.engine.chat_stream(user_text, self.session):
                response_text += chunk
            await self.api.post_dms_messages(
                guild_id=message.guild_id,
                content=f"{response_text}",
                msg_id=message.id
            )
        except Exception as e:
            print(f" 逻辑出错: {e}")

if __name__ == "__main__":
    intents = botpy.Intents.none()
    intents.public_guild_messages = True
    intents.direct_message = True
    intents.value |= (1 << 25)
    
    client = EbbingFlowBot(intents=intents)
    appid = os.getenv("QQ_BOT_APPID")
    secret = os.getenv("QQ_BOT_SECRET")
    
    print(f" 正在启动 QQ Bot (AppID: {appid})")
    client.run(appid=appid, secret=secret)