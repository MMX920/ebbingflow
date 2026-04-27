from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import logging
import os
import sqlite3

logger = logging.getLogger(__name__)

class ChatHistoryRepository(ABC):
    """对话历史仓储接口 (Repository Pattern)"""
    
    @abstractmethod
    def append_turn(self, *, user_id: str, session_id: str, role: str, speaker: str, content: str, timestamp: Optional[str] = None, metadata: Optional[Dict] = None) -> None:
        """持久化单轮对话片段"""
        pass

    @abstractmethod
    def get_recent(self, *, user_id: str, session_id: str, limit: int = 20) -> List[Dict]:
        """获取最近 N 条历史记录"""
        pass

    @abstractmethod
    def history_window(self, *, user_id: str, session_id: str, offset: int = 0, limit: int = 50) -> List[Dict]:
        """滑动窗口获取历史记录"""
        pass

    @abstractmethod
    def get_message_by_id(self, msg_id: int) -> Optional[Dict]:
        """获取特定 ID 的消息"""
        pass

class ChromaHistoryRepository(ChatHistoryRepository):
    """ChromaDB 仓储适配器：复用现有 VectorStorer 体系"""
    
    def __init__(self):
        try:
            from memory.vector.storer import VectorStorer
            self._storer = VectorStorer()
        except Exception as e:
            logger.error(f"ChromaHistoryRepository Initialization Failed: {e}")
            self._storer = None

    def append_turn(self, *, user_id: str, session_id: str, role: str, speaker: str, content: str, timestamp: Optional[str] = None, metadata: Optional[Dict] = None) -> None:
        if not self._storer: return
        try:
            self._storer.store_chat_turn(
                speaker=speaker,
                content=content,
                session_id=session_id,
                user_id=user_id,
                role=role,
                timestamp=timestamp,
            )
        except Exception as e:
            logger.error(f"ChromaHistoryRepository Save Error: {e}")

    def get_recent(self, *, user_id: str, session_id: str, limit: int = 20) -> List[Dict]:
        if not self._storer: return []
        try:
            return self._storer.get_recent_chat_history(user_id, limit=limit)
        except Exception as e:
            logger.error(f"ChromaHistoryRepository Load Error: {e}")
            return []

    def history_window(self, *, user_id: str, session_id: str, offset: int = 0, limit: int = 50) -> List[Dict]:
        """最小化实现：用于分页或滑动窗口（Chroma 暂不支持 offset，仅支持 limit）"""
        return self.get_recent(user_id=user_id, session_id=session_id, limit=limit)

    def get_message_by_id(self, msg_id: int) -> Optional[Dict]:
        return None  # Chroma 暂不支持按物理 ID 直接检索

class SqlHistoryRepository(ChatHistoryRepository):
    """SQL 仓储适配器：支持 PostgreSQL 和 SQLite (自愈式主存)"""
    
    def __init__(self):
        from memory.sql.pool import get_db
        from config import sqlite_config
        self.get_db = get_db
        self.sqlite_db_path = sqlite_config.db_path

    def _append_turn_sqlite_sync(self, *, user_id: str, session_id: str, role: str, speaker: str, content: str, timestamp: Optional[str] = None, metadata: Optional[Dict] = None) -> int:
        """
        在事件循环已运行的场景下，走 sqlite3 同步写入，避免 run_until_complete 嵌套报错。
        """
        import json
        os.makedirs(os.path.dirname(self.sqlite_db_path), exist_ok=True)
        conn = sqlite3.connect(self.sqlite_db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO ef_chat_sessions (session_id, user_id) VALUES (?, ?)",
                (session_id, user_id),
            )
            meta_json = json.dumps(metadata) if metadata else None
            if timestamp:
                cur.execute(
                    "INSERT INTO ef_chat_messages (session_id, role, speaker, content, timestamp, metadata) VALUES (?, ?, ?, ?, ?, ?)",
                    (session_id, role, speaker, content, timestamp, meta_json),
                )
            else:
                cur.execute(
                    "INSERT INTO ef_chat_messages (session_id, role, speaker, content, metadata) VALUES (?, ?, ?, ?, ?)",
                    (session_id, role, speaker, content, meta_json),
                )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()

    async def _ensure_session(self, user_id: str, session_id: str):
        """确保会话元数据存在"""
        async with self.get_db() as conn:
            # 兼容性查询
            is_sqlite = "sqlite" in str(type(conn)).lower()
            if is_sqlite:
                await conn.execute(
                    "INSERT OR IGNORE INTO ef_chat_sessions (session_id, user_id) VALUES (?, ?)",
                    (session_id, user_id)
                )
                await conn.commit()
            else:
                await conn.execute(
                    "INSERT INTO ef_chat_sessions (session_id, user_id) VALUES ($1, $2) ON CONFLICT (session_id) DO NOTHING",
                    session_id, user_id
                )

    def append_turn(self, *, user_id: str, session_id: str, role: str, speaker: str, content: str, timestamp: Optional[str] = None, metadata: Optional[Dict] = None) -> int:
        """同步包装异步调用（也可改为原生异步，这里为保持接口一致采用 run_until_complete 的思想或后台任务）
        注意：为了证据链，我们需要返回生成的自增 ID。
        """
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            # async 场景下无法 run_until_complete，优先用 sqlite 同步落盘
            if loop and loop.is_running():
                return self._append_turn_sqlite_sync(
                    user_id=user_id, session_id=session_id, role=role, speaker=speaker, content=content, timestamp=timestamp, metadata=metadata
                )
        except RuntimeError:
            pass

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(
            self.async_append_turn(
                user_id=user_id, session_id=session_id, role=role, speaker=speaker, content=content, timestamp=timestamp, metadata=metadata
            )
        )

    async def async_append_turn(self, *, user_id: str, session_id: str, role: str, speaker: str, content: str, timestamp: Optional[str] = None, metadata: Optional[Dict] = None) -> int:
        await self._ensure_session(user_id, session_id)
        
        async with self.get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            import json
            meta_json = json.dumps(metadata) if metadata else None
            
            if is_sqlite:
                if timestamp:
                    cursor = await conn.execute(
                        "INSERT INTO ef_chat_messages (session_id, role, speaker, content, timestamp, metadata) VALUES (?, ?, ?, ?, ?, ?)",
                        (session_id, role, speaker, content, timestamp, meta_json)
                    )
                else:
                    cursor = await conn.execute(
                        "INSERT INTO ef_chat_messages (session_id, role, speaker, content, metadata) VALUES (?, ?, ?, ?, ?)",
                        (session_id, role, speaker, content, meta_json)
                    )
                await conn.commit()
                return cursor.lastrowid
            else:
                # asyncpg 使用 $1, $2 占位符，且可以使用 RETURNING id
                if timestamp:
                    row = await conn.fetchrow(
                        "INSERT INTO ef_chat_messages (session_id, role, speaker, content, timestamp, metadata) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                        session_id, role, speaker, content, timestamp, meta_json
                    )
                else:
                    row = await conn.fetchrow(
                        "INSERT INTO ef_chat_messages (session_id, role, speaker, content, metadata) VALUES ($1, $2, $3, $4, $5) RETURNING id",
                        session_id, role, speaker, content, meta_json
                    )
                return row['id']

    def get_recent(self, *, user_id: str, session_id: str, limit: int = 20) -> List[Dict]:
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果已在 loop 中，这是一个设计缺陷的同步调用。
                # 在测试或特殊环境下，尝试包装一个简单容器，但生产环境应优先使用 async_get_recent
                return [] 
            return loop.run_until_complete(self.async_get_recent(user_id=user_id, session_id=session_id, limit=limit))
        except Exception as exc:
            logger.warning("get_recent fallback to [] due to sync bridge error: %s", exc)
            return []

    async def async_get_recent(self, *, user_id: str, session_id: str, limit: int = 20) -> List[Dict]:
        async with self.get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            sql = "SELECT id, role, speaker as name, content, timestamp FROM ef_chat_messages WHERE session_id = ? ORDER BY id DESC LIMIT ?" if is_sqlite else \
                  "SELECT id, role, speaker as name, content, timestamp FROM ef_chat_messages WHERE session_id = $1 ORDER BY id DESC LIMIT $2"

            if is_sqlite:
                cursor = await conn.execute(sql, (session_id, limit))
                rows = await cursor.fetchall()
            else:
                rows = await conn.fetch(sql, session_id, limit)
            
            results = []
            for r in rows:
                results.append(dict(r))
            return results[::-1] # 恢复时间正序

    def history_window(self, *, user_id: str, session_id: str, offset: int = 0, limit: int = 50) -> List[Dict]:
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running(): return []
            return loop.run_until_complete(self.async_history_window(user_id=user_id, session_id=session_id, offset=offset, limit=limit))
        except Exception as exc:
            logger.warning("history_window fallback to [] due to sync bridge error: %s", exc)
            return []

    async def async_history_window(self, *, user_id: str, session_id: str, offset: int = 0, limit: int = 50) -> List[Dict]:
        async with self.get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            sql = "SELECT id, role, speaker as name, content, timestamp FROM ef_chat_messages WHERE session_id = ? ORDER BY id ASC LIMIT ? OFFSET ?" if is_sqlite else \
                  "SELECT id, role, speaker as name, content, timestamp FROM ef_chat_messages WHERE session_id = $1 ORDER BY id ASC LIMIT $2 OFFSET $3"

            if is_sqlite:
                cursor = await conn.execute(sql, (session_id, limit, offset))
                rows = await cursor.fetchall()
            else:
                rows = await conn.fetch(sql, session_id, limit, offset)
            return [dict(r) for r in rows]

    def get_message_by_id(self, msg_id: int) -> Optional[Dict]:
        import asyncio
        return asyncio.run(self.async_get_message_by_id(msg_id))

    async def async_get_message_by_id(self, msg_id: int) -> Optional[Dict]:
        async with self.get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            sql = "SELECT id, role, speaker as name, content, timestamp, session_id FROM ef_chat_messages WHERE id = ?" if is_sqlite else \
                  "SELECT id, role, speaker as name, content, timestamp, session_id FROM ef_chat_messages WHERE id = $1"
            
            row = await conn.fetchrow(sql, msg_id) if not is_sqlite else None
            if is_sqlite:
                cursor = await conn.execute(sql, (msg_id,))
                r = await cursor.fetchone()
                return dict(r) if r else None
            return dict(row) if row else None

    async def async_get_message_exchange(self, msg_id: int) -> List[Dict]:
        """[M1] 获取证据链的一轮完整对话 (命中消息 + 下一轮回复)"""
        async with self.get_db() as conn:
            is_sqlite = "sqlite" in str(type(conn)).lower()
            # 找到当前消息及其紧随其后的下一条消息 (通常是 AI 回复)
            sql = """
                SELECT id, role, speaker as name, content, timestamp 
                FROM ef_chat_messages 
                WHERE session_id = (SELECT session_id FROM ef_chat_messages WHERE id = ?)
                  AND id >= ? 
                ORDER BY id ASC LIMIT 2
            """ if is_sqlite else """
                SELECT id, role, speaker as name, content, timestamp 
                FROM ef_chat_messages 
                WHERE session_id = (SELECT session_id FROM ef_chat_messages WHERE id = $1)
                  AND id >= $2 
                ORDER BY id ASC LIMIT 2
            """
            if is_sqlite:
                cursor = await conn.execute(sql, (msg_id, msg_id))
                rows = await cursor.fetchall()
            else:
                rows = await conn.fetch(sql, msg_id, msg_id)
            return [dict(r) for r in rows]
