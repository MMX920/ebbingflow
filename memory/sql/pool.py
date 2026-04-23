"""
PostgreSQL 连接池管理 (asyncpg)
- ebbingflow 对业务数据库只读访问
- 懒初始化，首次使用时创建连接池
"""
import logging
import contextlib
import os
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)

from config import postgres_config, sqlite_config

_pool = None
_pool_init_attempted = False

async def get_pool():
    """获取全局 PostgreSQL 连接池（懒初始化）。如未配置 则返回 None。"""
    global _pool, _pool_init_attempted
    if _pool is not None:
        return _pool

    if not postgres_config.is_configured():
        return None

    if _pool_init_attempted:
        return None

    try:
        import asyncpg
        _pool = await asyncpg.create_pool(
            dsn=postgres_config.connection_string(),
            min_size=1,
            max_size=5,
            command_timeout=10,
        )
        logger.info("[SQL Pool] PostgreSQL connection pool created (%s/%s)",
                    postgres_config.host, postgres_config.db)
    except Exception as exc:
        logger.warning("[SQL Pool] Failed to create PostgreSQL pool: %s", exc)
        _pool = None
    finally:
        _pool_init_attempted = True

    return _pool


async def close_pool():
    """关闭连接池（服务停止时调用）。"""
    global _pool, _pool_init_attempted
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("[SQL Pool] PostgreSQL connection pool closed")
    _pool_init_attempted = False


class AsyncSQLiteCompatCursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    async def fetchone(self):
        return self._cursor.fetchone()

    async def fetchall(self):
        return self._cursor.fetchall()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class AsyncSQLiteCompatConnection:
    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row

    async def execute(self, sql: str, params=()):
        cur = self._conn.execute(sql, params or ())
        return AsyncSQLiteCompatCursor(cur)

    async def commit(self):
        self._conn.commit()

    async def close(self):
        self._conn.close()


@contextlib.asynccontextmanager
async def get_db():
    """
    统一异步数据库访问接口 (Context Manager)
    优先使用 PostgreSQL 连结池，若不可用或未配置则使用 SQLite
    """
    pool = await get_pool()
    if pool:
        async with pool.acquire() as conn:
            yield conn
    else:
        db_path = sqlite_config.db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # Fallback to SQLite: 优先 aiosqlite，缺失时使用 sqlite3 兼容包装
        try:
            import aiosqlite
            async with aiosqlite.connect(db_path) as conn:
                conn.row_factory = aiosqlite.Row
                yield conn
        except ModuleNotFoundError:
            conn = AsyncSQLiteCompatConnection(db_path)
            try:
                yield conn
            finally:
                await conn.close()
