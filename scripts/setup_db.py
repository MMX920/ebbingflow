"""
EbbingFlow 数据库初始化脚本
用于创建 ef_chat 系列表格 (支持 PostgreSQL 和 SQLite 自动切换)
"""
import asyncio
import os
import sys

# 增加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory.sql.pool import get_db, close_pool
from config import postgres_config

async def setup_db():
    print("[DB Setup] Checking DB connection...")
    
    schema_path = os.path.join("memory", "sql", "schema", "ef_history.sql")
    if not os.path.exists(schema_path):
        print(f"[DB Setup] Schema file not found: {schema_path}")
        return

    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()

    try:
        async with get_db() as conn:
            # 增强判定：只要驱动名包含 sqlite（不区分大小写）即判定为 SQLite
            conn_type_str = str(type(conn)).lower()
            is_sqlite = "sqlite" in conn_type_str
            
            if is_sqlite:
                print(f"[DB Setup] Target: SQLite ({conn_type_str}). Adjusting Dialect...")
                # 简单方言转换
                sql = sql.replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
                sql = sql.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
                sql = sql.replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMP")
                sql = sql.replace("JSONB", "TEXT") 
            else:
                print(f"[DB Setup] Target: PostgreSQL ({postgres_config.db})")

            print("[DB Setup] Executing schema...")
            # 统一使用支持多语句执行的方法
            if hasattr(conn, "executescript"):
                await conn.executescript(sql)
            elif is_sqlite and hasattr(conn, "execute"):
                # 对于兼容层，如果不支持 executescript，则尝试拆分符号执行（简化版）
                for statement in sql.split(";"):
                    if statement.strip():
                        await conn.execute(statement + ";")
            else:
                await conn.execute(sql)
                
            print("[DB Setup] Core tables initialized successfully.")
    except Exception as e:
        print(f"[DB Setup] Error during schema execution: {e}")
    finally:
        await close_pool()

if __name__ == "__main__":
    asyncio.run(setup_db())
