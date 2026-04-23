"""
全量数据清空工具 (Global Data Purge Tool - Interactive v3.0)
集成了安全确认逻辑，解决了 Windows 批处理编码导致的乱码问题。
"""
import sys
import os
import shutil
import asyncio

# 确保引入当前项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import neo4j_config
from neo4j import AsyncGraphDatabase

# ANSI 颜色支持
C_RED = "\033[31m"
C_GREEN = "\033[32m"
C_YELLOW = "\033[33m"
C_RESET = "\033[0m"

async def clear_neo4j():
    print(f"\n{C_YELLOW}[1/2] 正在连接 Neo4j 数据库并执行原子级清空...{C_RESET}")
    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri, 
        auth=(neo4j_config.username, neo4j_config.password)
    )
    try:
        async with driver.session(database=neo4j_config.database) as session:
            await session.run("MATCH (n) DETACH DELETE n")
            print(f"{C_GREEN} ✓ Neo4j 图谱已全部重置。{C_RESET}")
    except Exception as e:
        print(f"{C_RED} ✗ Neo4j 清除失败: {e}{C_RESET}")
    finally:
        await driver.close()

def clear_local_storage():
    print(f"{C_YELLOW}[2/2] 正在清理本地持久化缓存 (.data/ 目录)...{C_RESET}")
    data_dir = ".data"
    if os.path.exists(data_dir):
        try:
            shutil.rmtree(data_dir)
            print(f"{C_GREEN} ✓ 本地 Session 与向量数据已物理抹除。{C_RESET}")
        except Exception as e:
            print(f"{C_RED} ✗ 本地文件删除失败: {e}{C_RESET}")
    else:
        print(f"{C_GREEN} - 本地没有需要清理的数据缓存。{C_RESET}")

async def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"{C_RED}")
    print("="*60)
    print("      🧠 MemGraph AI — 全量记忆清空控制台 (稳健版)")
    print("="*60)
    print(f"{C_RESET}")
    print(f" [⚠️ 警告]：此操作将执行以下自毁指令：")
    print(f" 1. 彻底清空 Neo4j 数据库中的所有图谱节点与关系。")
    print(f" 2. 物理抹除本地 .data/ 文件夹下的所有聊天记录与向量碎片。")
    print(f"\n{C_RED} !!! 注意：此操作不可恢复，AI 助手将彻底失忆 !!!{C_RESET}\n")
    
    confirm = input(" 你确定要抹除所有记忆并重置 AI 吗？(确认输入 y, 取消直接回车): ").strip().lower()
    
    if confirm != 'y':
        print(f"\n{C_GREEN} [操作取消] 记忆已保留。再见！{C_RESET}\n")
        return
    
    await clear_neo4j()
    clear_local_storage()
    
    print(f"\n{C_GREEN}========================================================")
    print(" ✨ 任务完成：AI 已经变成了一张白纸。")
    print(f"========================================================{C_RESET}\n")
    input(" 按任意键退出...")

if __name__ == "__main__":
    asyncio.run(main())
