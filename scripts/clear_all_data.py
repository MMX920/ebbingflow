"""
全量数据清空工具 (Global Data Purge Tool - Interactive v3.0)
"""
import sys
import os
import shutil
import asyncio

# 确保引入当前项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import neo4j_config
from neo4j import AsyncGraphDatabase

async def clear_neo4j():
    print("\n[1/2] 正在连接 Neo4j 数据库并执行原子级清空...")
    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri, 
        auth=(neo4j_config.username, neo4j_config.password)
    )
    try:
        async with driver.session(database=neo4j_config.database) as session:
            await session.run("MATCH (n) DETACH DELETE n")
            print(" [OK] Neo4j 图谱已全部重置。")
    except Exception as e:
        print(f" [ERROR] Neo4j 清除失败: {e}")
    finally:
        await driver.close()

def clear_local_storage():
    print("[2/2] 正在清理本地持久化缓存 (.data/ 目录)...")
    data_dir = ".data"
    if os.path.exists(data_dir):
        try:
            shutil.rmtree(data_dir)
            print(" [OK] 本地 Session 与向量数据已物理抹除。")
        except Exception as e:
            print(f" [ERROR] 本地文件删除失败: {e}")
    else:
        print(" [OK] 本地没有需要清理的数据缓存。")

async def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*60)
    print("      MemGraph AI - 全量记忆清空控制台 (稳健版)")
    print("="*60)
    print("\n [警告]：此操作将执行以下自毁指令：")
    print(" 1. 彻底清空 Neo4j 数据库中的所有图谱节点与关系。")
    print(" 2. 物理抹除本地 .data/ 文件夹下的所有聊天记录与向量碎片。")
    print("\n !!! 注意：此操作不可恢复，AI 助手将彻底失忆 !!!\n")
    
    # In automated environment, we assume 'y' if piped
    confirm = 'y'
    if sys.stdin.isatty():
        confirm = input(" 你确定要抹除所有记忆并重置 AI 吗？(确认输入 y, 取消直接回车): ").strip().lower()
    
    if confirm != 'y':
        print("\n [操作取消] 记忆已保留。再见！\n")
        return
    
    await clear_neo4j()
    clear_local_storage()
    
    print("\n" + "="*56)
    print(" 任务完成：AI 已经变成了一张白纸。")
    print("="*56 + "\n")

if __name__ == "__main__":
    asyncio.run(main())
