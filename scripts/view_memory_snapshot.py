"""
记忆全维度快照提取器 (Memory Snapshot Viewer)
整合扫描：本地 Session JSON (短时) + Neo4j Graph (长时图谱)
"""
import sys
import os
import json
import asyncio
import warnings
import logging
from typing import List
from datetime import datetime

# 彻底屏蔽 Neo4j 5.x 的 "属性不存在" 驱动警告刷屏
warnings.filterwarnings("ignore")
logging.getLogger("neo4j").setLevel(logging.ERROR)

# 强行绕过本地代理，防止 502 报错
import os
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# 确保能导入项目内模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import neo4j_config
from neo4j import AsyncGraphDatabase

# ANSI 颜色定义
C_BOLD = "\033[1m"
C_CYAN = "\033[36m"
C_GREEN = "\033[32m"
C_YELLOW = "\033[33m"
C_BLUE = "\033[34m"
C_MAGENTA = "\033[35m"
C_RED = "\033[31m"
C_RESET = "\033[0m"

async def get_long_term_memory():
    """从 Neo4j 提取结构化 5W1H 记忆"""
    events = []
    try:
        driver = AsyncGraphDatabase.driver(neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password))
        async with driver.session(database=neo4j_config.database) as session:
            # 查询所有 Event 和 CandidateEvent
            # 同时也查出与他们相连的 Entity
            query = """
            MATCH (s:Entity)-[:ACTOR_IN]->(e)
            WHERE 'Event' IN labels(e) OR 'CandidateEvent' IN labels(e)
            OPTIONAL MATCH (obj:Entity)-[:OBJECT_OF]->(e)
            OPTIONAL MATCH (src:Entity)-[r:SAID]->(e)
            RETURN s.name as sub, e.uuid as uid, labels(e) as labels,
                   e.predicate as pred, e.action_type as type, 
                   e.context as ctx, e.timestamp_reference as time,
                   e.impact_score as impact, e.emotion_label as emo,
                   e.status as status, e.created_at as created,
                   e.confidence as conf, src.name as source,
                   r.trust_score as trust, e.duration as dur,
                   e.metadata as meta,
                   obj.name as object
            ORDER BY e.created_at DESC
            """
            result = await session.run(query)
            events = await result.data()
        await driver.close()
    except Exception as e:
        print(f"{C_RED}[Neo4j 错误]{C_RESET} 无法连接图谱数据库: {e}")
    return events

def get_short_term_memory(user_id="user_shen"):
    """从本地 JSON 文件读取短时会话"""
    path = f".data/sessions/{user_id}.json"
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f).get("history", [])
        except (OSError, json.JSONDecodeError):
            return []
    return []

async def main():
    # 强制开启 Windows 控制台颜色支持
    os.system('')

    print(f"\n{C_BOLD}{C_MAGENTA}{'='*60}{C_RESET}")
    print(f"{C_BOLD}{C_MAGENTA} 🧠 EbbingFlow — 记忆全维度透视快照{C_RESET}")
    print(f"{C_BOLD}{C_MAGENTA}{'='*60}{C_RESET}")

    # --- 1. 短时记忆 (Session JSON) ---
    st_history = get_short_term_memory()
    print(f"\n{C_BOLD}{C_CYAN}[ ⚡ 短时会话记忆 (Short-Term: 最近对话) ]{C_RESET}")
    if st_history:
        for msg in st_history[-4:]:  # 只展示最近4条
            role_color = C_GREEN if msg['role'] == 'user' else C_BLUE
            role_name = "👤 你" if msg['role'] == 'user' else "🤖 Andrew"
            print(f"  {role_color}{role_name}{C_RESET}: {msg['content'][:100]}{'...' if len(msg['content'])>100 else ''}")
        print(f"  {C_YELLOW}... (还有 {len(st_history)-4} 条历史消息储存在会话文件中){C_RESET}")
    else:
        print("  (暂无本地会话存档)")

    # --- 2. 长时记忆 (Neo4j Graph Fact) ---
    print(f"\n{C_BOLD}{C_GREEN}[ 🕸️ 长时图谱事实 (Long-Term: 5W1H 结构化事实) ]{C_RESET}")
    lt_events = await get_long_term_memory()
    if lt_events:
        for i, e in enumerate(lt_events):
            # 强化空值校验，防止历史遗留数据导致崩溃
            labels = e.get('labels', [])
            tag = f"{C_RESET}[{C_YELLOW}待审批/低置信{C_RESET}]" if "CandidateEvent" in labels else ""
            status_dot = "🟢" if e.get('status') == 'active' else "🟡"
            
            # 处理时间格式
            time_obj = e.get('created')
            if time_obj and hasattr(time_obj, 'year'):
                time_str = f"{time_obj.year}-{time_obj.month:02d}-{time_obj.day:02d}"
            else:
                time_str = "未知日期"

            impact = e.get('impact', 0) if e.get('impact') is not None else 0
            sub = e.get('sub', '未知主体')
            pred = e.get('pred', '未知动作')
            obj = e.get('object', '')
            etype = str(e.get('type')) if e.get('type') is not None else '未知'
            ctx = str(e.get('ctx')) if e.get('ctx') is not None else '无'
            emo = str(e.get('emo')) if e.get('emo') is not None else '无'
            etime = str(e.get('time')) if e.get('time') is not None else '无时间标签'
            conf = float(e.get('conf')) if e.get('conf') is not None else 1.0
            source = str(e.get('source')) if e.get('source') is not None else 'unknown'
            trust = float(e.get('trust')) if e.get('trust') is not None else 0.8
            dur = str(e.get('dur')) if e.get('dur') is not None else 'n/a'
            meta_raw = e.get('meta')
            if isinstance(meta_raw, str):
                try:
                    meta_dict = json.loads(meta_raw)
                    meta = json.dumps(meta_dict, ensure_ascii=False)
                except (json.JSONDecodeError, TypeError):
                    meta = meta_raw
            else:
                meta = json.dumps(meta_raw, ensure_ascii=False) if meta_raw is not None else '{}'

            print(f"  {i+1}. {status_dot} {C_BOLD}{sub}{C_RESET} {C_YELLOW}{pred}{C_RESET} {obj} {tag}")
            print(f"     ├─ {C_BLUE}类型:{C_RESET} {etype:<12} {C_BLUE}时间:{C_RESET} {etime:<15} {C_BLUE}持续:{C_RESET} {dur}")
            print(f"     ├─ {C_BLUE}语境:{C_RESET} {ctx}")
            print(f"     ├─ {C_BLUE}重要度:{C_RESET} {impact}/10 {'⭐'*((int(impact)+1)//2):<10} {C_BLUE}情绪:{C_RESET} {emo}")
            print(f"     ├─ {C_BLUE}置信度:{C_RESET} {conf:.2f}    {C_BLUE}来源:{C_RESET} {source:<12} {C_BLUE}信誉:{C_RESET} {trust:.2f}")
            print(f"     ├─ {C_BLUE}元数据:{C_RESET} {meta}")
            print(f"     └─ {C_BLUE}系统记录于:{C_RESET} {time_str}")
            print("-" * 65)
    else:
        print("  (Neo4j 图谱中尚未提取任何深度记忆)")

    # --- 3. 语义向量记忆统计 (ChromaDB Vector Stats) ---
    print(f"\n{C_BOLD}{C_YELLOW}[ 🧭 语义向量分布 (Phase 6: Vector Knowledge Base) ]{C_RESET}")
    try:
        from memory.vector.storer import VectorStorer
        storer = VectorStorer()
        chat_count = storer.get_chat_count()
        doc_count = storer.get_doc_count()
        
        print(f"  💬 向量化聊天片段: {C_GREEN}{chat_count}{C_RESET} 条")
        print(f"  📄 向量化文档分块: {C_GREEN}{doc_count}{C_RESET} 条")
        
        if chat_count + doc_count > 0:
            print(f"  {C_YELLOW}→ 语义空间已激活，历史细节可以被精准召回。{C_RESET}")
        else:
            print(f"  {C_RESET}(向量库目前是空的，多聊几句或导入文档即可填充。)")
    except Exception as e:
        print(f"  {C_RED}[向量库警告]{C_RESET} 无法读取向量统计: {e}")

    print(f"\n{C_BOLD}{C_MAGENTA}{'='*60}{C_RESET}")
    print(f" 会话 ID: user_shen (路径: .data/sessions/) | 数据库: {neo4j_config.database}")
    print(f"{C_BOLD}{C_MAGENTA}{'='*60}{C_RESET}\n")

if __name__ == "__main__":
    asyncio.run(main())
