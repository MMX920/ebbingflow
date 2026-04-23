import asyncio
import logging
from neo4j import AsyncGraphDatabase
import sys
import os

# 允许导入项目根目录模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import neo4j_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CognitiveJanitor:
    def __init__(self):
        self.driver = AsyncGraphDatabase.driver(
            neo4j_config.uri,
            auth=(neo4j_config.username, neo4j_config.password)
        )
        self.database = neo4j_config.database

    async def close(self):
        await self.driver.close()

    async def run_cleanup(self):
        logger.info("🚀 [CognitiveJanitor] Starting graph deep cleaning...")
        
        async with self.driver.session(database=self.database) as session:
            # 1. 删除 SAME_AS 自环 (精确计数版)
            logger.info("🔨 Step 1: Cleaning SAME_AS self-loops...")
            loop_res = await session.run("""
                MATCH (a:Entity)-[r:RELATION]->(b:Entity)
                WHERE (r.type = 'SAME_AS' OR r.type = 'IDENTITY') AND id(a) = id(b)
                WITH collect(r) AS loops, count(r) AS to_delete
                FOREACH (x IN loops | DELETE x)
                RETURN to_delete AS count
            """)
            record = await loop_res.single()
            logger.info(f"✅ Removed {record['count']} self-loop relations.")

            # 2. 合并同秒内的重复改名事件 (P0: 类型鲁棒版，支持 String/Datetime 混杂对齐)
            logger.info("🔨 Step 2: Merging duplicate STATE_CHANGE events (Type-Robust Window)...")
            merge_res = await session.run("""
                MATCH (e:Event {action_type: 'STATE_CHANGE'})
                WHERE e.created_at IS NOT NULL
                WITH e,
                     CASE 
                        WHEN apoc.meta.type(e.created_at) = 'STRING' THEN substring(toString(e.created_at), 0, 19)
                        ELSE substring(toString(datetime(e.created_at)), 0, 19)
                     END AS sec_bucket
                WITH e.owner_id AS owner, e.subject AS sub, e.predicate AS pred, e.object AS obj,
                     sec_bucket, count(e) AS cnt, collect(e) AS evts
                WHERE cnt > 1
                WITH evts
                UNWIND evts[1..] AS redundant
                WITH collect(redundant) AS redundants, count(redundant) AS to_delete
                FOREACH (r IN redundants | DETACH DELETE r)
                RETURN to_delete AS count
            """)
            record = await merge_res.single()
            logger.info(f"✅ Merged {record['count']} redundant state events (Time bucket aligned).")

            # 3. 清理孤立的 CandidateEvent (可选项)
            logger.info("🔨 Step 3: Pruning orphaned candidate nodes...")
            prune_res = await session.run("""
                MATCH (c:CandidateEvent)
                WHERE NOT (c)<-[:CANDIDATE_ACTOR_IN]-()
                DELETE c
                RETURN count(c) as count
            """)
            prune_count = await prune_res.single()
            logger.info(f"✅ Pruned {prune_count['count']} orphaned candidates.")

        logger.info("✨ [CognitiveJanitor] Graph cleanup completed successfully.")

async def main():
    janitor = CognitiveJanitor()
    try:
        await janitor.run_cleanup()
    finally:
        await janitor.close()

if __name__ == "__main__":
    asyncio.run(main())
