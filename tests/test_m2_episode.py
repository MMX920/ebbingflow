import asyncio
import logging
import sys
from config import neo4j_config
from core.session import ChatSession
from core.chat_engine import get_standard_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("M21_Episode_Test")

async def test_m2_episode():
    logger.info("🎬 Starting M2.1 Episode Pipeline Verification...")
    
    # Check Neo4j
    import neo4j
    driver = neo4j.GraphDatabase.driver(neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password))
    driver.verify_connectivity()
    driver.close()

    session = ChatSession(session_id="test_episode_session", user_id="test_user")
    engine = get_standard_engine()
    
    # We need 5 turns to trigger an episode (GraphWriterMiddleware checks un_episoded_turns >= 5)
    conversations = [
        "你好，我叫王深思，我是深思实验室的创始人。",
        "我在北京工作，实验室最近在研发新一代图谱。这周很忙。",
        "昨天我买了一台新的测试服务器用于部署模型。",
        "我觉得大模型加知识图谱是不可阻挡的趋势。",
        "好了，就聊到这，请帮我总结一下刚刚的聊天！"
    ]
    
    for i, user_input in enumerate(conversations):
        logger.info(f"🔄 Turn {i+1}/5 - User: {user_input}")
        response_chunks = []
        async for chunk in engine.chat_stream(user_input, session):
            pass  # We just consume the stream to let middleware run
        # Add slight delay for graph background jobs to sequence correctly
        await asyncio.sleep(2)
        
    logger.info("⏳ Waiting for EpisodeManager background extraction (15s)...")
    await asyncio.sleep(15)

    # Validate Episode Node in Neo4j
    async with neo4j.AsyncGraphDatabase.driver(neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password)) as d:
        async with d.session() as s:
            result = await s.run("MATCH (ep:Episode {session_id: 'test_episode_session'}) RETURN ep.name as n, ep.summary as s, ep.evidence_msg_ids as m, ep.episode_id as eid")
            rows = await result.data()
            if rows:
                row = rows[0]
                logger.info("Found Episode Node in Neo4j")
                logger.info(f"ID: {row['eid']}")
                logger.info(f"Name: {row['n']}")
                logger.info(f"Summary: {row['s']}")
                logger.info(f"Evidence Msg IDs: {row['m']}")
                
                # Check CONTAINS_EVENT links
                res_events = await s.run("MATCH (ep:Episode {session_id: 'test_episode_session'})-[:CONTAINS_EVENT]->(ev:Event) RETURN count(ev) as c")
                c_rows = await res_events.data()
                count = c_rows[0]['c']
                logger.info(f"📌 Connected Events: {count}")
                
            else:
                logger.error("❌ Failed. No Episode node found in graph.")
                sys.exit(1)

    await engine.middleware_chain.close()
    logger.info("✅ M2.1 Episode Pipeline Verification: PASS")
    sys.exit(0)

if __name__ == "__main__":
    try:
         asyncio.run(test_m2_episode())
    except Exception as e:
         logger.exception(e)
