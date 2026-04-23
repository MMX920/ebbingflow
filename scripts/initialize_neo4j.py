import asyncio
import os
import sys
from neo4j import AsyncGraphDatabase
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from config import neo4j_config

async def initialize_neo4j():
    print(f"[Neo4j Setup] Connecting to {neo4j_config.uri}...")
    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri, 
        auth=(neo4j_config.username, neo4j_config.password)
    )
    
    async with driver.session(database=neo4j_config.database) as session:
        print("[Neo4j Setup] Creating constraints and indexes...")
        
        setup_queries = [
            # 1. Entity Constraints
            "DROP CONSTRAINT entity_id_unique IF EXISTS",
            "CREATE CONSTRAINT owner_entity_unique IF NOT EXISTS FOR (e:Entity) REQUIRE (e.owner_id, e.entity_id) IS UNIQUE",
            "CREATE INDEX entity_name_idx IF NOT EXISTS FOR (e:Entity) ON (e.name)",
            "CREATE INDEX entity_owner_idx IF NOT EXISTS FOR (e:Entity) ON (e.owner_id)",
            
            # 2. Event Constraints
            "CREATE CONSTRAINT event_uuid_unique IF NOT EXISTS FOR (e:Event) REQUIRE e.uuid IS UNIQUE",
            "CREATE INDEX event_owner_idx IF NOT EXISTS FOR (e:Event) ON (e.owner_id)",
            "CREATE INDEX event_status_idx IF NOT EXISTS FOR (e:Event) ON (e.status)",
            
            # 3. Episode & Saga (M2.2/2.3)
            "CREATE CONSTRAINT episode_uuid_unique IF NOT EXISTS FOR (e:Episode) REQUIRE e.episode_id IS UNIQUE",
            "CREATE CONSTRAINT saga_uuid_unique IF NOT EXISTS FOR (s:Saga) REQUIRE s.saga_id IS UNIQUE",
            
            # 建立占位节点以消除 Label Warning (首次运行时即便没数据也不报 warn)
            "MERGE (n:Episode {episode_id: '_schema_sentinel', owner_id: '_root'}) SET n.status = 'schema'",
            "MERGE (n:Saga {saga_id: '_schema_sentinel', owner_id: '_root'}) SET n.status = 'schema'"
        ]
        
        for query in setup_queries:
            try:
                await session.run(query)
                print(f"  [OK] {query.split('IF NOT EXISTS')[0].replace('CREATE ', '')}...")
            except Exception as e:
                print(f"  [ERROR] Failed to run query: {query}. Error: {e}")

    await driver.close()
    print("[Neo4j Setup] Initialization complete.")

if __name__ == "__main__":
    asyncio.run(initialize_neo4j())
