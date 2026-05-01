import asyncio
import os
import sys

from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from config import neo4j_config


def _friendly_neo4j_error(exc: Exception) -> str:
    text = str(exc)
    if isinstance(exc, AuthError):
        return (
            "Neo4j 用户名或密码不正确。请检查 .env 里的 "
            "NEO4J_USERNAME / NEO4J_PASSWORD。"
        )
    if isinstance(exc, ServiceUnavailable) or "Couldn't connect" in text or "Connect call failed" in text:
        return (
            f"没有连接到 Neo4j（{neo4j_config.uri}）。请先启动 Neo4j，"
            "并确认 Bolt 端口 7687 可以访问。"
            "如果你使用了其它地址或端口，请修改 .env 里的 NEO4J_URI。"
        )
    return f"Neo4j 初始化失败：{text}"


async def initialize_neo4j():
    print(f"[Neo4j Setup] Connecting to {neo4j_config.uri}...")
    driver = AsyncGraphDatabase.driver(
        neo4j_config.uri,
        auth=(neo4j_config.username, neo4j_config.password),
    )

    try:
        await driver.verify_connectivity()
        async with driver.session(database=neo4j_config.database) as session:
            print("[Neo4j Setup] Creating constraints and indexes...")

            setup_queries = [
                "DROP CONSTRAINT entity_id_unique IF EXISTS",
                "CREATE CONSTRAINT owner_entity_unique IF NOT EXISTS FOR (e:Entity) REQUIRE (e.owner_id, e.entity_id) IS UNIQUE",
                "CREATE INDEX entity_name_idx IF NOT EXISTS FOR (e:Entity) ON (e.name)",
                "CREATE INDEX entity_owner_idx IF NOT EXISTS FOR (e:Entity) ON (e.owner_id)",
                "CREATE CONSTRAINT event_uuid_unique IF NOT EXISTS FOR (e:Event) REQUIRE e.uuid IS UNIQUE",
                "CREATE INDEX event_owner_idx IF NOT EXISTS FOR (e:Event) ON (e.owner_id)",
                "CREATE INDEX event_status_idx IF NOT EXISTS FOR (e:Event) ON (e.status)",
                "CREATE CONSTRAINT episode_uuid_unique IF NOT EXISTS FOR (e:Episode) REQUIRE e.episode_id IS UNIQUE",
                "CREATE CONSTRAINT saga_uuid_unique IF NOT EXISTS FOR (s:Saga) REQUIRE s.saga_id IS UNIQUE",
                "MERGE (n:Episode {episode_id: '_schema_sentinel', owner_id: '_root'}) SET n.status = 'schema'",
                "MERGE (n:Saga {saga_id: '_schema_sentinel', owner_id: '_root'}) SET n.status = 'schema'",
            ]

            for query in setup_queries:
                try:
                    await session.run(query)
                    print(f"  [OK] {query.split('IF NOT EXISTS')[0].replace('CREATE ', '')}...")
                except Exception as exc:
                    print(f"  [ERROR] Neo4j schema query failed: {query}")
                    print(f"          {_friendly_neo4j_error(exc)}")
                    return False
    except Exception as exc:
        print(f"[Neo4j Setup] {_friendly_neo4j_error(exc)}")
        print("[Neo4j Setup] 已跳过图数据库初始化。")
        print("[Neo4j Setup] 如需启用图记忆，请启动 Neo4j 后重新运行服务。")
        return False
    finally:
        await driver.close()

    print("[Neo4j Setup] Initialization complete.")
    return True


if __name__ == "__main__":
    asyncio.run(initialize_neo4j())
