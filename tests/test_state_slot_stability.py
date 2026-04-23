import os
import sys
import unittest
import asyncio
import uuid
import time
from datetime import datetime
from neo4j import AsyncGraphDatabase

# 项目根目录
sys.path.append(os.getcwd())

from config import neo4j_config, identity_config
from memory.graph.writer import AsyncGraphWriter
from memory.event.slots import MemoryEvent, ActionType

class TestStateSlotStability(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.writer = AsyncGraphWriter()
        self.owner_id = f"test_user_{uuid.uuid4().hex[:8]}"
        self.session_id = f"sess_{uuid.uuid4().hex[:8]}"
        self.driver = AsyncGraphDatabase.driver(
            neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password)
        )
        async with self.driver.session(database=neo4j_config.database) as session:
            await session.run("DROP CONSTRAINT entity_id_unique IF EXISTS")
            await session.run(
                "CREATE CONSTRAINT owner_entity_unique IF NOT EXISTS FOR (e:Entity) REQUIRE (e.owner_id, e.entity_id) IS UNIQUE"
            )
            await session.run(
                "MERGE (e:Entity {entity_id: $uid, owner_id: $oid}) SET e.name = 'TestHost'",
                uid=identity_config.user_id, oid=self.owner_id
            )

    async def asyncTearDown(self):
        async with self.driver.session(database=neo4j_config.database) as session:
            await session.run("MATCH (n {owner_id: $oid}) DETACH DELETE n", oid=self.owner_id)
        await self.writer.close()
        await self.driver.close()

    async def _get_active_count(self, slot):
        async with self.driver.session(database=neo4j_config.database) as session:
            res = await session.run(
                "MATCH (e:Entity {entity_id: $uid, owner_id: $oid})-[:ACTOR_IN]->(evt:Event {status: 'active', temporal_slot: $slot, owner_id: $oid}) "
                "RETURN count(evt) as cnt",
                uid=identity_config.user_id, oid=self.owner_id, slot=slot
            )
            record = await res.single()
            return record["cnt"]

    async def _get_invalid_count(self, slot):
        async with self.driver.session(database=neo4j_config.database) as session:
            res = await session.run(
                "MATCH (e:Entity {entity_id: $uid, owner_id: $oid})-[:ACTOR_IN]->(evt:Event {status: 'invalidated', temporal_slot: $slot, owner_id: $oid}) "
                "RETURN count(evt) as cnt",
                uid=identity_config.user_id, oid=self.owner_id, slot=slot
            )
            record = await res.single()
            return record["cnt"]

    async def test_slot_stability_sequential(self):
        # 核心校验：20 次状态演化
        slots = ["role", "state", "name"]
        for i in range(20):
            slot = slots[i % 3]
            val = f"Value_{i}_{uuid.uuid4().hex[:4]}"
            # 明确关键词以通过 _infer_temporal_slot
            if slot == "role": pred = "has a new role: teacher"
            elif slot == "state": pred = "enters state: happy"
            else: pred = "is now named: Bob"
            
            event = MemoryEvent(
                subject=identity_config.user_id,
                predicate=pred,
                object=val,
                action_type=ActionType.STATE_CHANGE,
                timestamp_reference="SNAPSHOT"
            )
            
            await self.writer.write_events([event], [], self.session_id, self.owner_id)
            await asyncio.sleep(0.1) 

        # 最终验证
        role_active = await self._get_active_count("role")
        state_active = await self._get_active_count("state")
        name_active = await self._get_active_count("name")
        
        print(f"Active Slots - role: {role_active}, state: {state_active}, name: {name_active}")
        
        role_invalid = await self._get_invalid_count("role")
        state_invalid = await self._get_invalid_count("state")
        name_invalid = await self._get_invalid_count("name")
        print(f"Invalidated Slots - role: {role_invalid}, state: {state_invalid}, name: {name_invalid}")

        self.assertEqual(role_active, 1, "Role should have exactly 1 active record")
        self.assertEqual(state_active, 1, "State should have exactly 1 active record")
        self.assertEqual(name_active, 1, "Name should have exactly 1 active record")
        
        # 总量校验
        self.assertEqual(role_active + state_active + name_active + role_invalid + state_invalid + name_invalid, 20)

if __name__ == "__main__":
    unittest.main()
