import os
import sys
import unittest
import json
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

# 项目根目录
sys.path.append(os.getcwd())

from memory.integration.cdc_checkpoint import CDCCheckpointManager
from memory.integration.crm_sync import make_idempotency_key, build_conflict_candidates, CRMChange, CRMUpsertRequest

class TestCRMSyncProtocol(unittest.TestCase):
    def setUp(self):
        # 隔离测试数据库
        if os.path.exists(".data/cdc_checkpoint_test.db"):
            os.remove(".data/cdc_checkpoint_test.db")
        
        with patch("memory.integration.cdc_checkpoint.DB_PATH", ".data/cdc_checkpoint_test.db"):
            self.manager = CDCCheckpointManager()

    def tearDown(self):
        self.manager.close()
        if os.path.exists(".data/cdc_checkpoint_test.db"):
             os.remove(".data/cdc_checkpoint_test.db")

    def test_cdc_ack_only_forward(self):
        # Case A: /cdc/ack 只能前进
        cid, oid = "cons_01", "user_001"
        self.manager.ack_checkpoint(cid, oid, 10)
        self.assertEqual(self.manager.get_checkpoint(cid, oid), 10)
        
        # 尝试后退，位点保持不变
        self.manager.ack_checkpoint(cid, oid, 5)
        self.assertEqual(self.manager.get_checkpoint(cid, oid), 10)
        
        # 前进
        self.manager.ack_checkpoint(cid, oid, 15)
        self.assertEqual(self.manager.get_checkpoint(cid, oid), 15)

    def test_replay_idempotency(self):
        # Case D: replay 重放同一 key 只标记一次
        key = make_idempotency_key("user_001", "evt_999", "role", "Manager")
        self.assertFalse(self.manager.is_replayed(key))
        
        self.manager.mark_replayed(key)
        self.assertTrue(self.manager.is_replayed(key))
        
        # 再次标记不报错
        self.manager.mark_replayed(key)
        self.assertTrue(self.manager.is_replayed(key))

    def test_build_conflict_candidates_no_exception(self):
        # 新增 Case: 验证修复后的字段名
        change = CRMChange(
            external_event_id="ext_01", owner_id="user_001", target_id="user_001",
            slot="name", value="Andrew", timestamp="2026-04-13T10:00:00Z"
        )
        cands = build_conflict_candidates(change, 0.65)
        self.assertEqual(len(cands), 1)
        self.assertEqual(cands[0].source, "crm")
        self.assertEqual(cands[0].record_time, "2026-04-13T10:00:00Z")

    @patch("api.server.global_db_driver")
    def test_crm_upsert_weight_integration(self, mock_db):
        from memory.identity.conflict_resolver import ConflictResolver, ConflictCandidate
        
        cands = [
            ConflictCandidate(value="Master", source="user", confidence=1.0, record_time="2026-04-10"),
            ConflictCandidate(value="Client", source="crm", confidence=1.0, record_time="2026-04-11")
        ]
        res = ConflictResolver.resolve_conflict("name", cands)
        self.assertEqual(res.winner, "Master")
        self.assertIn("Source: user", res.winner_reason)

    @patch("api.server.global_db_driver")
    @patch("api.server.identity_config")
    def test_crm_upsert_whitelist_and_audit(self, mock_cfg, mock_db):
        from api.server import crm_upsert
        
        async def run_test():
            mock_cfg.enable_crm_sync = True
            mock_cfg.crm_source_weight = 0.65
            
            # 1. Test invalid slot
            req = CRMUpsertRequest(changes=[
                CRMChange(external_event_id="e1", owner_id="u1", target_id="t1", slot="hack_property", value="v", timestamp="2026-01-01")
            ])
            resp = await crm_upsert(req)
            self.assertEqual(resp["results"][0]["status"], "ignored")
            self.assertEqual(resp["results"][0]["reason"], "invalid_slot")
            
            # 2. Test valid slot and applied (mock empty curr value)
            mock_session = AsyncMock()
            mock_db.session.return_value.__aenter__.return_value = mock_session
            
            mock_curr_res = AsyncMock()
            mock_curr_res.single.return_value = None
            mock_session.run.return_value = mock_curr_res
            
            req_ok = CRMUpsertRequest(changes=[
                CRMChange(external_event_id="e2", owner_id="u1", target_id="t1", slot="name", value="NewName", timestamp="2026-01-01")
            ])
            resp_ok = await crm_upsert(req_ok)
            self.assertEqual(resp_ok["status"], "success")
            self.assertEqual(resp_ok["results"][0]["status"], "applied")

        asyncio.run(run_test())

if __name__ == "__main__":
    unittest.main()
