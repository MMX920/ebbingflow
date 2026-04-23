import os
import sys
import unittest
import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

# 项目根目录
sys.path.append(os.getcwd())

from memory.integration.crm_sync import CRMChange, CRMUpsertRequest, CRMReplayRequest
from api.server import crm_upsert, crm_replay, ack_cdc_version

class TestCRME2EMock(unittest.IsolatedAsyncioTestCase):
    async def test_crm_protocol_e2e(self):
        report_details = []
        
        # 1. Mock 依赖
        with patch("api.server.global_db_driver") as mock_db, \
             patch("api.server.checkpoint_manager") as mock_chk, \
             patch("api.server.identity_config") as mock_cfg:
            
            mock_cfg.enable_crm_sync = True
            mock_cfg.crm_source_weight = 0.65
            
            # 2. 测试 /crm/upsert (仲裁)
            mock_session = AsyncMock()
            mock_db.session.return_value.__aenter__.return_value = mock_session
            mock_res = AsyncMock()
            mock_res.single.return_value = None # 模拟无旧记录
            mock_session.run.return_value = mock_res
            
            req = CRMUpsertRequest(changes=[
                CRMChange(external_event_id="ext_001", owner_id="user_001", target_id="user_001", slot="name", value="Andrew CRM", timestamp="2026-01-01")
            ])
            resp = await crm_upsert(req)
            self.assertEqual(resp["status"], "success")
            report_details.append({"step": "upsert", "result": resp})

            # 3. 测试 /crm/events/replay (幂等)
            mock_chk.is_replayed.return_value = True # 模拟已重放
            replay_req = CRMReplayRequest(batch_id="b1", changes=[req.changes[0]])
            replay_resp = await crm_replay(replay_req)
            self.assertEqual(replay_resp["skipped"], 1)
            report_details.append({"step": "replay_idempotency", "result": replay_resp})

            # 4. 测试 /cdc/ack (位点单调)
            mock_chk.ack_checkpoint.return_value = 100
            ack_resp = await ack_cdc_version(consumer_id="c1", owner_id="user_001", version=100)
            self.assertEqual(ack_resp["current_version"], 100)
            report_details.append({"step": "cdc_ack", "result": ack_resp})

        # 生成报告
        report = {
            "status": "PASS",
            "components": ["upsert", "replay", "ack"],
            "details": report_details
        }
        os.makedirs(".data", exist_ok=True)
        with open(".data/crm_mock_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        
        print("CRM E2E Mock Report Generated.")

if __name__ == "__main__":
    unittest.main()
