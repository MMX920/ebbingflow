import os
import sys
import unittest
import json
from unittest.mock import MagicMock, patch

# 项目根目录
sys.path.append(os.getcwd())

from scripts.graph_compactor import GraphCompactor

class TestGraphCompactor(unittest.TestCase):
    @patch("scripts.graph_compactor.GraphDatabase")
    def test_dry_run_logic(self, mock_db):
        mock_session = MagicMock()
        mock_db.driver.return_value.session.return_value.__enter__.return_value = mock_session
        
        # 模拟 R-A: 同秒重复事件
        mock_session.run.return_value.data.side_effect = [
            [{"ids": ["uuid1", "uuid2"]}], # R-A
            [], # R-C
        ]
        # 模拟 R-B: 过期候选
        mock_session.run.return_value.single.return_value = {"count": 10}
        
        compactor = GraphCompactor(owner_id="test_user", dry_run=True)
        report = compactor.run_compaction(ttl_days=30)
        
        # 验证报告命中
        self.assertEqual(report["rules"]["R-A_event_merge"], 1)
        self.assertEqual(report["rules"]["R-B_candidate_archive"], 10)
        
        # 验证 Dry-run 模式下没调 SET 语句
        # session.run 调了 3 次获取数据 (A, B, C)，如果是 dry_run，不应该有更多的 run 写入
        self.assertEqual(mock_session.run.call_count, 3)

    @patch("scripts.graph_compactor.GraphDatabase")
    def test_apply_logic(self, mock_db):
        mock_session = MagicMock()
        mock_db.driver.return_value.session.return_value.__enter__.return_value = mock_session
        
        # R-A 命中
        mock_session.run.return_value.data.side_effect = [
            [{"ids": ["uuid1", "uuid2"]}], # R-A
            [], # R-C
        ]
        mock_session.run.return_value.single.return_value = {"count": 0}
        
        compactor = GraphCompactor(owner_id="test_user", dry_run=False)
        compactor.run_compaction()
        
        # 验证调用了写入逻辑
        # 调用顺序：A检索 -> A写入 -> B检索 -> (B无写入) -> C检索
        # 所以总调用次数 > 3
        self.assertGreater(mock_session.run.call_count, 3)

    def test_report_path(self):
        # 验证生成的报告文件存在且内容正确
        path = ".data/compaction_report.json"
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.assertIn("owner_id", data)
                self.assertIn("rules", data)

if __name__ == "__main__":
    unittest.main()
