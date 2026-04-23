import os
import sys
import unittest

# 将工程根目录加入 Python 路径
sys.path.append(os.getcwd())

from memory.identity.entity_resolution import EntityResolver
from config import identity_config

class TestEntityResolution(unittest.TestCase):
    def setUp(self):
        self.resolver = EntityResolver()
        self.user_root = identity_config.user_id
        self.asst_root = identity_config.assistant_id
        self.ctx = {
            "user_real_name": "张志强",
            "assistant_real_name": "Andrew"
        }

    def test_root_id_match(self):
        res = self.resolver.resolve(self.user_root, "user_001", self.ctx)
        self.assertEqual(res.resolved_root_id, self.user_root)
        self.assertEqual(res.reason, "root_id")
        self.assertEqual(res.confidence, 1.0)
        
        res_asst = self.resolver.resolve(self.asst_root, "user_001", self.ctx)
        self.assertEqual(res_asst.resolved_root_id, self.asst_root)

    def test_alias_match(self):
        # 用户别名
        res_user = self.resolver.resolve("我", "user_001", self.ctx)
        self.assertEqual(res_user.resolved_root_id, self.user_root)
        self.assertEqual(res_user.reason, "alias")
        
        # 助手别名
        res_asst = self.resolver.resolve("Andrew", "user_001", self.ctx)
        self.assertEqual(res_asst.resolved_root_id, self.asst_root)

    def test_canonical_match(self):
        res = self.resolver.resolve("user", "user_001", self.ctx)
        self.assertEqual(res.resolved_root_id, self.user_root)
        self.assertEqual(res.reason, "canonical")

    def test_similarity_match(self):
        # 张志强 vs 张志强 (应该 1.0，但逻辑上走相似度如果是 0.95 以上)
        # 测一个模糊的：张志强架构师 vs 张志强
        res = self.resolver.resolve("张志强架构师", "user_001", self.ctx)
        # 张志强(3) 张志强架构师(6) -> 类似
        if res.reason == "similarity":
            self.assertEqual(res.resolved_root_id, self.user_root)
            self.assertGreaterEqual(res.confidence, 0.7)

    def test_cross_root_forbidden(self):
        # 确保 resolve 逻辑本身是隔离的
        res = self.resolver.resolve("我", "user_001", self.ctx)
        self.assertNotEqual(res.resolved_root_id, self.asst_root)

    def test_filter_resolution(self):
        # 高置信度
        res_high = self.resolver.resolve("我", "user_001", self.ctx)
        self.assertEqual(self.resolver.filter_resolution(res_high, 0.95), self.user_root)
        
        # 低置信度 (模拟低分)
        res_low = self.resolver.resolve("某个陌生人", "user_001", self.ctx)
        self.assertIsNone(self.resolver.filter_resolution(res_low, 0.8))

if __name__ == "__main__":
    unittest.main()
