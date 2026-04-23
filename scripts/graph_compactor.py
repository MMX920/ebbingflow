"""
图谱治理与增量压缩工具 (Graph Compactor)
负责治理长期运行产生的噪音数据，包括：
1. 合并高频重复事件 (R-A)
2. 归档过期无效候选 (R-B)
3. 压缩冗余推理关系 (R-C)
"""
import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# 路径修复
sys.path.append(os.getcwd())
from config import neo4j_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Compactor")

class GraphCompactor:
    def __init__(self, owner_id: str, dry_run: bool = True):
        self._driver = GraphDatabase.driver(
            neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password)
        )
        self.database = neo4j_config.database or "neo4j"
        self.owner_id = owner_id
        self.dry_run = dry_run
        self.report = {
            "timestamp": datetime.utcnow().isoformat(),
            "owner_id": owner_id,
            "dry_run": dry_run,
            "scanned": 0,
            "rules": {
                "R-A_event_merge": 0,
                "R-B_candidate_archive": 0,
                "R-C_relation_suppress": 0
            },
            "saved_ratio": 0.0,
            "errors": []
        }

    def close(self):
        self._driver.close()

    def run_compaction(self, ttl_days: int = 30):
        """执行全量治理任务"""
        logger.info(f"Starting compaction for owner: {self.owner_id} (Dry-run: {self.dry_run})")
        
        try:
            with self._driver.session(database=self.database) as session:
                # 1. R-A: 同秒重复事件压缩
                self._compact_duplicate_events(session)
                
                # 2. R-B: 过期候选归档
                self._archive_expired_candidates(session, ttl_days)
                
                # 3. R-C: 冗余推理边抑制
                self._suppress_redundant_inferred_rels(session)
                
                # 计算比例
                total_ops = sum(self.report["rules"].values())
                if self.report["scanned"] > 0:
                    self.report["saved_ratio"] = round(total_ops / self.report["scanned"], 4)
                
                # 保存报告
                self._save_report()
                
        except Exception as e:
            logger.error(f"Compaction failed: {e}")
            self.report["errors"].append(str(e))
        
        logger.info("Compaction finished. Report saved.")
        return self.report

    def _compact_duplicate_events(self, session):
        """合并同秒重复事件"""
        # 查找逻辑：semantic_key 相同且 record_time 截断到秒一致的 active 事件
        query = """
        MATCH (e:Event {owner_id: $uid, status: 'active'})
        WITH e,
             COALESCE(properties(e)['semantic_key'], '') AS skey,
             COALESCE(properties(e)['compacted'], false) AS compacted,
             substring(toString(COALESCE(properties(e)['record_time'], properties(e)['created_at'], '')), 0, 19) AS second_bucket
        WHERE skey <> '' AND compacted = false
        WITH skey, second_bucket, collect(e) AS evts
        WHERE size(evts) > 1
        RETURN skey, second_bucket, [x in evts | x.uuid] AS ids
        """
        res = session.run(query, uid=self.owner_id)
        records = res.data()
        self.report["scanned"] += len(records)
        
        for r in records:
            ids = r["ids"]
            to_compact = ids[1:] # 保留第一条，压缩剩余
            self.report["rules"]["R-A_event_merge"] += len(to_compact)
            
            if not self.dry_run:
                session.run(
                    "MATCH (e:Event) WHERE e.uuid IN $ids "
                    "SET e.compacted = true, e.status = 'compacted', e.compaction_reason = 'R-A_second_bucket_dedup'",
                    ids=to_compact
                )

    def _archive_expired_candidates(self, session, ttl_days):
        """归档低价值候选"""
        cutoff = (datetime.utcnow() - timedelta(days=ttl_days)).isoformat()
        query = """
        MATCH (e:Event {owner_id: $uid, status: 'candidate'})
        WHERE COALESCE(properties(e)['created_at'], '') < $cutoff
        RETURN count(e) AS count
        """
        res = session.run(query, uid=self.owner_id, cutoff=cutoff)
        count = res.single()["count"]
        self.report["rules"]["R-B_candidate_archive"] += count
        
        if not self.dry_run and count > 0:
            session.run(
                "MATCH (e:Event {owner_id: $uid, status: 'candidate'}) "
                "WHERE COALESCE(e.created_at, '') < $cutoff "
                "SET e.status = 'archived', e.archived = true, e.compaction_reason = 'R-B_ttl_expired'",
                uid=self.owner_id, cutoff=cutoff
            )

    def _suppress_redundant_inferred_rels(self, session):
        """抑制冗余推演边 (若已有同义人工边)"""
        query = """
        MATCH (a:Entity {owner_id: $uid})-[r1:RELATION]->(b:Entity)
        MATCH (a)-[r2:RELATION]->(b)
        WHERE elementId(r1) <> elementId(r2) 
          AND COALESCE(properties(r1)['inferred'], false) = true 
          AND COALESCE(properties(r2)['inferred'], false) = false
          AND r1.type = r2.type 
          AND COALESCE(properties(r1)['status'], 'active') = 'active'
        RETURN elementId(r1) AS rid
        """
        res = session.run(query, uid=self.owner_id)
        records = res.data()
        self.report["rules"]["R-C_relation_suppress"] += len(records)
        
        if not self.dry_run:
            ids = [r["rid"] for r in records]
            session.run(
                "MATCH ()-[r]-() WHERE elementId(r) IN $ids "
                "SET r.status = 'suppressed', r.suppressed = true, r.compaction_reason = 'R-C_human_override'",
                ids=ids
            )

    def _save_report(self):
        os.makedirs(".data", exist_ok=True)
        with open(".data/compaction_report.json", "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description="Graph Compaction CLI")
    parser.add_argument("--owner", default="user_001", help="Target owner ID")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Simulate only")
    parser.add_argument("--apply", action="store_false", dest="dry_run", help="Apply changes")
    parser.add_argument("--candidate-ttl-days", type=int, default=30, help="Candidate archive threshold")
    
    args = parser.parse_args()
    
    compactor = GraphCompactor(args.owner, args.dry_run)
    report = compactor.run_compaction(args.candidate_ttl_days)
    
    print(json.dumps(report, indent=2, ensure_ascii=False))
    compactor.close()

if __name__ == "__main__":
    main()
