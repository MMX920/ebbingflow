import sys
import os
import asyncio
import json
import argparse
from datetime import datetime
from neo4j import AsyncGraphDatabase

# 确保能导入项目配置
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import neo4j_config, identity_config
except ImportError:
    print("Error: Could not import config. Please run from project root.")
    sys.exit(1)

class IdentityAuditor:
    def __init__(self):
        self.uri = neo4j_config.uri
        self.auth = (neo4j_config.username, neo4j_config.password)
        self.database = neo4j_config.database
        self.driver = None

    async def connect(self):
        self.driver = AsyncGraphDatabase.driver(self.uri, auth=self.auth)

    async def close(self):
        if self.driver:
            await self.driver.close()

    async def run_audit(self):
        results = {
            "timestamp": datetime.now().isoformat(),
            "diagnosis": {},
            "summary": "UNKNOWN",
            "errors": []
        }

        async with self.driver.session(database=self.database) as session:
            # 1. ROOT_STATUS
            try:
                root_ids = [identity_config.user_id, identity_config.assistant_id]
                res = await session.run(
                    "MATCH (e:Entity) WHERE e.entity_id IN $ids RETURN e.entity_id as id, e.name as name, e.status as status",
                    ids=root_ids
                )
                roots = {r["id"]: {"name": r["name"], "status": r["status"]} async for r in res}
                results["diagnosis"]["ROOT_STATUS"] = {
                    "expected": root_ids,
                    "found": list(roots.keys()),
                    "details": roots,
                    "missing": [rid for rid in root_ids if rid not in roots]
                }
            except Exception as e:
                results["errors"].append(f"ROOT_STATUS Error: {e}")

            # 2. ORPHAN_ENTITIES
            try:
                res = await session.run(
                    "MATCH (e:Entity) WHERE NOT (e)--() RETURN e.name as name LIMIT 20"
                )
                orphans = [r["name"] async for r in res]
                count_res = await session.run("MATCH (e:Entity) WHERE NOT (e)--() RETURN count(e) as count")
                count_record = await count_res.single()
                results["diagnosis"]["ORPHAN_ENTITIES"] = {
                    "count": count_record["count"] if count_record else 0,
                    "samples": orphans
                }
            except Exception as e:
                results["errors"].append(f"ORPHAN_ENTITIES Error: {e}")

            # 3. SELF_LOOP_RELATIONS
            try:
                res = await session.run(
                    "MATCH (a:Entity)-[r]->(a) RETURN count(r) as count"
                )
                record = await res.single()
                results["diagnosis"]["SELF_LOOP_RELATIONS"] = {
                    "count": record["count"] if record else 0
                }
            except Exception as e:
                results["errors"].append(f"SELF_LOOP Error: {e}")

            # 4. EVENT_HEALTH
            try:
                res = await session.run("""
                    MATCH (e:Event)
                    RETURN 
                        sum(CASE WHEN e.status = 'active' THEN 1 ELSE 0 END) as active,
                        sum(CASE WHEN e.status = 'invalidated' THEN 1 ELSE 0 END) as invalidated,
                        sum(CASE WHEN e.invalid_at IS NOT NULL THEN 1 ELSE 0 END) as has_invalid_at
                """)
                record = await res.single()
                results["diagnosis"]["EVENT_HEALTH"] = {
                    "active": record["active"] or 0,
                    "invalidated": record["invalidated"] or 0,
                    "has_invalid_at": record["has_invalid_at"] or 0
                }
            except Exception as e:
                results["errors"].append(f"EVENT_HEALTH Error: {e}")

            # 5. TEMPORAL_SLOT_HEALTH
            try:
                res = await session.run("""
                    MATCH (e:Event)
                    WHERE e.action_type = 'STATE_CHANGE' OR e.predicate IN ['name', 'role', 'state']
                    RETURN 
                        sum(CASE WHEN e.temporal_slot IS NULL THEN 1 ELSE 0 END) as missing,
                        count(e) as total
                """)
                record = await res.single()
                results["diagnosis"]["TEMPORAL_SLOT_HEALTH"] = {
                    "missing": record["missing"] or 0,
                    "total": record["total"] or 0
                }
            except Exception as e:
                results["errors"].append(f"TEMPORAL_SLOT Error: {e}")

        # Final Summary
        diag = results["diagnosis"]
        if results["errors"] or (diag.get("ROOT_STATUS") and diag["ROOT_STATUS"]["missing"]):
            results["summary"] = "FAIL"
        elif (
            (diag.get("ORPHAN_ENTITIES") and diag["ORPHAN_ENTITIES"]["count"] > 0) or
            (diag.get("SELF_LOOP_RELATIONS") and diag["SELF_LOOP_RELATIONS"]["count"] > 0) or
            (diag.get("TEMPORAL_SLOT_HEALTH") and diag["TEMPORAL_SLOT_HEALTH"]["missing"] > 0)
        ):
            results["summary"] = "WARN"
        else:
            results["summary"] = "PASS"

        return results

def print_text_report(res):
    print("\n" + "="*60)
    print(f" EBBINGFLOW IDENTITY STATE AUDIT REPORT")
    print(f" Timestamp: {res['timestamp']}")
    print("="*60)

    diag = res["diagnosis"]
    
    # 1. ROOT
    r = diag.get("ROOT_STATUS", {})
    status_str = "OK" if not r.get("missing") else f"MISSING: {r['missing']}"
    print(f"\n[1] ROOT_STATUS: {status_str}")
    for rid, detail in r.get("details", {}).items():
        print(f"    - {rid}: {detail['name']} ({detail['status'] or 'no-status'})")

    # 2. ORPHANS
    o = diag.get("ORPHAN_ENTITIES", {})
    print(f"\n[2] ORPHAN_ENTITIES: {o.get('count', 0)}")
    if o.get("samples"):
        print(f"    Samples: {', '.join(o['samples'])}")

    # 3. SELF_LOOPS
    s = diag.get("SELF_LOOP_RELATIONS", {})
    print(f"\n[3] SELF_LOOP_RELATIONS: {s.get('count', 0)}")

    # 4. EVENTS
    e = diag.get("EVENT_HEALTH", {})
    print(f"\n[4] EVENT_HEALTH")
    print(f"    - Active:      {e.get('active', 0)}")
    print(f"    - Invalidated: {e.get('invalidated', 0)}")
    print(f"    - Has invalid_at: {e.get('has_invalid_at', 0)}")

    # 5. TEMPORAL SLOTS
    ts = diag.get("TEMPORAL_SLOT_HEALTH", {})
    print(f"\n[5] TEMPORAL_SLOT_HEALTH")
    print(f"    - Missing: {ts.get('missing', 0)} / Total State Changes: {ts.get('total', 0)}")

    if res["errors"]:
        print("\n[!] RUNTIME ERRORS:")
        for err in res["errors"]:
            print(f"    - {err}")

    print("\n" + "-"*60)
    color = ""
    if res["summary"] == "PASS": color = "\033[92m" # Green
    elif res["summary"] == "WARN": color = "\033[93m" # Yellow
    elif res["summary"] == "FAIL": color = "\033[91m" # Red
    
    print(f" FINAL SUMMARY: {color}{res['summary']}\033[0m")
    print("-"*60 + "\n")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    args = parser.parse_args()

    auditor = IdentityAuditor()
    try:
        await auditor.connect()
        results = await auditor.run_audit()
        
        if args.json:
            print(json.dumps(results, indent=2, ensure_ascii=False))
        else:
            print_text_report(results)
            
        sys.exit(0 if results["summary"] != "FAIL" else 1)
    except Exception as e:
        if args.json:
            print(json.dumps({"summary": "FAIL", "error": str(e)}, indent=2))
        else:
            print(f"\n\033[91mFAIL: Fatal Audit Error\033[0m")
            print(f"Reason: {e}")
        sys.exit(1)
    finally:
        await auditor.close()

if __name__ == "__main__":
    asyncio.run(main())
