"""
关系语义推理引擎 (Relation Reasoner Engine)
根据预设的语义规则（对称、传递、反转等）对图谱关系进行逻辑推演。
"""
import logging
from typing import List, Dict, Any, Optional
from neo4j import AsyncGraphDatabase
from config import neo4j_config

logger = logging.getLogger(__name__)

# 关系层级定义
REL_LEVELS = {
    "IDENTITY": ["SAME_AS", "ALIAS_OF"],
    "ROLE": ["HAS_ROLE", "SERVES", "OWNS"],
    "SOCIAL": ["KNOWS", "FRIEND_OF", "COLLEAGUE_OF"],
    "STATE": ["CURRENT_STATE", "MOOD"],
    "GENERIC": []
}

class RelationReasoner:
    def __init__(self, driver: AsyncGraphDatabase = None):
        self._driver = driver or AsyncGraphDatabase.driver(
            neo4j_config.uri, auth=(neo4j_config.username, neo4j_config.password)
        )
        self.database = neo4j_config.database or "neo4j"

    async def reason(self, new_relations: List[Dict[str, Any]], owner_id: str) -> List[Dict[str, Any]]:
        """
        基于当前新增关系执行推理逻辑。
        返回需要补齐的推演边列表。
        """
        inferred_rels = []
        try:
            async with self._driver.session(database=self.database) as session:
                for rel in new_relations:
                    # 提取基础信息
                    rt = rel.get("type", "").upper()
                    f_name = rel.get("from")
                    t_name = rel.get("to")
                    fid = rel.get("from_id")
                    tid = rel.get("to_id")
                    source_msg_id = rel.get("source_msg_id")
                    
                    if not f_name or not t_name: continue

                    # R4: 对称关系推演 (FRIEND_OF)
                    if rt == "FRIEND_OF":
                        inferred_rels.append(self._make_inferred(t_name, "FRIEND_OF", f_name, tid, fid, "R4", source_msg_id))

                    # R3: 可逆关系推演 (SERVES -> OWNS)
                    if rt == "SERVES":
                        inferred_rels.append(self._make_inferred(t_name, "OWNS", f_name, tid, fid, "R3", source_msg_id))
                    if rt == "OWNS":
                        inferred_rels.append(self._make_inferred(t_name, "SERVES", f_name, tid, fid, "R3", source_msg_id))

                    # R1/R2: 基于 SAME_AS 的推理 (需要查询图谱状态)
                    if rt == "SAME_AS":
                        # R1: 传递性 (A=B, B=C -> A=C)
                        chain_res = await session.run(
                            "MATCH (b:Entity {owner_id: $uid})-[:RELATION {type: 'SAME_AS'}]->(c:Entity) "
                            "WHERE (b.name = $tid OR b.entity_id = $tid) AND b.owner_id = $uid "
                            "RETURN c.name as c_name, c.entity_id as c_id",
                            tid=tid or t_name, uid=owner_id
                        )
                        chain_data = await chain_res.data()
                        for r in chain_data:
                            if r['c_name'] != f_name and r['c_id'] != fid: # 去环自环
                                inferred_rels.append(self._make_inferred(f_name, "SAME_AS", r['c_name'], fid, r['c_id'], "R1", source_msg_id))
                        
                        # R2: 迁移性 (A=B, A-R-X -> B-R-X)
                        # 这里简单实现：如果 A=B，则将 B 也连接到 A 参与的所有 Event/Relation (Neo4j 层实现负载较大，此处仅做语义记录)
                        # 实际上：R2 通常通过检索层的实体合并完成，此处仅生成 B-SAME_AS-A 对称边
                        inferred_rels.append(self._make_inferred(t_name, "SAME_AS", f_name, tid, fid, "R2", source_msg_id))

        except Exception as e:
            logger.error(f"🧬 [RELATION_REASONER_FAILED] 推理失败: {e}")
            
        return inferred_rels

    def _make_inferred(
        self,
        f: str,
        t: str,
        to: str,
        fid: str,
        tid: str,
        rule: str,
        source_msg_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """构造推演边负载"""
        return {
            "from": f,
            "type": t,
            "to": to,
            "from_id": fid,
            "to_id": tid,
            "inferred": True,
            "inference_rule": rule,
            "confidence": 0.6,
            "status": "active",
            "source_msg_id": source_msg_id,
        }

    async def close(self):
        # 外部驱动不关闭
        pass
