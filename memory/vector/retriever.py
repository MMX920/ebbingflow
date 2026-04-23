"""
向量检索引擎 (Vector Retriever v1.0)
-----------------------------------------------
负责从 ChromaDB 中通过语义相似度召回历史聊天和文档知识。
核心设计：
  - 窗口截断逻辑：自动排除最近 N 轮对话（防止近期记忆冲突）
  - 双集合检索：同时扫描 chat_memory 和 knowledge_base
  - 结果格式化：返回带有元数据的结构化对象列表
"""
import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from memory.vector.storer import VectorStorer

logger = logging.getLogger(__name__)


@dataclass
class VectorSearchResult:
    """向量检索结果的标准化数据结构"""
    content: str
    score: float          # 相似度分数 (0~1，越高越相关)
    speaker: str
    timestamp: str
    source_type: str      # "chat" or "document"
    source_name: str      # 文档来源或会话ID


class VectorRetriever:
    """
    GraphRAG 向量检索器
    - 支持窗口截断（window_cutoff 条之前的内容才参与向量检索）
    - 联合检索聊天记忆 + 文档知识库
    """
    
    def __init__(self, window_cutoff: int = 6):
        """
        参数：
            window_cutoff: 最近 N 轮对话不参与向量检索（默认6轮=12条记录）
                           这些近期对话已在 system prompt 的短时记忆区中，无需重复召回
        """
        self.storer = VectorStorer()
        self.window_cutoff = window_cutoff

    def retrieve(
        self,
        query: str,
        session_id: str,
        recent_timestamps: Optional[List[str]] = None,
        top_k: int = 5,
        include_docs: bool = True
    ) -> List[VectorSearchResult]:
        """
        执行双轨向量检索：
        1. 在聊天记忆中搜索（排除最近 N 轮）
        2. 在文档知识库中搜索
        3. 归并排序返回
        """
        results: List[VectorSearchResult] = []
        
        # ── 轨道一：历史聊天记忆检索 ──────────────────────────────
        try:
            chat_results = self.storer.chat_collection.query(
                query_texts=[query],
                n_results=min(top_k, self.storer.get_chat_count() or 1),
                where={"session_id": session_id}  # 只在当前用户的记忆范围内查找
            )
            
            if chat_results and chat_results.get("documents"):
                docs = chat_results["documents"][0]
                metas = chat_results["metadatas"][0]
                dists = chat_results["distances"][0]
                
                for doc, meta, dist in zip(docs, metas, dists):
                    # 余弦距离转相似度 (distance 越小越相似)
                    score = 1 - dist
                    
                    # 窗口截断：如果时间戳在最近 N 轮内，跳过
                    if recent_timestamps and meta.get("timestamp") in recent_timestamps:
                        continue
                    
                    results.append(VectorSearchResult(
                        content=doc,
                        score=score,
                        speaker=meta.get("speaker", "unknown"),
                        timestamp=meta.get("timestamp", ""),
                        source_type="chat",
                        source_name=meta.get("session_id", "")
                    ))
        except Exception as e:
            logger.warning(f"[VectorRetriever] 聊天记忆检索异常: {e}")
        
        # ── 轨道二：文档知识库检索 ──────────────────────────────
        if include_docs and self.storer.get_doc_count() > 0:
            try:
                doc_results = self.storer.doc_collection.query(
                    query_texts=[query],
                    n_results=min(top_k, self.storer.get_doc_count())
                )
                
                if doc_results and doc_results.get("documents"):
                    docs = doc_results["documents"][0]
                    metas = doc_results["metadatas"][0]
                    dists = doc_results["distances"][0]
                    
                    for doc, meta, dist in zip(docs, metas, dists):
                        score = 1 - dist
                        results.append(VectorSearchResult(
                            content=doc,
                            score=score,
                            speaker="[文档]",
                            timestamp=meta.get("timestamp", ""),
                            source_type="document",
                            source_name=meta.get("source", "unknown")
                        ))
            except Exception as e:
                logger.warning(f"[VectorRetriever] 文档知识库检索异常: {e}")
        
        # 按相似度降序排列，返回最相关的 top_k 条
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]

    def format_for_prompt(self, results: List[VectorSearchResult], max_chars: int = 800) -> str:
        """
        将检索结果格式化为可直接注入 System Prompt 的文本块。
        """
        if not results:
            return ""
        
        lines = ["[向量记忆召回区] 以下是语义相关的历史信息片段："]
        total_chars = 0
        
        for r in results:
            entry = f"  [{r.source_type.upper()}] ({r.speaker} | {r.timestamp[:10] if r.timestamp else '?'}) {r.content}"
            if total_chars + len(entry) > max_chars:
                break
            lines.append(entry)
            total_chars += len(entry)
        
        return "\n".join(lines)
