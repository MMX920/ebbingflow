"""
向量存储引擎 (Vector Storer v1.0) 统一版本
支持 Local (SentenceTransformers), OpenAI-compatible (含 Dashscope/Ollama) 嵌入。
"""
import os
import uuid
import logging
import hashlib
from typing import List, Optional, Dict, Any
from datetime import datetime

import chromadb
from chromadb.utils import embedding_functions

from config import embed_config

logger = logging.getLogger(__name__)

CHROMA_PERSIST_DIR = ".data/chroma"


class VectorInitError(Exception):
    """向量库初始化致命错误"""


class DashscopeSafeEmbeddingWrapper:
    def __init__(self, base_fn):
        self.base_fn = base_fn

    def __call__(self, input):
        if isinstance(input, str):
            input = [input]
        all_embeddings = []
        batch_size = 10
        for i in range(0, len(input), batch_size):
            batch = input[i : i + batch_size]
            embeddings = self.base_fn(batch)
            all_embeddings.extend(embeddings)
        return all_embeddings

    def __getattr__(self, name):
        return getattr(self.base_fn, name)

    def name(self) -> str:
        return "dashscope-safe-wrapper"


class VectorStorer:
    """ChromaDB 向量写入/读取中心器"""

    def __init__(self):
        try:
            os.makedirs(CHROMA_PERSIST_DIR, exist_ok=True)
            self.client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)

            self.embed_fn = self._get_embedding_function()
            self.degraded = False

            if self.embed_fn:
                try:
                    # 低开销握手测试
                    self.embed_fn(["h"])
                except Exception as he:
                    logger.warning(
                        "[VectorStorer] 初始网络握手缓慢 (%s), 引擎将尝试在运行时自动恢复。",
                        he,
                    )
            else:
                raise VectorInitError("Embedding Function 未正确配置")

            self.chat_collection = self._get_or_create_collection_safe(
                name="chat_memory",
                metadata={"hnsw:space": "cosine"},
            )
            self.doc_collection = self._get_or_create_collection_safe(
                name="knowledge_base",
                metadata={"hnsw:space": "cosine"},
            )

            logger.info("[VectorStorer] ChromaDB 引擎就绪 (Type: %s)", embed_config.type)
        except Exception as e:
            logger.critical("[VectorStorer] 核心初始化失败: %s", e)
            raise VectorInitError(f"无法启动向量引擎: {e}")

    def _get_or_create_collection_safe(self, name: str, metadata: Dict[str, Any]):
        """
        Chroma collection open/create with embedding-function conflict fallback.
        """
        try:
            return self.client.get_or_create_collection(
                name=name,
                embedding_function=self.embed_fn,
                metadata=metadata,
            )
        except ValueError as exc:
            msg = str(exc)
            if "Embedding function conflict" in msg:
                logger.warning(
                    "[VectorStorer] Embedding function conflict on '%s'. "
                    "Falling back to opening existing collection without binding a new function.",
                    name,
                )
                return self.client.get_or_create_collection(name=name, metadata=metadata)
            raise

    def _get_embedding_function(self):
        e_type = (embed_config.type or "").lower()

        if e_type in {"openai", "ollama"}:
            logger.info("[EmbedFactory] 启用 OpenAI 兼容嵌入接口: %s", embed_config.model)
            base_fn = embedding_functions.OpenAIEmbeddingFunction(
                api_key=embed_config.api_key,
                api_base=embed_config.base_url,
                model_name=embed_config.model,
            )

            if "dashscope" in (embed_config.base_url or "").lower():
                logger.info("[EmbedFactory] 检测到 Dashscope 接口，激活 10-Batch 安全装箱器。")
                return DashscopeSafeEmbeddingWrapper(base_fn)

            return base_fn

        logger.info("[EmbedFactory] 启用本地 Sentence-Transformers 嵌入: %s", embed_config.model)
        return embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=embed_config.model
        )

    def store_chat_turn(
        self,
        speaker: str,
        content: str,
        session_id: str,
        user_id: str = "default_user",
        role: str = "user",
        timestamp: Optional[str] = None,
    ) -> None:
        if not content or not content.strip():
            return

        try:
            content_hash = hashlib.md5(content.encode()).hexdigest()
            results = self.chat_collection.get(
                where={
                    "$and": [
                        {"session_id": session_id},
                        {"role": role},
                        {"content_hash": content_hash},
                    ]
                },
                limit=1,
            )
            if results and results.get("ids") and results.get("metadatas"):
                meta = (results["metadatas"][0] or {})
                ts = meta.get("timestamp")
                if ts:
                    try:
                        from datetime import timezone

                        old_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        now_dt = datetime.now(old_dt.tzinfo or timezone.utc)
                        if (now_dt - old_dt).total_seconds() <= 3:
                            return
                    except Exception:
                        pass
        except Exception:
            pass

        from core.monitoring import token_monitor

        token_monitor.record_embedding_usage(len(content))

        doc_id = str(uuid.uuid4())
        timestamp = timestamp or datetime.now().isoformat()

        self.chat_collection.add(
            documents=[content],
            ids=[doc_id],
            metadatas=[
                {
                    "speaker": speaker,
                    "role": role,
                    "session_id": session_id,
                    "user_id": user_id,
                    "timestamp": timestamp,
                    "content_hash": hashlib.md5(content.encode()).hexdigest(),
                    "type": "chat",
                }
            ],
        )

    def store_document_chunks(
        self,
        chunks: List[str],
        source_name: str,
        metadata_extra: Optional[Dict[str, Any]] = None,
    ) -> int:
        if not chunks:
            return 0

        from core.monitoring import token_monitor

        total_len = sum(len(c) for c in chunks)
        token_monitor.record_embedding_usage(total_len)

        ids = [str(uuid.uuid4()) for _ in chunks]
        base_meta = {
            "source": source_name,
            "type": "document",
            "timestamp": datetime.now().isoformat(),
        }
        if metadata_extra:
            base_meta.update(metadata_extra)

        metadatas = [base_meta.copy() for _ in chunks]
        self.doc_collection.add(documents=chunks, ids=ids, metadatas=metadatas)
        return len(chunks)

    def get_chat_count(self) -> int:
        return self.chat_collection.count()

    def get_doc_count(self) -> int:
        return self.doc_collection.count()

    def get_recent_chat_history(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """从向量库中恢复该用户最近的历史对话。"""
        try:
            results = self.chat_collection.get(where={"user_id": user_id}, limit=500)
            if not results or not results.get("ids"):
                return []

            messages: List[Dict[str, Any]] = []
            for i in range(len(results["ids"])):
                messages.append(
                    {
                        "role": results["metadatas"][i].get("role", "user"),
                        "content": results["documents"][i],
                        "name": results["metadatas"][i].get("speaker"),
                        "timestamp": results["metadatas"][i].get("timestamp", ""),
                    }
                )

            messages.sort(key=lambda x: x["timestamp"])
            return messages[-limit:] if limit > 0 else messages
        except Exception as e:
            logger.error("[VectorStorer] 恢复历史记录失败: %s", e)
            return []
