"""Vector retrieval over chat memory and user-scoped knowledge chunks."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from memory.vector.storer import VectorStorer

logger = logging.getLogger(__name__)


@dataclass
class VectorSearchResult:
    content: str
    score: float
    speaker: str
    timestamp: str
    source_type: str
    source_name: str


class VectorRetriever:
    """GraphRAG vector retriever with user_id isolation."""

    def __init__(self, window_cutoff: int = 6):
        self.storer = VectorStorer()
        self.window_cutoff = window_cutoff

    def retrieve(
        self,
        query: str,
        session_id: str,
        user_id: str = "default_user",
        recent_timestamps: Optional[List[str]] = None,
        top_k: int = 5,
        include_docs: bool = True,
    ) -> List[VectorSearchResult]:
        results: List[VectorSearchResult] = []

        try:
            chat_results = self.storer.chat_collection.query(
                query_texts=[query],
                n_results=min(top_k, self.storer.get_chat_count() or 1),
                where={"$and": [{"session_id": session_id}, {"user_id": user_id}]},
            )
            results.extend(self._normalize_results(chat_results, "chat", "chat_memory", recent_timestamps))
        except Exception as exc:
            logger.warning("[VectorRetriever] chat retrieval failed: %s", exc)

        if include_docs and self.storer.get_doc_count() > 0:
            try:
                doc_results = self.storer.doc_collection.query(
                    query_texts=[query],
                    n_results=min(top_k, self.storer.get_doc_count()),
                    where={"user_id": user_id},
                )
                results.extend(self._normalize_results(doc_results, "document", "knowledge_base", None))
            except Exception as exc:
                logger.warning("[VectorRetriever] document retrieval failed: %s", exc)

        results.sort(key=lambda item: item.score, reverse=True)
        return results[:top_k]

    @staticmethod
    def _normalize_results(
        raw: Dict[str, Any],
        source_type: str,
        fallback_source: str,
        recent_timestamps: Optional[List[str]],
    ) -> List[VectorSearchResult]:
        if not raw or not raw.get("documents") or not raw["documents"][0]:
            return []

        output: List[VectorSearchResult] = []
        docs = raw["documents"][0]
        metas = raw.get("metadatas", [[]])[0]
        dists = raw.get("distances", [[]])[0]

        for doc, meta, dist in zip(docs, metas, dists):
            meta = meta or {}
            if recent_timestamps and meta.get("timestamp") in recent_timestamps:
                continue
            output.append(
                VectorSearchResult(
                    content=doc,
                    score=1 - dist,
                    speaker=meta.get("speaker", "[Document]" if source_type == "document" else "unknown"),
                    timestamp=meta.get("timestamp", ""),
                    source_type=source_type,
                    source_name=meta.get("source") or meta.get("session_id") or fallback_source,
                )
            )
        return output

    def format_for_prompt(self, results: List[VectorSearchResult], max_chars: int = 800) -> str:
        if not results:
            return ""

        lines = ["[Vector memory recall] Relevant historical context:"]
        total_chars = 0

        for item in results:
            entry = f"  [{item.source_type.upper()}] ({item.speaker} | {item.timestamp[:10] if item.timestamp else '?'}) {item.content}"
            if total_chars + len(entry) > max_chars:
                break
            lines.append(entry)
            total_chars += len(entry)

        return "\n".join(lines)
