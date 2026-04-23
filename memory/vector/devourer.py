"""
Document Devourer (v1.0)
-----------------------------------------------
Features:
  - Sliding window chunking (Overlap protection)
  - Batch vector storage
  - Event & Relation extraction via EventExtractor
  - Dual-write: VectorStore + Neo4j Graph
"""
import asyncio
import logging
from typing import Optional, List

from memory.vector.storer import VectorStorer
from memory.event.extractor import EventExtractor
from memory.identity.resolver import Actor

logger = logging.getLogger(__name__)

C_GREEN = "\033[32m"
C_CYAN = "\033[36m"
C_YELLOW = "\033[33m"
C_RESET = "\033[0m"


class DocumentDevourer:
    """
    Engine to digest long documents into vector and graph memory.
    """
    
    def __init__(self, chunk_size: int = 2000, overlap: int = 300):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.storer = VectorStorer()
        self.extractor = EventExtractor()

    def _sliding_chunk(self, text: str) -> List[str]:
        chunks = []
        start = 0
        while start < len(text):
            end = min(start + self.chunk_size, len(text))
            chunks.append(text[start:end])
            if end == len(text):
                break
            start += self.chunk_size - self.overlap
        return chunks

    async def devour(
        self,
        text: str,
        source_name: str,
        actor: Actor,
        session_id: str = "document_ingestion",
        extract_graph: bool = False
    ) -> dict:
        print(f"\n{C_CYAN}[DocumentDevourer] Starting ingestion for: {source_name} ({len(text)} chars){C_RESET}")
        
        # 1. Chunking
        chunks = self._sliding_chunk(text)
        print(f"{C_CYAN}[DocumentDevourer] Split into {len(chunks)} chunks (size={self.chunk_size}, overlap={self.overlap}){C_RESET}")
        
        # 2. Vector Storage
        stored_count = self.storer.store_document_chunks(
            chunks=chunks,
            source_name=source_name,
            metadata_extra={"session_id": session_id}
        )
        print(f"{C_GREEN}[DocumentDevourer] [OK] {stored_count} chunks stored in vector memory.{C_RESET}")
        
        # 3. Graph Extraction (Optional)
        total_events = 0
        if extract_graph:
            from memory.graph.writer import AsyncGraphWriter
            graph_writer = AsyncGraphWriter()
            
            for i, chunk in enumerate(chunks):
                print(f"{C_YELLOW}[DocumentDevourer] Processing chunk {i+1}/{len(chunks)}...{C_RESET}", end="\r")
                valid_events, candidate_events, relations, observations, valid_envelopes = await self.extractor.extract_events_from_text(
                    chunk, actor
                )
                
                if valid_events or candidate_events:
                    await graph_writer.write_events(
                        valid_events=valid_events,
                        candidate_events=candidate_events,
                        session_id=session_id,
                        owner_id=actor.speaker_id,
                        current_names={
                            "user": actor.speaker_name,
                            "assistant": actor.target_name,
                        },
                        chat_session=None,
                    )
                    total_events += len(valid_events)
                if relations:
                    await graph_writer.write_relations(
                        relations=relations,
                        session_id=session_id,
                        owner_id=actor.speaker_id,
                        current_names={
                            "user": actor.speaker_name,
                            "assistant": actor.target_name,
                        },
                        chat_session=None,
                    )
            
            await graph_writer.close()
            print(f"\n{C_GREEN}[DocumentDevourer] [OK] Graph ingestion complete. Extracted {total_events} events.{C_RESET}")
        else:
            print(f"{C_CYAN}[DocumentDevourer] Graph extraction skipped.{C_RESET}")
        
        return {
            "chunks_stored": stored_count,
            "events_extracted": total_events,
            "source": source_name
        }
