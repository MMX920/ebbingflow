"""Saga manager: cluster episodes into long-running story lines."""

import json
import logging
import uuid
from typing import List, Optional

from bridge.llm import LLMBridge
from config import memory_llm_config
from memory.event.slots import MemoryEpisode, MemorySaga

logger = logging.getLogger(__name__)


class SagaManager:
    """Discover and evolve long-running sagas from episodes."""

    def __init__(self):
        self.bridge = LLMBridge(memory_llm_config, category="memory")

    async def cluster_episodes_into_saga(
        self,
        new_episode: MemoryEpisode,
        existing_sagas: List[MemorySaga],
    ) -> Optional[MemorySaga]:
        """Return matched saga or create a new one when uncertain."""
        if not existing_sagas:
            return await self._create_new_saga(new_episode)

        sagas_info = "\n".join(
            [f"- ID: {s.saga_id}, Title: {s.title}, Desc: {s.description}" for s in existing_sagas]
        )

        prompt = (
            "You are a long-term memory archivist.\n"
            "Decide whether the new episode belongs to one of the existing sagas.\n\n"
            "Existing sagas:\n"
            f"{sagas_info}\n\n"
            "New episode:\n"
            f"Title: {new_episode.name}\n"
            f"Summary: {new_episode.summary}\n\n"
            "Output exactly one token:\n"
            "- a matching saga ID, or\n"
            "- NEW"
        )

        try:
            decision_raw = await self.bridge.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            decision = str(decision_raw or "").strip().upper()

            if not decision or "NEW" in decision:
                return await self._create_new_saga(new_episode)

            for saga in existing_sagas:
                saga_id = str(saga.saga_id or "").strip().upper()
                if saga_id and saga_id in decision:
                    return saga

            return await self._create_new_saga(new_episode)

        except Exception as exc:
            logger.error("[SagaManager] Decision failed: %s", exc)
            return await self._create_new_saga(new_episode)

    async def _create_new_saga(self, episode: MemoryEpisode) -> MemorySaga:
        """Create a new saga scaffold from one episode."""
        prompt = (
            "Generate a saga JSON from the episode summary.\n"
            f"Episode summary: {episode.summary}\n\n"
            "Return JSON with keys: title, description."
        )
        try:
            response = await self.bridge.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                response_format={"type": "json_object"},
            )
            payload = json.loads(str(response or "{}"))
            title = str(payload.get("title") or "Untitled Saga").strip() or "Untitled Saga"
            description = str(payload.get("description") or episode.summary or "").strip() or "No description"
        except Exception as exc:
            logger.error("[SagaManager] Create failed: %s", exc)
            title = f"Saga-{episode.name or 'episode'}"
            description = str(episode.summary or "No description")

        return MemorySaga(
            saga_id=str(uuid.uuid4()),
            title=title,
            description=description,
            start_time=episode.start_time,
            last_active=episode.end_time,
            associated_episode_ids=[episode.episode_id],
        )
