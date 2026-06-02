import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncContextManager
from uuid import UUID

from core.entities.service import EntityService
from core.models import Claim, Narrative, Video
from core.narratives.models import (
    NarrativeDetail,
    NarrativeInput,
    NarrativeListItem,
    NarrativePatchInput,
    NarrativeStats,
    NarrativeSummary,
    ViralNarrativeSummary,
)
from core.narratives.repo import NarrativeRepository
from core.narratives.api import NarrativesApiClient
from core.uow import ConnectionFactory, uow

logger = logging.getLogger(__name__)

_api = NarrativesApiClient()

# Best-effort entity-extraction trigger: seconds to wait between attempts.
# len + 1 = total attempts (here: 3 attempts, ~4s worst case before giving up).
_EXTRACTION_RETRY_BACKOFFS = (1.0, 3.0)

# Keep strong references to in-flight background trigger tasks so the event
# loop doesn't garbage-collect them mid-flight; the done-callback drops them.
_background_tasks: set[asyncio.Task] = set()


def _merge_narrative_context(
    existing: str | None, new: str | None
) -> str | None:
    """Concatenate narrative context entries with a timestamped separator."""
    if not new:
        return existing
    if not existing:
        return new
    timestamp = datetime.now().isoformat()
    return f"{existing}\n\n--{timestamp}--\n\n{new}"


class NarrativeService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[NarrativeRepository]:
        return uow(NarrativeRepository, self._connection_factory)

    async def create_narrative(self, narrative: NarrativeInput) -> Narrative:
        async with self.repo() as repo:
            if not await repo.claims_exist(narrative.claim_ids):
                raise ValueError("one or more claims not found")

            # Process entities first
            entity_ids = []
            if narrative.entities:
                entity_service = EntityService(self._connection_factory)
                entity_ids = await entity_service.process_entities(narrative.entities)

            # First check if a narrative with the same title exists
            existing_narrative = await repo.find_by_title(narrative.title)

            # If no narrative with the same title exists, check for narrative_id in metadata
            if not existing_narrative:
                narrative_id_in_metadata = narrative.metadata.get("narrative_id")
                if narrative_id_in_metadata:
                    existing_narrative = await repo.find_by_narrative_id_in_metadata(
                        narrative_id_in_metadata
                    )

            if existing_narrative:
                # Merge claim_ids and topic_ids with existing ones
                existing_claim_ids = [claim.id for claim in existing_narrative.claims]
                merged_claim_ids = list(set(existing_claim_ids + narrative.claim_ids))

                existing_topic_ids = [topic.id for topic in existing_narrative.topics]
                merged_topic_ids = list(set(existing_topic_ids + narrative.topic_ids))

                # Merge entity_ids with existing ones
                existing_entity_ids = [entity.id for entity in existing_narrative.entities]
                merged_entity_ids = list(set(existing_entity_ids + entity_ids))

                # Concatenate narrative_context with existing one
                merged_narrative_context = _merge_narrative_context(
                    existing_narrative.narrative_context,
                    narrative.narrative_context,
                )

                updated_narrative = await repo.update_narrative(
                    narrative_id=existing_narrative.id,
                    title=narrative.title,
                    description=narrative.description,
                    narrative_context=merged_narrative_context,
                    claim_ids=merged_claim_ids,
                    topic_ids=merged_topic_ids,
                    entity_ids=merged_entity_ids,
                    metadata=narrative.metadata,
                )
                if updated_narrative is None:
                    raise ValueError(f"Failed to update narrative with ID {existing_narrative.id}")
                result = updated_narrative
                is_new = False
            else:
                result = await repo.create_narrative(
                    title=narrative.title,
                    description=narrative.description,
                    claim_ids=narrative.claim_ids,
                    topic_ids=narrative.topic_ids,
                    entity_ids=entity_ids,
                    metadata=narrative.metadata,
                    narrative_context=narrative.narrative_context,
                )
                is_new = True

        # Narrative is committed. Trigger entity extraction best-effort and in
        # the background, so a slow or unavailable narratives service never
        # blocks or fails narrative creation (same best-effort contract the
        # external update/delete sync already uses). Retries run out-of-band;
        # if they all fail, the periodic backfill sweep recovers the narrative.
        #
        # Only fire on NEW narratives: extraction re-sends the enriched narrative
        # back here (update path), so triggering on updates too would feed back
        # into itself and re-extract in a loop. Updates/merges don't need
        # re-extraction via this path.
        external_id = narrative.metadata.get("narrative_id")
        if is_new and external_id:
            # Pass our own PK: the knowledge graph is keyed by the backend
            # narrative id, and handing it over explicitly avoids the race where
            # the trigger reaches narratives before it has written backend_id
            # into its Qdrant payload.
            self._schedule_entity_extraction(external_id, str(result.id))

        return result

    def _schedule_entity_extraction(
        self, external_narrative_id: str, backend_id: str
    ) -> None:
        """Fire entity extraction in the background; never blocks the caller."""
        task = asyncio.create_task(
            self._trigger_entity_extraction(external_narrative_id, backend_id)
        )
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)

    async def _trigger_entity_extraction(
        self, external_narrative_id: str, backend_id: str
    ) -> None:
        """Best-effort: ask the external narratives API to extract a narrative's
        entities, retrying a few times on transient failure.

        Never raises — entity extraction is secondary enrichment and must not
        affect narrative creation. After exhausting retries it logs an error so
        the narrative can be recovered later by the periodic backfill sweep.
        """
        if not _api.is_configured():
            return

        attempts = len(_EXTRACTION_RETRY_BACKOFFS) + 1
        detail = ""
        for attempt in range(1, attempts + 1):
            try:
                response = await _api.extract_entities(
                    external_narrative_id, backend_id=backend_id
                )
                if response.status_code < 400:
                    logger.info(
                        f"Queued entity extraction for narrative "
                        f"{external_narrative_id} (attempt {attempt}/{attempts})"
                    )
                    return
                detail = f"status={response.status_code} body={response.text[:200]}"
            except Exception as e:
                detail = repr(e)

            logger.warning(
                f"Entity extraction trigger for {external_narrative_id} failed "
                f"(attempt {attempt}/{attempts}): {detail}"
            )
            if attempt < attempts:
                await asyncio.sleep(_EXTRACTION_RETRY_BACKOFFS[attempt - 1])

        logger.error(
            f"Gave up triggering entity extraction for narrative "
            f"{external_narrative_id} after {attempts} attempts ({detail}); "
            "periodic backfill must recover it."
        )

    async def get_narrative(self, narrative_id: UUID) -> Narrative | None:
        async with self.repo() as repo:
            return await repo.get_narrative(narrative_id)

    async def get_narrative_detail(
        self,
        narrative_id: UUID,
        claims_limit: int = 10,
        videos_limit: int = 10,
    ) -> NarrativeDetail | None:
        async with self.repo() as repo:
            return await repo.get_narrative_detail(
                narrative_id, claims_limit=claims_limit, videos_limit=videos_limit
            )

    async def get_narrative_claims(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Claim], int]:
        async with self.repo() as repo:
            if not await repo.narrative_exists(narrative_id):
                raise ValueError("narrative not found")
            return await repo.get_narrative_claims(narrative_id, limit, offset)

    async def get_narrative_videos(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Video], int]:
        async with self.repo() as repo:
            if not await repo.narrative_exists(narrative_id):
                raise ValueError("narrative not found")
            return await repo.get_narrative_videos(narrative_id, limit, offset)

    async def get_narrative_stats(self, narrative_id: UUID) -> NarrativeStats | None:
        async with self.repo() as repo:
            return await repo.get_narrative_stats(narrative_id)

    async def get_narratives_by_claim(self, claim_id: UUID) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim(claim_id)

    async def get_narratives_by_claim_list(self, claim_id: UUID) -> list[NarrativeListItem]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim_list(claim_id)

    async def get_all_narratives(
        self,
        limit: int = 100,
        offset: int = 0,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end : datetime | None = None,
        language: str | None = None,
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit,
                offset=offset,
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            total = await repo.count_all_narratives(
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            return narratives, total

    async def get_all_narratives_list(
        self,
        limit: int = 100,
        offset: int = 0,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end: datetime | None = None,
        language: str | None = None,
    ) -> tuple[list[NarrativeListItem], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives_list(
                limit=limit,
                offset=offset,
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            total = await repo.count_all_narratives(
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            return narratives, total

    async def get_narratives_by_entity(
        self, entity_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit, offset=offset, entity_id=entity_id
            )
            total = await repo.count_all_narratives(entity_id=entity_id)
            return narratives, total

    async def update_narrative(
        self,
        narrative_id: UUID,
        data: NarrativePatchInput,
    ) -> Narrative | None:
        async with self.repo() as repo:

            existing_narrative = await repo.get_narrative(narrative_id)
            if not existing_narrative:
                return None

            merged_entity_ids = None
            if data.entities is not None:
                entity_service = EntityService(self._connection_factory)
                entity_ids = await entity_service.process_entities(data.entities)

                existing_entity_ids = [entity.id for entity in existing_narrative.entities]
                merged_entity_ids = list(set(existing_entity_ids + entity_ids))

            merged_claim_ids = data.claim_ids
            if data.claim_ids is not None:
                existing_claim_ids = [claim.id for claim in existing_narrative.claims]
                merged_claim_ids = list(set(existing_claim_ids + data.claim_ids))

            merged_topic_ids = data.topic_ids
            if data.topic_ids is not None:
                existing_topic_ids = [topic.id for topic in existing_narrative.topics]
                merged_topic_ids = list(set(existing_topic_ids + data.topic_ids))

            # Concatenate narrative_context with existing one
            merged_narrative_context = None
            if data.narrative_context is not None:
                merged_narrative_context = _merge_narrative_context(
                    existing_narrative.narrative_context,
                    data.narrative_context,
                )

            updated = await repo.update_narrative(
                narrative_id=narrative_id,
                title=data.title,
                description=data.description,
                narrative_context=merged_narrative_context,
                claim_ids=merged_claim_ids,
                topic_ids=merged_topic_ids,
                entity_ids=merged_entity_ids,
                metadata=data.metadata,
            )

        # Sync to external API after successful local update
        if updated:
            external_id = updated.metadata.get("narrative_id")
            if external_id and (data.title is not None or data.narrative_context is not None):
                await self._sync_external_narrative(
                    external_narrative_id=external_id,
                    title=updated.title,
                    narrative_context=updated.narrative_context if data.narrative_context is not None else None,
                )

        return updated

    async def delete_narrative(self, narrative_id: UUID) -> None:
        async with self.repo() as repo:
            narrative = await repo.get_narrative(narrative_id)
            if narrative and narrative.metadata.get("narrative_id"):
                await self._delete_external_narrative(
                    narrative.metadata["narrative_id"]
                )

            await repo.delete_narrative(narrative_id)

    async def _delete_external_narrative(self, external_narrative_id: str) -> None:
        """Delete a narrative from the external narratives API."""
        if not _api.is_configured():
            return

        response = await _api.delete_narrative(external_narrative_id)

        if response.status_code == 404:
            logger.info(
                f"Narrative {external_narrative_id} not found on external API, "
                "continuing with local delete"
            )
            return

        if response.status_code >= 400:
            logger.error(
                f"External API delete error: status={response.status_code}, "
                f"response={response.text}"
            )
            response.raise_for_status()

        logger.info(f"Deleted narrative {external_narrative_id} from external API")

    async def _sync_external_narrative(
        self,
        external_narrative_id: str,
        title: str,
        narrative_context: str | None = None,
    ) -> None:
        """Sync narrative fields to the external narratives API.

        Logs a warning on failure but does not raise.
        """
        if not _api.is_configured():
            return

        try:
            response = await _api.update_narrative(
                external_narrative_id,
                title=title,
                narrative_context=narrative_context,
            )

            if response.status_code >= 400:
                logger.warning(
                    f"External API sync error: status={response.status_code}, "
                    f"response={response.text}"
                )
            else:
                logger.info(
                    f"Synced narrative {external_narrative_id} to external API"
                )
        except Exception as e:
            logger.warning(
                f"Failed to sync narrative {external_narrative_id} to external API: {e}"
            )

    async def update_metadata(
        self, narrative_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.repo() as repo:
            updated = await repo.update_narrative(
                narrative_id=narrative_id,
                metadata=metadata,
            )
            if not updated:
                raise ValueError("narrative not found")
            return updated.metadata

    async def get_narratives_by_topic(
        self, topic_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_topic(
                topic_id, limit=limit, offset=offset
            )

    async def get_viral_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_viral_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[ViralNarrativeSummary]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[NarrativeSummary]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )
