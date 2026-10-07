import logging
from typing import AsyncContextManager
from uuid import UUID

from core.alerts.models import (
    DIGEST_SECTION_LIMIT,
    AlertRule,
    AlertRuleInput,
    DigestEntry,
    DigestItem,
    DigestNarrativeGroup,
    DigestSection,
    NarrativeOption,
)
from core.alerts.repo import AlertRepository
from core.alerts.summary import condition_summary
from core.alerts.validation import validate
from core.email import get_emailer
from core.email.messages import alert_digest_message
from core.errors import NotFoundError
from core.uow import ConnectionFactory, uow

log = logging.getLogger(__name__)


def _section(found: dict[UUID, DigestItem]) -> DigestSection:
    items = list(found.values())
    return DigestSection(items=items[:DIGEST_SECTION_LIMIT], total=len(items))


class AlertService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[AlertRepository]:
        return uow(AlertRepository, self._connection_factory)

    # The API: always the signed-in person's own alerts ----------------------------

    async def list_own(self, organisation_id: UUID, user_id: UUID) -> list[AlertRule]:
        async with self.repo() as repo:
            return await repo.list_own(organisation_id, user_id)

    async def get_own(self, organisation_id: UUID, user_id: UUID, alert_id: UUID) -> AlertRule:
        async with self.repo() as repo:
            alert = await repo.get_own(organisation_id, user_id, alert_id)
        if not alert:
            raise NotFoundError()
        return alert

    async def create(self, organisation_id: UUID, user_id: UUID, data: AlertRuleInput) -> AlertRule:
        name, conditions = validate(data)
        async with self.repo() as repo:
            alert_id = await repo.create(organisation_id, user_id, name, data.enabled, conditions)
            alert = await repo.get_own(organisation_id, user_id, alert_id)
        assert alert
        return alert

    async def replace(
        self, organisation_id: UUID, user_id: UUID, alert_id: UUID, data: AlertRuleInput
    ) -> AlertRule:
        name, conditions = validate(data)
        async with self.repo() as repo:
            current = await repo.get_own(organisation_id, user_id, alert_id)
            if not current:
                raise NotFoundError()
            before = [(c.type, c.narrative_id, c.filters) for c in current.conditions]
            after = [(c.type, c.narrative_id, c.filters) for c in conditions]
            # No backlog: new conditions, or enabling it again, start counting now
            restart = before != after or (data.enabled and not current.enabled)
            await repo.replace(alert_id, name, data.enabled, conditions, restart)
            alert = await repo.get_own(organisation_id, user_id, alert_id)
        assert alert
        return alert

    async def delete(self, organisation_id: UUID, user_id: UUID, alert_id: UUID) -> None:
        async with self.repo() as repo:
            if not await repo.get_own(organisation_id, user_id, alert_id):
                raise NotFoundError()
            await repo.delete(alert_id)

    async def narratives_matching(self, text: str, limit: int) -> list[NarrativeOption]:
        if len(text.strip()) < 2:
            return []
        async with self.repo() as repo:
            return [NarrativeOption(**row) for row in await repo.narratives_matching(text.strip(), limit)]

    # The daily e-mail -----------------------------------------------------------

    async def _evaluate(
        self, repo: AlertRepository, alert: AlertRule
    ) -> tuple[DigestEntry, list[dict]]:
        """What one alert reports now, sorted into the e-mail's sections, and the
        report rows to record once it has been sent. Each element appears once, with
        every condition it met; a claim of a followed narrative is listed under that
        narrative only."""
        since = await repo.counting_since(alert.id)
        narratives: dict[UUID, DigestItem] = {}
        claims: dict[UUID, DigestItem] = {}
        in_narrative: dict[UUID, dict[UUID, DigestItem]] = {}

        def add(found: dict[UUID, DigestItem], element_id: UUID, title: str, position: int) -> None:
            if element_id in found:
                found[element_id].conditions.append(position)
            else:
                found[element_id] = DigestItem(id=element_id, title=title, conditions=[position])

        for position, condition in enumerate(alert.conditions, start=1):
            for element_id, title in await repo.matches(alert.id, condition, since):
                if condition.type == "new_narrative":
                    add(narratives, element_id, title, position)
                elif condition.type == "new_claim_in_narrative" and condition.narrative_id:
                    add(in_narrative.setdefault(condition.narrative_id, {}), element_id, title, position)
                else:
                    add(claims, element_id, title, position)

        for group in in_narrative.values():
            for item in group.values():
                other = claims.pop(item.id, None)
                if other:
                    item.conditions = sorted(set(item.conditions) | set(other.conditions))

        titles = await repo.narrative_titles(list(in_narrative)) if in_narrative else {}
        entry = DigestEntry(
            alert_id=alert.id,
            alert_name=alert.name,
            narratives=_section(narratives),
            in_narratives=[
                DigestNarrativeGroup(narrative_id=nid, narrative_title=titles.get(nid, ""), **_section(group).model_dump())
                for nid, group in in_narrative.items()
            ],
            claims=_section(claims),
        )
        rows = [
            {"kind": "narrative", "id": i.id, "conditions": i.conditions, "section": "narratives", "in_narrative_id": None}
            for i in narratives.values()
        ]
        rows += [
            {"kind": "claim", "id": i.id, "conditions": i.conditions, "section": "in_narrative", "in_narrative_id": nid}
            for nid, group in in_narrative.items()
            for i in group.values()
        ]
        rows += [
            {"kind": "claim", "id": i.id, "conditions": i.conditions, "section": "claims", "in_narrative_id": None}
            for i in claims.values()
        ]
        return entry, rows

    async def digest_preview(self, organisation_id: UUID, user_id: UUID) -> list[DigestEntry]:
        """What the next e-mail would contain for this person, recording nothing."""
        async with self.repo() as repo:
            entries = []
            for alert in await repo.list_own(organisation_id, user_id):
                if alert.enabled:
                    entry, _ = await self._evaluate(repo, alert)
                    if entry.total:
                        entries.append(entry)
            return entries

    async def send_digests(self) -> tuple[int, int]:
        """One e-mail per person with their triggered alerts, newest first. An
        alert's reports are recorded in the same transaction as its e-mail is sent:
        if sending fails, nothing is recorded and the matches go out next time.
        Returns (e-mails sent, alerts triggered)."""
        async with self.repo() as repo:
            people = await repo.recipients_with_alerts()
        emailer = await get_emailer()
        sent = triggered = 0
        for person in people:
            try:
                async with self.repo() as repo:
                    alerts = [
                        a
                        for a in await repo.list_own(person["organisation_id"], person["user_id"])
                        if a.enabled
                    ]
                    entries, reports = [], []
                    for alert in alerts:
                        entry, rows = await self._evaluate(repo, alert)
                        if entry.total:
                            entries.append(entry)
                            reports.append((alert.id, rows))
                    if not entries:
                        continue
                    for alert_id, rows in reports:
                        await repo.record_reports(alert_id, rows)
                    names = await repo.names([c for a in alerts for c in a.conditions])
                    summaries = {a.id: [condition_summary(c, names) for c in a.conditions] for a in alerts}
                    subject, html = alert_digest_message(entries, summaries)
                    emailer.send(person["email"], subject, html)
                sent += 1
                triggered += len(entries)
            except Exception:
                # One person's failure doesn't stop the others; their matches stay
                # unrecorded and go out next time.
                log.exception("Failed to send the alert e-mail to user %s", person["user_id"])
        return sent, triggered
