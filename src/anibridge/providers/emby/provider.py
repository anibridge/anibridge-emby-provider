"""AniBridge provider implementation for Emby."""

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

import msgspec
from anibridge.provider.base import (
    Account,
    Artwork,
    Capabilities,
    Descriptor,
    ExternalId,
    Facet,
    FacetName,
    FieldSpec,
    Identifiers,
    InboundRequest,
    InboundResult,
    Metadata,
    Node,
    NodeChange,
    NodeFlag,
    NodeKind,
    NodeSpec,
    NumericConstraint,
    Page,
    Part,
    Progress,
    Provider,
    Rating,
    Record,
    RecordField,
    RecordKind,
    RecordSpec,
    Ref,
    Role,
    ScanItem,
    ScanQuery,
    State,
    Status,
    Step,
    Structure,
    SupportsInboundChanges,
    SupportsScan,
    Titles,
    Value,
)
from anibridge.utils.datetime import normalize_local_datetime

from anibridge.providers.emby.client import EmbyClient, EmbyItem
from anibridge.providers.emby.config import EmbyProviderConfig
from anibridge.providers.emby.webhook import EmbyWebhookEventType, WebhookParser

__all__ = ["EmbyProvider"]

_PROGRESS_KIND = "progress"
_PAGE_SIZE = 50
_TEMPORAL_FIELDS = frozenset(
    {
        RecordField.STARTED_AT,
        RecordField.FINISHED_AT,
        RecordField.LAST_ACTIVITY_AT,
    }
)
_ALL_RECORD_FIELDS = frozenset(RecordField)
_INBOUND_EVENTS = frozenset(
    {
        EmbyWebhookEventType.ITEM_MARK_FAVORITE,
        EmbyWebhookEventType.ITEM_MARK_PLAYED,
        EmbyWebhookEventType.ITEM_MARK_UNFAVORITE,
        EmbyWebhookEventType.ITEM_MARK_UNPLAYED,
        EmbyWebhookEventType.ITEM_RATE,
        EmbyWebhookEventType.LIBRARY_NEW,
        EmbyWebhookEventType.PLAYBACK_STOP,
    }
)

_PROVIDER_ID_MAP: dict[str, dict[str, str]] = {
    "movie": {
        "AniDB": "anidb",
        "AniList": "anilist",
        "Imdb": "imdb_movie",
        "Tmdb": "tmdb_movie",
        "Tvdb": "tvdb_movie",
    },
    "show": {
        "AniDB": "anidb",
        "AniList": "anilist",
        "Imdb": "imdb_show",
        "Tmdb": "tmdb_show",
        "Tvdb": "tvdb_show",
    },
}
_SHOW_FETCHER_TO_PROVIDER: dict[str, str] = {
    "AniDB": "anidb",
    "AniList": "anilist",
    "TheTVDB": "tvdb_show",
    "TheMovieDb": "tmdb_show",
    "IMDb": "imdb_show",
}


class _ScanCursor(msgspec.Struct, frozen=True):
    """Bulk scan position inside an Emby library section."""

    section: str
    offset: int


class EmbyProvider(Provider, SupportsScan, SupportsInboundChanges):
    """Emby source provider for the provider contract."""

    DISPLAY_NAME = "Emby"
    NAMESPACE = "emby"

    def __init__(
        self,
        *,
        logger,
        config: Mapping[str, object] | None = None,
    ) -> None:
        """Parse configuration and prepare the Emby client."""
        super().__init__(logger=logger, config=config)
        self.parsed_config = msgspec.convert(config or {}, type=EmbyProviderConfig)
        self._client = EmbyClient(
            logger=self.log,
            url=self.parsed_config.url,
            token=self.parsed_config.token,
            user=self.parsed_config.user,
            section_filter=self.parsed_config.sections,
            genre_filter=self.parsed_config.genres,
        )
        self._account: Account | None = None
        self._sections: tuple[EmbyItem, ...] = ()
        self._sections_by_key: dict[str, EmbyItem] = {}
        self._show_ids_by_section: dict[str, tuple[str, ...]] = {}

    async def initialize(self) -> None:
        """Connect to Emby and prepare provider state."""
        self.log.debug("Initializing Emby provider client")
        await self._client.initialize()
        self._account = Account(
            key=self._client.user_id(),
            title=self._client.user_name(),
        )
        self._sections = tuple(self._client.sections())
        self._sections_by_key = {
            str(section.id): section
            for section in self._sections
            if section.id is not None
        }
        self._show_ids_by_section = {}
        for section in self._sections:
            if section.collection_type != "tvshows":
                continue
            if section.id is None:
                continue
            provider_order = tuple(
                provider
                for metadata_fetcher in self._client.show_metadata_fetchers_for_section(
                    str(section.id)
                )
                if (provider := _SHOW_FETCHER_TO_PROVIDER.get(metadata_fetcher))
            )
            if provider_order:
                self._show_ids_by_section[str(section.id)] = provider_order
        self.log.debug(
            "Emby provider initialized for user id=%s with %s sections",
            self._account.key,
            len(self._sections),
        )

    def account(self) -> Account | None:
        """Return the connected Emby account."""
        return self._account

    def capabilities(self) -> Capabilities:
        """Advertise Emby source capabilities."""
        return Capabilities(
            roles=frozenset({Role.SOURCE}),
            facets=frozenset(
                {
                    FacetName.TITLES,
                    FacetName.ARTWORK,
                    FacetName.IDS,
                    FacetName.STRUCTURE,
                    FacetName.METADATA,
                }
            ),
            nodes=(
                NodeSpec(Descriptor("movie", NodeKind.FILM)),
                NodeSpec(
                    Descriptor("show", NodeKind.SERIES),
                    coordinate_axes=("season", "episode"),
                ),
                NodeSpec(
                    Descriptor("season", NodeKind.SEASON),
                    coordinate_axes=("episode",),
                ),
                NodeSpec(Descriptor("episode", NodeKind.EPISODE)),
            ),
            records=(
                RecordSpec(
                    kind=Descriptor(_PROGRESS_KIND, RecordKind.PROGRESS),
                    fields={
                        RecordField.STATUS: FieldSpec(
                            RecordField.STATUS,
                            readable=True,
                            values=(
                                Descriptor("watchlisted", Status.PLANNED),
                                Descriptor("watching", Status.ACTIVE),
                                Descriptor("completed", Status.COMPLETED),
                            ),
                        ),
                        RecordField.PROGRESS: FieldSpec(
                            RecordField.PROGRESS,
                            readable=True,
                        ),
                        RecordField.RATING: FieldSpec(
                            RecordField.RATING,
                            readable=True,
                            constraints=(NumericConstraint(0, 10, 0.5),),
                        ),
                        RecordField.STARTED_AT: FieldSpec(
                            RecordField.STARTED_AT,
                            readable=True,
                        ),
                        RecordField.FINISHED_AT: FieldSpec(
                            RecordField.FINISHED_AT,
                            readable=True,
                        ),
                        RecordField.LAST_ACTIVITY_AT: FieldSpec(
                            RecordField.LAST_ACTIVITY_AT,
                            readable=True,
                        ),
                        RecordField.REPEAT_COUNT: FieldSpec(
                            RecordField.REPEAT_COUNT,
                            readable=True,
                            constraints=(NumericConstraint(0, None, 1),),
                        ),
                    },
                ),
            ),
            external_authorities=frozenset(
                {
                    "anidb",
                    "anilist",
                    "imdb_movie",
                    "imdb_show",
                    "tmdb_movie",
                    "tmdb_show",
                    "tvdb_movie",
                    "tvdb_show",
                }
            ),
        )

    async def close(self) -> None:
        """Release Emby resources."""
        await self._client.close()
        self._sections = ()
        self._sections_by_key = {}
        self._show_ids_by_section = {}
        self.log.debug("Closed Emby provider")

    async def clear_cache(self) -> None:
        """Clear Emby provider caches."""
        self._client.clear_cache()

    async def scan(self, query: ScanQuery) -> Page[ScanItem]:
        """Scan Emby sections into contract nodes and records."""
        accepted_kinds = {"movie", "show"}
        if query.native_node_kinds:
            accepted_kinds &= set(query.native_node_kinds)
        include_records = query.include_records and (
            not query.native_record_kinds or _PROGRESS_KIND in query.native_record_kinds
        )
        if not accepted_kinds:
            return Page(items=(), total=0)

        page_limit = query.limit or _PAGE_SIZE
        if page_limit < 1:
            return Page(items=(), total=0)

        if query.sources:
            items = await self._scan_requested_refs(query, include_records)
            return Page(items=tuple(items), total=len(items))

        items: list[ScanItem] = []
        section_index = 0
        section_offset = 0
        if query.cursor:
            scan_cursor = msgspec.json.decode(query.cursor.encode(), type=_ScanCursor)
            section_offset = max(scan_cursor.offset, 0)
            for index, section in enumerate(self._sections):
                if str(section.id) == scan_cursor.section:
                    section_index = index
                    break
            else:
                raise ValueError(
                    f"Unknown Emby scan section cursor {scan_cursor.section!r}"
                )
        next_cursor: _ScanCursor | None = None

        while section_index < len(self._sections):
            section = self._sections[section_index]
            if not self._section_matches(section, accepted_kinds):
                section_index += 1
                section_offset = 0
                continue

            remaining = page_limit - len(items)
            if remaining <= 0:
                section_id = str(section.id) if section.id is not None else ""
                next_cursor = _ScanCursor(section_id, section_offset)
                break

            page = await self._client.list_section_items_page(
                section,
                offset=section_offset,
                limit=remaining,
                require_watched=query.require_user_data,
            )
            for raw_item in page.items:
                scan_item = await self._scan_item_for(
                    section,
                    raw_item,
                    query,
                    include_records,
                )
                if scan_item is None:
                    continue
                items.append(scan_item)
            if page.next_offset is not None:
                section_id = str(section.id) if section.id is not None else ""
                next_cursor = _ScanCursor(section_id, page.next_offset)
                break

            section_index += 1
            section_offset = 0
            if len(items) >= page_limit and section_index < len(self._sections):
                next_section = self._sections[section_index]
                section_id = str(next_section.id) if next_section.id is not None else ""
                next_cursor = _ScanCursor(section_id, 0)
                break

        return Page(
            items=tuple(items),
            cursor=msgspec.json.encode(next_cursor).decode()
            if next_cursor is not None
            else None,
            total=None if next_cursor is not None else len(items),
        )

    async def _scan_requested_refs(
        self,
        query: ScanQuery,
        include_records: bool,
    ) -> list[ScanItem]:
        items: list[ScanItem] = []
        for ref in query.sources:
            if not ref.is_anchor:
                continue
            try:
                raw_item = await self._client.get_item(ref.key)
            except Exception:
                self.log.warning("Invalid Emby media ref %s", ref.key)
                continue
            if raw_item.type in {"Episode", "Season"}:
                key = raw_item.series_id or raw_item.parent_id
                if key is None:
                    continue
                raw_item = await self._client.get_item(key)
            section = self._section_for_item(raw_item)
            if section is None:
                continue
            scan_item = await self._scan_item_for(
                section,
                raw_item,
                query,
                include_records,
            )
            if scan_item is not None:
                items.append(scan_item)
        return items

    async def _scan_item_for(
        self,
        section: EmbyItem,
        item: EmbyItem,
        query: ScanQuery,
        include_records: bool,
    ) -> ScanItem | None:
        item_kind = self._native_kind(item)
        if item_kind is None:
            return None
        if query.native_node_kinds and item_kind not in query.native_node_kinds:
            return None

        node = await self._node_for_item(section, item, item_kind, query.facets)
        records = (
            await self._records_for_item(section, item, query.record_fields)
            if include_records
            else ()
        )
        if (
            query.require_user_data
            and include_records
            and not any(record.values for record in records)
        ):
            return None
        return ScanItem(node=node, records=records)

    async def parse_inbound(self, request: InboundRequest) -> InboundResult:
        """Parse Emby webhook payloads into source refs."""
        if self._account is None:
            raise RuntimeError("Provider must be initialized before parsing webhooks")

        payload = WebhookParser.from_inbound(
            headers=request.headers,
            body=request.body,
        )
        if not payload.event_type or not payload.top_level_item_id:
            return InboundResult(matched=False)

        if payload.event_type not in _INBOUND_EVENTS:
            return InboundResult(matched=False)

        if payload.event_type is not EmbyWebhookEventType.LIBRARY_NEW:
            if payload.account_id is None:
                return InboundResult(matched=False)
            if payload.account_id != self._account.key:
                return InboundResult(matched=False)

        return InboundResult(
            matched=True,
            changes=(NodeChange(ref=Ref.anchor(payload.top_level_item_id)),),
        )

    async def _node_for_item(
        self,
        section: EmbyItem,
        item: EmbyItem,
        kind: str,
        facets: frozenset[FacetName],
    ) -> Node:
        flags = {NodeFlag.ANCHOR, NodeFlag.TRACKABLE}
        labels: list[str] = []
        if section.name:
            labels.append(str(section.name))
        if item.production_year:
            labels.append(str(item.production_year))
        if kind == "show":
            flags.update({NodeFlag.CONTAINER, NodeFlag.ORDERED_PARTS})

        hydrated: dict[FacetName, Facet] = {}
        if FacetName.TITLES in facets:
            hydrated[FacetName.TITLES] = Titles(primary=str(item.name or ""))
        if FacetName.ARTWORK in facets:
            artwork = await self._artwork_for_item(item)
            if artwork is not None:
                hydrated[FacetName.ARTWORK] = Artwork({"poster": artwork})
        if FacetName.IDS in facets:
            hydrated[FacetName.IDS] = Identifiers(self._external_ids(section, item))
        if FacetName.STRUCTURE in facets and kind == "show":
            hydrated[FacetName.STRUCTURE] = await self._structure_for_show(item)
        if FacetName.METADATA in facets:
            hydrated[FacetName.METADATA] = Metadata(
                {
                    "section": section.name,
                    "item_id": str(item.id),
                    "provider_ids": tuple(
                        f"{key}:{value}"
                        for key, value in (item.provider_ids or {}).items()
                        if value
                    ),
                }
            )

        return Node(
            ref=Ref.anchor(str(item.id)),
            kind=kind,
            title=str(item.name) if item.name is not None else None,
            url=self._client.build_item_url(str(item.id)) if item.id else None,
            labels=tuple(labels),
            flags=frozenset(flags),
            facets=hydrated,
        )

    async def _records_for_item(
        self,
        section: EmbyItem,
        item: EmbyItem,
        fields: frozenset[RecordField],
    ) -> tuple[Record, ...]:
        kind = self._native_kind(item)
        if kind == "movie":
            return (await self._record_for_movie(section, item, fields),)
        if kind == "show":
            return await self._records_for_show(section, item, fields)
        return ()

    async def _records_for_show(
        self,
        section: EmbyItem,
        item: EmbyItem,
        fields: frozenset[RecordField],
    ) -> tuple[Record, ...]:
        if item.id is None:
            return ()
        requested = fields or _ALL_RECORD_FIELDS
        show_ids = self._external_ids(section, item)
        season_refs = [
            (season, season.id)
            for season in await self._client.list_show_seasons(item.id)
            if season.id is not None and season.index_number is not None
        ]
        episode_sets = await asyncio.gather(
            *(
                self._client.list_show_episodes(show_id=item.id, season_id=season_id)
                for _season, season_id in season_refs
            )
        )
        records: list[Record] = []
        for (season, _season_id), raw_episodes in zip(
            season_refs,
            episode_sets,
            strict=True,
        ):
            episodes = tuple(raw_episodes)
            history_dates: tuple[datetime, ...] = ()
            if requested & _TEMPORAL_FIELDS:
                history_dates = tuple(
                    viewed_at.astimezone(UTC)
                    for episode in episodes
                    if (
                        viewed_at := normalize_local_datetime(
                            episode.user_data.last_played_date
                            if episode.user_data
                            else None
                        )
                    )
                    is not None
                )
            record = self._record_for_season(
                item,
                season,
                requested,
                episodes=episodes,
                history_dates=history_dates,
                show_ids=show_ids,
            )
            if record.values:
                records.append(record)
        return tuple(records)

    async def _record_for_movie(
        self,
        section: EmbyItem,
        item: EmbyItem,
        fields: frozenset[RecordField],
    ) -> Record:
        requested = fields or _ALL_RECORD_FIELDS
        values: dict[RecordField, Value] = {}
        user_data = item.user_data
        watched = 1 if user_data is not None and user_data.play_count else 0
        progress = Progress(current=watched, total=1, unit="movie")
        completed = watched >= 1

        if RecordField.STATUS in requested:
            status = await self._item_status(section, item, progress, completed)
            if status is not None:
                values[RecordField.STATUS] = status
        if RecordField.PROGRESS in requested and watched > 0:
            values[RecordField.PROGRESS] = progress
        if (
            RecordField.RATING in requested
            and user_data is not None
            and user_data.rating is not None
        ):
            values[RecordField.RATING] = Rating(
                float(user_data.rating),
                (0, 10, 0.5),
            )
        if RecordField.REPEAT_COUNT in requested:
            repeats = self._repeat_count((item,))
            if repeats:
                values[RecordField.REPEAT_COUNT] = repeats

        if requested & _TEMPORAL_FIELDS:
            history_dates = await self._history_dates(item)
            if history_dates:
                if RecordField.STARTED_AT in requested:
                    values[RecordField.STARTED_AT] = min(history_dates)
                if RecordField.LAST_ACTIVITY_AT in requested:
                    values[RecordField.LAST_ACTIVITY_AT] = max(history_dates)
                if completed and RecordField.FINISHED_AT in requested:
                    values[RecordField.FINISHED_AT] = max(history_dates)

        return Record(
            ref=Ref.anchor(str(item.id)),
            kind=_PROGRESS_KIND,
            key=str(item.id),
            ids=self._external_ids(section, item),
            values=values,
            url=self._client.build_item_url(str(item.id)) if item.id else None,
        )

    def _record_for_season(
        self,
        show: EmbyItem,
        season: EmbyItem,
        fields: frozenset[RecordField],
        *,
        episodes: tuple[EmbyItem, ...],
        history_dates: tuple[datetime, ...],
        show_ids: tuple[ExternalId, ...],
    ) -> Record:
        requested = fields or _ALL_RECORD_FIELDS
        values: dict[RecordField, Value] = {}
        watched = sum(
            1
            for episode in episodes
            if episode.user_data is not None and (episode.user_data.play_count or 0) > 0
        )
        progress = Progress(current=watched, total=len(episodes), unit="episode")
        completed = len(episodes) > 0 and watched >= len(episodes)

        if RecordField.STATUS in requested:
            status = self._show_status(show, progress, completed)
            if status is not None:
                values[RecordField.STATUS] = status
        if RecordField.PROGRESS in requested and watched > 0:
            values[RecordField.PROGRESS] = progress
        if RecordField.RATING in requested:
            rating = None
            if season.user_data is not None:
                rating = season.user_data.rating
            if rating is None and show.user_data is not None:
                rating = show.user_data.rating
            if rating is not None:
                values[RecordField.RATING] = Rating(float(rating), (0, 10, 0.5))
        if RecordField.REPEAT_COUNT in requested:
            repeats = self._repeat_count(episodes)
            if repeats:
                values[RecordField.REPEAT_COUNT] = repeats

        if requested & _TEMPORAL_FIELDS and history_dates:
            if RecordField.STARTED_AT in requested:
                values[RecordField.STARTED_AT] = min(history_dates)
            if RecordField.LAST_ACTIVITY_AT in requested:
                values[RecordField.LAST_ACTIVITY_AT] = max(history_dates)
            if completed and RecordField.FINISHED_AT in requested:
                values[RecordField.FINISHED_AT] = max(history_dates)

        index = int(season.index_number or 0)
        season_ref = Ref.anchor(str(show.id)).child("season", index)
        return Record(
            ref=season_ref,
            kind=_PROGRESS_KIND,
            key=f"{show.id}:s{index}",
            ids=self._season_external_ids(show_ids, index),
            values=values,
        )

    async def _item_status(
        self,
        section: EmbyItem,
        item: EmbyItem,
        progress: Progress,
        completed: bool,
    ) -> State | None:
        if completed:
            return State(native="completed", status=Status.COMPLETED)
        try:
            if await self._client.is_on_continue_watching(section, item):
                return State(native="watching", status=Status.ACTIVE)
        except Exception:
            self.log.debug("Unable to inspect Emby continue-watching state")
        if (progress.current or 0) > 0:
            return State(native="watching", status=Status.ACTIVE)
        if self._client.is_on_watchlist(item):
            return State(native="watchlisted", status=Status.PLANNED)
        if item.user_data is not None and item.user_data.rating is not None:
            return State(native="watching", status=Status.ACTIVE)
        return None

    def _show_status(
        self,
        show: EmbyItem,
        progress: Progress,
        completed: bool,
    ) -> State | None:
        if completed:
            return State(native="completed", status=Status.COMPLETED)
        if (progress.current or 0) > 0:
            return State(native="watching", status=Status.ACTIVE)
        if self._client.is_on_watchlist(show):
            return State(native="watchlisted", status=Status.PLANNED)
        if show.user_data is not None and show.user_data.rating is not None:
            return State(native="watching", status=Status.ACTIVE)
        return None

    @staticmethod
    def _repeat_count(items: Sequence[EmbyItem]) -> int:
        watched_count = min(
            (
                int(item.user_data.play_count or 0)
                for item in items
                if item.user_data is not None
            ),
            default=0,
        )
        return max(watched_count - 1, 0)

    async def _history_dates(self, item: EmbyItem) -> tuple[datetime, ...]:
        history = await self._client.fetch_history(item)
        dates = tuple(viewed_at.astimezone(UTC) for _key, viewed_at in history)
        if dates:
            return dates

        user_data = item.user_data
        fallback = normalize_local_datetime(
            user_data.last_played_date if user_data else None
        )
        return (fallback.astimezone(UTC),) if fallback is not None else ()

    def _external_ids(
        self,
        section: EmbyItem,
        item: EmbyItem,
    ) -> tuple[ExternalId, ...]:
        kind = self._native_kind(item)
        if kind is None:
            return ()
        mapping = _PROVIDER_ID_MAP[kind]
        ids: dict[str, ExternalId] = {}
        for provider_key, value in (item.provider_ids or {}).items():
            authority = mapping.get(provider_key)
            if authority is None or not value:
                continue
            scope = "R" if kind == "movie" and authority == "anidb" else None
            external_id = ExternalId(authority, str(value), scope)
            ids.setdefault(external_id.descriptor, external_id)
        external_ids = tuple(ids.values())
        if kind == "show":
            external_ids = self._ordered_show_ids(section, external_ids)
        return external_ids

    @staticmethod
    def _season_external_ids(
        show_ids: tuple[ExternalId, ...],
        season: int,
    ) -> tuple[ExternalId, ...]:
        ids: list[ExternalId] = []
        for item in show_ids:
            if item.scope is not None:
                continue
            if item.authority == "anilist":
                scope = None
            elif item.authority == "anidb":
                scope = "S" if season == 0 else "R"
            elif item.authority.endswith("_show"):
                scope = f"s{season}"
            else:
                continue
            ids.append(ExternalId(item.authority, item.value, scope))
        return tuple(ids)

    def _ordered_show_ids(
        self,
        section: EmbyItem,
        ids: tuple[ExternalId, ...],
    ) -> tuple[ExternalId, ...]:
        if section.id is None:
            return ids
        provider_order = self._show_ids_by_section.get(str(section.id), ())
        if self.parsed_config.strict:
            return (
                tuple(item for item in ids if item.authority == provider_order[0])
                if provider_order
                else ids
            )

        if not provider_order:
            return ids
        provider_rank = {provider: rank for rank, provider in enumerate(provider_order)}
        fallback_rank = len(provider_rank)
        return tuple(
            sorted(
                ids,
                key=lambda item: provider_rank.get(item.authority, fallback_rank),
            )
        )

    async def _structure_for_show(self, show: EmbyItem) -> Structure:
        if show.id is None:
            return Structure(axes=("season", "episode"))
        parts: list[Part] = []
        for episode in await self._client.list_show_episodes(show_id=show.id):
            position = (
                Step("season", int(episode.parent_index_number or 0)),
                Step("episode", int(episode.index_number or 0)),
            )
            parts.append(
                Part(
                    position=position,
                    title=str(episode.name) if episode.name is not None else None,
                    key=str(episode.id) if episode.id is not None else None,
                )
            )
        return Structure(axes=("season", "episode"), parts=tuple(parts))

    async def _artwork_for_item(self, item: EmbyItem) -> str | None:
        tags = item.image_tags or {}
        tag = tags.get("Primary")
        if item.id is None or not tag:
            return None
        try:
            url = self._client.build_image_url(str(item.id), tag=str(tag))
            return await self._client.fetch_image_as_data_url(url)
        except Exception:
            self.log.exception("Failed to fetch Emby poster")
            return None

    def _section_for_item(self, item: EmbyItem) -> EmbyItem | None:
        parent_id = str(item.parent_id) if item.parent_id is not None else None
        if parent_id is not None and parent_id in self._sections_by_key:
            return self._sections_by_key[parent_id]
        kind = self._native_kind(item)
        for section in self._sections:
            if kind == "movie" and section.collection_type == "movies":
                return section
            if kind == "show" and section.collection_type == "tvshows":
                return section
        return None

    @staticmethod
    def _section_matches(section: EmbyItem, accepted_kinds: set[str]) -> bool:
        if section.collection_type == "movies":
            return "movie" in accepted_kinds
        if section.collection_type == "tvshows":
            return "show" in accepted_kinds
        return False

    @staticmethod
    def _native_kind(item: EmbyItem) -> str | None:
        if item.type == "Movie":
            return "movie"
        if item.type == "Series":
            return "show"
        return None
