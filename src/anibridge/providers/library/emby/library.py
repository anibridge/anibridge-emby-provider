"""Emby library provider implementation."""

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from anibridge.library import (
    HistoryEntry,
    LibraryEntry,
    LibraryEpisode,
    LibraryMedia,
    LibraryMovie,
    LibraryProvider,
    LibrarySeason,
    LibrarySection,
    LibraryShow,
    LibraryUser,
    MediaKind,
)
from anibridge.library.base import MappingDescriptor
from anibridge.utils.cache import cache, ttl_cache
from anibridge.utils.image import fetch_image_as_data_url
from anibridge.utils.types import ProviderLogger
from emby_client.models.base_item_dto import BaseItemDto

from anibridge.providers.library.emby.client import EmbyClient
from anibridge.providers.library.emby.config import EmbyProviderConfig
from anibridge.providers.library.emby.webhook import EmbyWebhookEventType, WebhookParser

if TYPE_CHECKING:
    from starlette.requests import Request

_PROVIDER_ID_MAP = {
    "movie": {
        "anidb": "anidb",
        "anilist": "anilist",
        "imdb": "imdb_movie",
        "tmdb": "tmdb_movie",
        "tvdb": "tvdb_movie",
    },
    "show": {
        "anidb": "anidb",
        "anilist": "anilist",
        "imdb": "imdb_show",
        "tmdb": "tmdb_show",
        "tvdb": "tvdb_show",
    },
}

_STRICT_FETCHER_TO_PROVIDER = {
    "AniDB": "anidb",
    "AniList": "anilist",
    "TheTVDB": "tvdb_show",
    "TheMovieDb": "tmdb_show",
    "IMDb": "imdb_show",
}


class EmbyLibrarySection(LibrarySection["EmbyLibraryProvider"]):
    """Concrete `LibrarySection` backed by an Emby view."""

    def __init__(self, provider: EmbyLibraryProvider, item: BaseItemDto) -> None:
        """Represent an Emby library section."""
        self._provider = provider
        self._section = item
        self._key = str(item.id)
        self._title = str(item.name)
        collection = (item.collection_type or "").lower()
        self._media_kind = (
            MediaKind.SHOW if collection == "tvshows" else MediaKind.MOVIE
        )


class EmbyLibraryMedia(LibraryMedia["EmbyLibraryProvider"]):
    """Base class for Emby media objects (metadata focused)."""

    def __init__(
        self,
        provider: EmbyLibraryProvider,
        section: EmbyLibrarySection,
        item: BaseItemDto,
        kind: MediaKind,
    ) -> None:
        """Initialize the media wrapper."""
        self._provider = provider
        self._section = section
        self._item = item
        self._media_kind = kind
        self._key = str(item.id)
        self._title = str(item.name)

    @property
    def poster_image(self) -> str | None:
        """Return a base64 data URL for the item's poster artwork if available."""
        tags = self._item.image_tags or {}
        tag = tags.get("Primary") if isinstance(tags, dict) else None
        if not tag and isinstance(tags, dict):
            tag = tags.get("primary")
        if not tag or self._item.id is None:
            return None

        try:
            url = self._provider._client.build_image_url(
                str(self._item.id), tag=str(tag)
            )
            return fetch_image_as_data_url(url, timeout=3)
        except Exception:
            self._provider.log.error("Failed to fetch Emby poster")
            return None

    @property
    def external_url(self) -> str | None:
        """URL to the Emby page, if available."""
        item_id = self._item.id
        if item_id is None:
            return None
        return self._provider._client.build_item_url(str(item_id))


class EmbyLibraryEntry(LibraryEntry["EmbyLibraryProvider"]):
    """Common behaviour for Emby-backed library objects."""

    def __init__(
        self,
        provider: EmbyLibraryProvider,
        section: EmbyLibrarySection,
        item: BaseItemDto,
        kind: MediaKind,
    ) -> None:
        """Initialize the entry wrapper."""
        self._provider = provider
        self._section = section
        self._item = item
        self._media_kind = kind
        self._media = EmbyLibraryMedia(provider, section, item, kind)
        self._key = str(item.id)
        self._title = str(item.name)

    def mapping_descriptors(self) -> Sequence[MappingDescriptor]:
        """Return mapping descriptors for this media item."""
        provider_ids = self._item.provider_ids or {}
        media_key = "show" if self._media_kind == MediaKind.SHOW else "movie"
        mapping = _PROVIDER_ID_MAP.get(media_key, {})
        descriptors: list[MappingDescriptor] = []

        for provider_key, value in provider_ids.items():
            if not value:
                continue
            normalized = str(provider_key).lower()
            mapped = mapping.get(normalized)
            if not mapped:
                continue
            descriptors.append((mapped, str(value), None))

        if self._media_kind == MediaKind.SHOW and self._provider.parsed_config.strict:
            required_provider = self._provider._strict_show_provider_by_section.get(
                self._section.key
            )
            if not required_provider:
                return ()
            descriptors = [
                descriptor
                for descriptor in descriptors
                if descriptor[0] == required_provider
            ]

        return tuple(descriptors)

    @property
    def on_watching(self) -> bool:
        """Check if the media item is on the user's current watching list."""
        return self._provider.is_on_continue_watching(
            self._section._section,
            self._item,
        )

    @property
    def on_watchlist(self) -> bool:
        """Check if the media item is on the user's watchlist."""
        return self._provider.is_on_watchlist(self._item)

    @property
    def user_rating(self) -> int | None:
        """Return the user rating for this media item on a 0-100 scale."""
        user_data = self._item.user_data
        rating = user_data.rating if user_data else None
        if rating is None:
            return None
        return round(float(rating) * 10)

    @property
    def view_count(self) -> int:
        """Return the number of times this media item has been viewed."""
        user_data = self._item.user_data
        if user_data is None or not user_data.played:
            return 0
        return user_data.play_count or 1

    async def history(self) -> Sequence[HistoryEntry]:
        """Fetch the viewing history for this media item."""
        entries = await self._provider.get_history(self._item)
        return entries

    def media(self) -> EmbyLibraryMedia:
        """Return the media metadata for this item."""
        return self._media

    @property
    async def review(self) -> str | None:
        """Return the user's review text for this item, if any."""
        return None

    def section(self) -> EmbyLibrarySection:
        """Return the library section this media item belongs to."""
        return self._section


class EmbyLibraryMovie(EmbyLibraryEntry, LibraryMovie["EmbyLibraryProvider"]):
    """Concrete `LibraryMovie` wrapper for Emby movie objects."""

    def __init__(
        self,
        provider: EmbyLibraryProvider,
        section: EmbyLibrarySection,
        item: BaseItemDto,
    ) -> None:
        """Initialize the movie wrapper."""
        super().__init__(provider, section, item, MediaKind.MOVIE)

    def mapping_descriptors(self) -> Sequence[MappingDescriptor]:
        """Return mapping descriptors for this movie, with no scope."""
        descriptors: list[MappingDescriptor] = []
        for descriptor in super().mapping_descriptors():
            if descriptor[0] == "anidb":
                descriptors.append((descriptor[0], descriptor[1], "R"))
            else:
                descriptors.append(descriptor)
        return tuple(descriptors)


class EmbyLibraryShow(EmbyLibraryEntry, LibraryShow["EmbyLibraryProvider"]):
    """Concrete `LibraryShow` wrapper for Emby series objects."""

    def __init__(
        self,
        provider: EmbyLibraryProvider,
        section: EmbyLibrarySection,
        item: BaseItemDto,
    ) -> None:
        """Initialize the show wrapper."""
        super().__init__(provider, section, item, MediaKind.SHOW)

    def episodes(self) -> Sequence[EmbyLibraryEpisode]:
        """Return all episodes belonging to the show."""
        if self._item.id is None:
            return ()
        seasons = self.seasons()
        return tuple(episode for season in seasons for episode in season.episodes())

    @ttl_cache(ttl=15, maxsize=1)
    def seasons(self) -> Sequence[EmbyLibrarySeason]:
        """Return all seasons belonging to the show."""
        if self._item.id is None:
            return ()
        seasons = self._provider._client.list_show_seasons(show_id=self._item.id)
        return tuple(
            EmbyLibrarySeason(self._provider, self._section, season, show=self)
            for season in seasons
        )


class EmbyLibrarySeason(EmbyLibraryEntry, LibrarySeason["EmbyLibraryProvider"]):
    """Concrete `LibrarySeason` wrapper for Emby season objects."""

    def __init__(
        self,
        provider: EmbyLibraryProvider,
        section: EmbyLibrarySection,
        item: BaseItemDto,
        *,
        show: EmbyLibraryShow | None = None,
    ) -> None:
        """Initialize the season wrapper."""
        super().__init__(provider, section, item, MediaKind.SEASON)
        self._show = show
        self.index = int(item.index_number or 0)

    @ttl_cache(ttl=15, maxsize=1)
    def episodes(self) -> Sequence[EmbyLibraryEpisode]:
        """Return the episodes belonging to this season."""
        show_id = self._item.series_id or self._item.parent_id
        if show_id is None or self._item.id is None:
            return ()
        episodes = self._provider._client.list_show_episodes(
            show_id=show_id,
            season_id=self._item.id,
        )
        return tuple(
            EmbyLibraryEpisode(
                self._provider, self._section, episode, season=self, show=self._show
            )
            for episode in episodes
        )

    @cache
    def show(self) -> EmbyLibraryShow:
        """Return the parent show."""
        if self._show is not None:
            return self._show
        show_id = self._item.series_id or self._item.parent_id
        if show_id is None:
            raise RuntimeError("Season is missing SeriesId")
        raw_show = self._provider._client.get_item(show_id)
        self._show = EmbyLibraryShow(self._provider, self._section, raw_show)
        return self._show

    def mapping_descriptors(self) -> Sequence[MappingDescriptor]:
        """Return mapping descriptors with season scopes applied."""
        descriptors: list[MappingDescriptor] = []
        for provider, entry_id, _ in self.show().mapping_descriptors():
            scope: str | None = f"s{self.index}"
            if provider == "anilist":
                scope = None
            elif provider == "anidb":
                scope = "S" if self.index == 0 else "R"
            descriptors.append((provider, entry_id, scope))
        return tuple(descriptors)


class EmbyLibraryEpisode(EmbyLibraryEntry, LibraryEpisode["EmbyLibraryProvider"]):
    """Concrete `LibraryEpisode` wrapper for Emby episode objects."""

    def __init__(
        self,
        provider: EmbyLibraryProvider,
        section: EmbyLibrarySection,
        item: BaseItemDto,
        *,
        season: EmbyLibrarySeason | None = None,
        show: EmbyLibraryShow | None = None,
    ) -> None:
        """Initialize the episode wrapper."""
        super().__init__(provider, section, item, MediaKind.EPISODE)
        self._season = season
        self._show = show
        self.index = int(item.index_number or 0)
        parent_index_number = item.parent_index_number
        if parent_index_number is None and season is not None:
            parent_index_number = season.index
        self.season_index = int(parent_index_number or 0)

    @cache
    def season(self) -> EmbyLibrarySeason:
        """Return the parent season."""
        if self._season is not None:
            return self._season
        season_id = self._item.season_id or self._item.parent_id
        if season_id is None:
            raise RuntimeError("Episode is missing SeasonId")
        raw_season = self._provider._client.get_item(season_id)
        self._season = EmbyLibrarySeason(
            self._provider,
            self._section,
            raw_season,
            show=self._show,
        )
        return self._season

    @cache
    def show(self) -> EmbyLibraryShow:
        """Return the parent show."""
        if self._show is not None:
            return self._show
        show_id = self._item.series_id
        if show_id is None:
            self._show = self.season().show()
            return self._show
        raw_show = self._provider._client.get_item(show_id)
        self._show = EmbyLibraryShow(self._provider, self._section, raw_show)
        return self._show

    def mapping_descriptors(self) -> Sequence[MappingDescriptor]:
        """Return mapping descriptors with season scopes applied."""
        return self.season().mapping_descriptors()


class EmbyLibraryProvider(LibraryProvider):
    """Default Emby `LibraryProvider` backed by an Emby server."""

    NAMESPACE = "emby"

    def __init__(self, *, logger: ProviderLogger, config: dict | None = None) -> None:
        """Parse configuration and prepare provider defaults."""
        super().__init__(logger=logger, config=config)
        self.parsed_config = EmbyProviderConfig.model_validate(config or {})
        self._client = self._create_client()
        self._user: LibraryUser | None = None
        self._sections: list[EmbyLibrarySection] = []
        self._section_map: dict[str, EmbyLibrarySection] = {}
        self._strict_show_provider_by_section: dict[str, str] = {}

    async def initialize(self) -> None:
        """Connect to Emby and prepare provider state."""
        self.log.debug("Initializing Emby provider client")
        await self._client.initialize()
        self._user = LibraryUser(
            key=self._client.user_id(),
            title=self._client.user_name(),
        )
        self._sections = self._build_sections()
        self._strict_show_provider_by_section.clear()

        if self.parsed_config.strict:
            for section in self._sections:
                if section.media_kind != MediaKind.SHOW:
                    continue
                metadata_fetcher = self._client.show_metadata_fetcher_for_section(
                    section.key
                )
                if not metadata_fetcher:
                    continue
                if provider := _STRICT_FETCHER_TO_PROVIDER.get(metadata_fetcher):
                    self._strict_show_provider_by_section[section.key] = provider

        self.log.debug(
            "Emby provider initialized for user id=%s with %s sections",
            self._user.key,
            len(self._sections),
        )

    async def close(self) -> None:
        """Release any resources held by the provider."""
        self.log.debug("Closing Emby provider")
        await self._client.close()
        self._sections.clear()
        self._section_map.clear()
        self._strict_show_provider_by_section.clear()
        self.log.debug("Closed Emby provider")

    def user(self) -> LibraryUser | None:
        """Return the Emby user represented by this provider."""
        return self._user

    async def get_sections(self) -> Sequence[LibrarySection]:
        """Enumerate Emby library sections visible to the provider user."""
        return tuple(self._sections)

    async def list_items(
        self,
        section: LibrarySection,
        *,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: Sequence[str] | None = None,
    ) -> Sequence[LibraryEntry]:
        """List items in an Emby library section matching the provided criteria."""
        if not isinstance(section, EmbyLibrarySection):
            self.log.warning(
                "Emby list_items received an incompatible section instance"
            )
            raise TypeError(
                "Emby providers expect section objects created by the provider"
            )

        raw_items = await self._client.list_section_items(
            section._section,
            min_last_modified=min_last_modified,
            require_watched=require_watched,
            keys=keys,
        )
        return tuple(self._wrap_entry(section, item) for item in raw_items)

    async def parse_webhook(self, request: Request) -> tuple[bool, Sequence[str]]:
        """Parse an Emby webhook request and determine affected media items."""
        payload = await WebhookParser.from_request(request)

        if not payload.event_type:
            self.log.warning("Webhook: No supported event type found in payload")
            raise ValueError("No supported event type found in webhook payload")

        if not payload.top_level_item_id:
            self.log.warning("Webhook: No item ID found in payload")
            raise ValueError("No item ID found in webhook payload")

        sync_events = {
            EmbyWebhookEventType.ITEM_MARK_FAVORITE,
            EmbyWebhookEventType.ITEM_MARK_PLAYED,
            EmbyWebhookEventType.ITEM_MARK_UNFAVORITE,
            EmbyWebhookEventType.ITEM_MARK_UNPLAYED,
            EmbyWebhookEventType.ITEM_RATE,
            EmbyWebhookEventType.LIBRARY_NEW,
            EmbyWebhookEventType.PLAYBACK_STOP,
        }

        event_type = payload.event_type
        if event_type not in sync_events:
            self.log.debug("Webhook: Ignoring event type %s", event_type)
            return (False, tuple())

        if event_type != EmbyWebhookEventType.LIBRARY_NEW:
            if not self._user:
                self.log.warning("Webhook: Provider user has not been initialized")
                return (False, tuple())

            if not payload.account_id:
                self.log.debug(
                    "Webhook: Ignoring event %s with no account id",
                    event_type,
                )
                return (False, tuple())

            if payload.account_id.lower() != self._user.key.lower():
                self.log.debug(
                    "Webhook: Ignoring event %s for user ID %s",
                    event_type,
                    payload.account_id,
                )
                return (False, tuple())

        self.log.debug(
            "Webhook: Matched webhook event %s for sync key %s",
            event_type,
            payload.top_level_item_id,
        )
        return (True, (payload.top_level_item_id,))

    async def clear_cache(self) -> None:
        """Reset any cached Emby responses maintained by the provider."""
        self._client.clear_cache()
        # Note this clears the class level caches, which will be a no-op since the
        # caches used here are instance level. However, I'm leaving this in place
        # in case anibridge-utils caches support instance level cache clearing from
        # the class call in the future.
        EmbyLibraryShow.seasons.cache_clear()
        EmbyLibrarySeason.episodes.cache_clear()

    def is_on_continue_watching(self, section: BaseItemDto, item: BaseItemDto) -> bool:
        """Determine whether the given item appears in Continue Watching."""
        return self._client.is_on_continue_watching(section, item)

    def is_on_watchlist(self, item: BaseItemDto) -> bool:
        """Determine whether the given item appears in the user's favorites list."""
        return self._client.is_on_watchlist(item)

    async def get_history(self, item: BaseItemDto) -> Sequence[HistoryEntry]:
        """Return the watch history for the given Emby item."""
        history = await self._client.fetch_history(item)
        return tuple(
            HistoryEntry(library_key=entry_id, viewed_at=timestamp)
            for entry_id, timestamp in history
        )

    def _build_sections(self) -> list[EmbyLibrarySection]:
        """Construct the list of Emby library sections available to the user."""
        sections: list[EmbyLibrarySection] = []
        self._section_map.clear()

        for raw in self._client.sections():
            wrapper = EmbyLibrarySection(self, raw)
            self._section_map[wrapper.key] = wrapper
            sections.append(wrapper)
        return sections

    def _wrap_entry(
        self, section: EmbyLibrarySection, item: BaseItemDto
    ) -> EmbyLibraryEntry:
        """Wrap an Emby item in the appropriate library entry class."""
        item_type = (item.type or "").lower()
        if item_type == "episode":
            return EmbyLibraryEpisode(self, section, item)
        if item_type == "season":
            return EmbyLibrarySeason(self, section, item)
        if item_type == "series":
            return EmbyLibraryShow(self, section, item)
        if item_type == "movie":
            return EmbyLibraryMovie(self, section, item)
        raise TypeError(f"Unsupported Emby media type: {item.type!r}")

    def _create_client(self) -> EmbyClient:
        """Construct and return an Emby client for this provider."""
        return EmbyClient(
            logger=self.log,
            url=self.parsed_config.url,
            token=self.parsed_config.token,
            user=self.parsed_config.user,
            section_filter=self.parsed_config.sections,
            genre_filter=self.parsed_config.genres,
        )
