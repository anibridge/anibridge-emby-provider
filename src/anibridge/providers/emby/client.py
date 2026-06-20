"""Direct Emby REST client consumed by the Emby provider."""

import base64
import importlib.metadata
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, cast
from urllib.parse import urlencode

import aiohttp
import msgspec
from anibridge.utils.datetime import normalize_local_datetime

from anibridge.providers.emby.models import (
    EmbyItem,
    EmbyItemPage,
    EmbySectionItemsPage,
    EmbyUser,
    EmbyVirtualFolder,
)

__all__ = ["EmbyClient"]

_EMBY_PACKAGE_NAME = "anibridge-emby-provider"
_SECTION_TYPES = frozenset({"movies", "tvshows"})
_TV_ITEM_TYPES = frozenset({"Series", "Season", "Episode"})


class _ContinueWatchingCacheEntry(msgspec.Struct, frozen=True):
    """Immutable Next Up deck cache entry with scoped item ids."""

    keys: frozenset[str]
    cached_at: datetime


class EmbyClient:
    """Small Emby REST client for provider-native reads."""

    ITEM_FIELDS: tuple[str, ...] = (
        "SortName",
        "DateCreated",
        "DateLastMediaAdded",
        "DateLastSaved",
        "ProviderIds",
        "ParentId",
    )

    _CONTINUE_CACHE_TTL = timedelta(seconds=55)

    def __init__(
        self,
        *,
        logger: Logger,
        url: str,
        token: str,
        user: str,
        section_filter: Sequence[str] | None = None,
        genre_filter: Sequence[str] | None = None,
    ) -> None:
        """Initialize the direct REST client."""
        self.log = logger
        self._base_url = url.rstrip("/")
        self._token = token
        self._user = user
        self._section_filter = set(section_filter or ())
        self._genre_filter = set(genre_filter or ())
        self._session: aiohttp.ClientSession | None = None
        self._user_id: str | None = None
        self._user_name: str | None = None
        self._server_version: str | None = None
        self._sections: list[EmbyItem] = []
        self._show_metadata_fetcher_order_by_section_id: dict[str, tuple[str, ...]] = {}
        self._continue_cache: dict[str, _ContinueWatchingCacheEntry] = {}

    async def initialize(self) -> None:
        """Authenticate and populate server metadata."""
        self._configure_client()
        self._server_version = await self._load_server_version()
        self._warn_if_unsupported_server_version()
        user = await self._resolve_user()

        if user.id is None:
            raise ValueError(f"Emby user has no id: {self._user}")
        self._user_id = user.id
        self._user_name = user.name or user.id
        self._sections = await self._load_sections()
        self._show_metadata_fetcher_order_by_section_id = (
            await self._load_show_metadata_fetcher_orders()
        )

    async def close(self) -> None:
        """Release held HTTP resources."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._user_id = None
        self._user_name = None
        self._server_version = None
        self._sections.clear()
        self._show_metadata_fetcher_order_by_section_id.clear()
        self.clear_cache()

    def user_id(self) -> str:
        """Get the Emby user id for the session."""
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._user_id

    def user_name(self) -> str:
        """Get the Emby user display name for the session."""
        if self._user_name is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._user_name

    def server_version(self) -> str | None:
        """Return the detected Emby server version, if available."""
        return self._server_version

    def auth_headers(self) -> dict[str, str]:
        """Get request headers for authenticated Emby calls."""
        return {"X-Emby-Token": self._token}

    def sections(self) -> Sequence[EmbyItem]:
        """Get cached media sections."""
        return tuple(self._sections)

    def show_metadata_fetchers_for_section(self, section_id: str) -> Sequence[str]:
        """Return enabled TV metadata fetchers for a section in priority order."""
        return self._show_metadata_fetcher_order_by_section_id.get(section_id, ())

    async def list_section_items(
        self,
        section: EmbyItem,
        *,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: Sequence[str] | None = None,
    ) -> Sequence[EmbyItem]:
        """Fetch items for a section with filtering applied."""
        items: list[EmbyItem] = []
        offset = 0
        page_size = 250
        while True:
            page = await self.list_section_items_page(
                section,
                offset=offset,
                limit=page_size,
                min_last_modified=normalize_local_datetime(min_last_modified),
                require_watched=require_watched,
                keys=keys,
            )
            items.extend(page.items)
            if page.next_offset is None:
                return tuple(items)
            offset = page.next_offset

    async def list_section_items_page(
        self,
        section: EmbyItem,
        *,
        offset: int,
        limit: int,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: Sequence[str] | None = None,
    ) -> EmbySectionItemsPage:
        """Fetch one bounded Emby section window."""
        if limit < 1:
            raise ValueError("Emby section page limit must be positive")

        normalized_min_last_modified = normalize_local_datetime(min_last_modified)
        is_movies = section.collection_type == "movies"
        if require_watched and not is_movies:
            items = await self._fetch_section_items(
                section,
                min_last_modified=normalized_min_last_modified,
                require_watched=require_watched,
                keys=keys,
            )
            next_offset = offset + limit if offset + limit < len(items) else None
            return EmbySectionItemsPage(
                items=tuple(items[offset : offset + limit]),
                next_offset=next_offset,
            )

        ids_filter = [key.strip() for key in keys or () if key.strip()]
        page = await self._query_user_items_page(
            include_item_types="Movie" if is_movies else "Series",
            parent_id=section.id,
            enable_user_data=True,
            enable_images=True,
            ids=ids_filter or None,
            is_played=True if require_watched else None,
            offset=offset,
            limit=limit,
        )
        items = self._filter_items_by_last_modified(
            page.items,
            normalized_min_last_modified,
        )
        total = page.total_record_count
        next_offset = (
            offset + limit
            if (total is not None and offset + limit < total)
            or (total is None and len(page.items) == limit)
            else None
        )
        return EmbySectionItemsPage(items=tuple(items), next_offset=next_offset)

    async def list_show_seasons(self, show_id: str) -> Sequence[EmbyItem]:
        """Return seasons for an Emby show."""
        return tuple(
            await self._query_user_items(
                parent_id=show_id,
                include_item_types="Season",
                enable_user_data=True,
                enable_images=True,
            )
        )

    async def list_show_episodes(
        self, *, show_id: str, season_id: str | None = None
    ) -> Sequence[EmbyItem]:
        """Return episodes for an Emby show."""
        return tuple(
            await self._query_user_items(
                parent_id=season_id or show_id,
                include_item_types="Episode",
                enable_user_data=True,
                enable_images=True,
            )
        )

    async def get_item(self, item_id: str) -> EmbyItem:
        """Fetch metadata for a single Emby item."""
        return await self._request(
            f"/Users/{self._require_user_id()}/Items/{item_id}",
            model=EmbyItem,
        )

    async def fetch_history(self, item: EmbyItem) -> Sequence[tuple[str, datetime]]:
        """Return play history tuples for an item."""
        if item.id is None:
            return ()

        if item.type in {"Season", "Series"}:
            episodes = await self.list_show_episodes(
                show_id=item.id,
                season_id=item.id if item.type == "Season" else None,
            )
            history: list[tuple[str, datetime]] = []
            for episode in episodes:
                if not episode.id:
                    continue
                last_played = normalize_local_datetime(
                    episode.user_data.last_played_date if episode.user_data else None
                )
                if last_played is not None:
                    history.append((episode.id, last_played))
            return tuple(history)

        last_played = normalize_local_datetime(
            item.user_data.last_played_date if item.user_data else None
        )
        return ((item.id, last_played),) if last_played is not None else ()

    async def is_on_continue_watching(self, section: EmbyItem, item: EmbyItem) -> bool:
        """Determine whether the item appears in Emby's Next Up deck."""
        if section.id is None or item.id is None or item.type not in _TV_ITEM_TYPES:
            return False

        now = datetime.now(tz=UTC)
        cache_entry = self._continue_cache.get(section.id)
        if self._continue_cache_needs_refresh(cache_entry, item, now):
            cache_entry = await self._load_continue_cache_entry(section.id, now)

        assert cache_entry is not None
        return item.id in cache_entry.keys

    @staticmethod
    def is_on_watchlist(item: EmbyItem) -> bool:
        """Determine whether the item is on the user's favorites list."""
        return bool(item.user_data.is_favorite if item.user_data else False)

    def build_image_url(
        self, item_id: str, *, image_type: str = "Primary", tag: str | None = None
    ) -> str:
        """Construct an image URL."""
        params = {"width": 92, "quality": 90, "api_key": self._token}
        if tag:
            params["tag"] = tag
        return (
            f"{self._base_url}/Items/{item_id}/Images/{image_type}?{urlencode(params)}"
        )

    def build_item_url(self, item_id: str) -> str:
        """Construct an Emby web URL for an item details page."""
        return f"{self._base_url}/web/index.html#!/item?{urlencode({'id': item_id})}"

    async def fetch_image_as_data_url(self, url: str) -> str:
        """Fetch an image through the authenticated session as a data URL."""
        session = self._require_session()
        async with session.get(url) as response:
            raw = await response.read()
            content_type = response.headers.get("Content-Type", "image/jpeg")
        encoded = base64.b64encode(raw).decode("ascii")
        return f"data:{content_type};base64,{encoded}"

    def clear_cache(self) -> None:
        """Clear cached metadata."""
        self._continue_cache.clear()

    def _configure_client(self) -> None:
        """Set up the HTTP session."""
        self._session = aiohttp.ClientSession(
            headers={
                "Accept": "application/json",
                "X-Emby-Token": self._token,
                "User-Agent": self._user_agent(),
            },
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=30),
        )

    async def _load_server_version(self) -> str | None:
        """Return the Emby server version reported by the server, if available."""
        try:
            info = await self._request("/System/Info", model=dict[str, object])
        except Exception:
            self.log.debug("Failed to load Emby server version", exc_info=True)
            return None

        version = info.get("Version")
        return str(version) if version else None

    def _warn_if_unsupported_server_version(self) -> None:
        """Warn when the detected Emby major version is not the supported one."""
        if not self._server_version:
            return
        match = re.match(r"^\s*(\d+)", self._server_version)
        if match is None or int(match.group(1)) != 4:
            self.log.warning(
                "Untested Emby server version detected: %s. The provider is only "
                "tested with major version 4.x, so unexpected behavior may occur.",
                self._server_version,
            )

    async def _resolve_user(self) -> EmbyUser:
        """Locate the configured Emby user by id or name."""
        users = await self._request("/Users", model=tuple[EmbyUser, ...])
        target = self._user.strip()
        if not target:
            raise ValueError("Emby provider requires a non-empty user value")

        for user in users:
            if user.id == target or user.name == target:
                return user

        raise ValueError(f"Unable to locate Emby user: {self._user}")

    async def _load_sections(self) -> list[EmbyItem]:
        """Fetch and filter media sections available to the user."""
        page = await self._request(
            f"/Users/{self._require_user_id()}/Views",
            params={"IncludeExternalContent": "false"},
            model=EmbyItemPage,
        )
        sections: list[EmbyItem] = []
        for section in page.items:
            if section.collection_type not in _SECTION_TYPES:
                continue
            if self._section_filter and section.name not in self._section_filter:
                continue
            sections.append(section)
        return sections

    async def _fetch_section_items(
        self,
        section: EmbyItem,
        *,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: Sequence[str] | None = None,
    ) -> list[EmbyItem]:
        """Fetch items from a section with optional filtering."""
        is_movies = section.collection_type == "movies"
        include_types = "Movie" if is_movies else "Series"
        ids_filter = [key.strip() for key in keys or () if key.strip()]

        if not require_watched or is_movies:
            return self._filter_items_by_last_modified(
                await self._query_user_items(
                    include_item_types=include_types,
                    parent_id=section.id,
                    enable_user_data=True,
                    enable_images=True,
                    ids=ids_filter or None,
                    is_played=True if require_watched else None,
                ),
                min_last_modified,
            )

        confirmed = await self._series_ids_with_activity(
            section=section,
            ids_filter=ids_filter or None,
            min_last_modified=min_last_modified,
        )
        if not confirmed:
            return []
        return await self._query_user_items(
            include_item_types=include_types,
            ids=confirmed,
            enable_user_data=True,
            enable_images=True,
        )

    async def _series_ids_with_activity(
        self,
        *,
        section: EmbyItem,
        ids_filter: Sequence[str] | None,
        min_last_modified: datetime | None,
    ) -> list[str]:
        """Return series ids with watched activity for a TV section."""
        if ids_filter is not None:
            raw = await self._query_user_items(
                include_item_types="Series,Season,Episode",
                ids=ids_filter,
            )
            series_to_check = {
                series_id
                for item in raw
                if (
                    series_id := (
                        item.id
                        if item.type == "Series"
                        else item.series_id or item.parent_id
                    )
                )
            }
            return [
                series_id
                for series_id in series_to_check
                if self._filter_items_by_last_modified(
                    await self._query_user_items(
                        include_item_types="Episode",
                        parent_id=series_id,
                        is_played=True,
                        limit=1,
                    ),
                    min_last_modified,
                )
            ]

        watched = await self._query_user_items(
            include_item_types="Episode",
            parent_id=section.id,
            is_played=True,
            min_date_last_saved_for_user=min_last_modified,
        )
        series_ids = {episode.series_id for episode in watched if episode.series_id}
        season_ids = {
            episode.parent_id
            for episode in watched
            if episode.parent_id and not episode.series_id
        }
        if season_ids:
            seasons = await self._query_user_items(
                include_item_types="Season",
                ids=tuple(season_ids),
            )
            series_ids |= {season.parent_id for season in seasons if season.parent_id}
        return list(series_ids)

    async def _query_user_items(
        self,
        *,
        include_item_types: str,
        parent_id: str | None = None,
        ids: Sequence[str] | None = None,
        is_played: bool | None = None,
        min_date_last_saved_for_user: datetime | None = None,
        enable_user_data: bool = False,
        enable_images: bool = False,
        limit: int | None = None,
    ) -> list[EmbyItem]:
        """Query Emby's generic user-items endpoint."""
        page = await self._query_user_items_page(
            include_item_types=include_item_types,
            parent_id=parent_id,
            ids=ids,
            is_played=is_played,
            min_date_last_saved_for_user=min_date_last_saved_for_user,
            enable_user_data=enable_user_data,
            enable_images=enable_images,
            limit=limit,
        )
        return list(page.items)

    async def _query_user_items_page(
        self,
        *,
        include_item_types: str,
        parent_id: str | None = None,
        ids: Sequence[str] | None = None,
        is_played: bool | None = None,
        min_date_last_saved_for_user: datetime | None = None,
        enable_user_data: bool = False,
        enable_images: bool = False,
        offset: int | None = None,
        limit: int | None = None,
    ) -> EmbyItemPage:
        """Query one Emby user-items page."""
        params: dict[str, object] = {
            "IncludeItemTypes": include_item_types,
            "Recursive": "true",
            "Fields": ",".join(self.ITEM_FIELDS),
            "ParentId": parent_id,
            "IsPlayed": "true" if is_played else None,
            "EnableUserData": "true" if enable_user_data else None,
            "EnableImages": "true" if enable_images else None,
            "Genres": "|".join(self._genre_filter) if self._genre_filter else None,
            "StartIndex": offset,
            "Limit": limit,
            "Ids": ",".join(ids) if ids is not None else None,
        }
        if min_date_last_saved_for_user is not None:
            params["MinDateLastSavedForUser"] = min_date_last_saved_for_user.isoformat()

        return await self._request(
            f"/Users/{self._require_user_id()}/Items",
            params=params,
            model=EmbyItemPage,
        )

    def _filter_items_by_last_modified(
        self, items: Sequence[EmbyItem], min_last_modified: datetime | None
    ) -> list[EmbyItem]:
        """Filter items by metadata and user activity timestamps."""
        if min_last_modified is None:
            return list(items)

        filtered: list[EmbyItem] = []
        for item in items:
            user_data = item.user_data
            timestamps = (
                item.date_last_saved,
                item.date_last_media_added,
                item.date_created,
                user_data.last_played_date if user_data else None,
            )
            normalized_timestamps = (
                normalize_local_datetime(value) for value in timestamps
            )
            if any(
                timestamp is not None and timestamp >= min_last_modified
                for timestamp in normalized_timestamps
            ):
                filtered.append(item)
        return filtered

    async def _load_show_metadata_fetcher_orders(self) -> dict[str, tuple[str, ...]]:
        """Get enabled TV metadata fetchers for each section in priority order."""
        folders = await self._request(
            "/Library/VirtualFolders",
            model=tuple[EmbyVirtualFolder, ...],
        )
        section_fetchers: dict[str, tuple[str, ...]] = {}
        for folder in folders:
            if not folder.item_id or folder.collection_type != "tvshows":
                continue

            type_options = (
                folder.library_options.type_options if folder.library_options else ()
            )
            for option in type_options:
                if option.type != "Series":
                    continue

                fetchers = self._metadata_fetchers(
                    option.metadata_fetcher_order,
                    option.metadata_fetchers,
                )
                if fetchers:
                    section_fetchers[folder.item_id] = fetchers
                    break
        return section_fetchers

    def _continue_cache_needs_refresh(
        self,
        cache_entry: _ContinueWatchingCacheEntry | None,
        item: EmbyItem,
        now: datetime,
    ) -> bool:
        """Return whether a section's cached Next Up deck should be refreshed."""
        if cache_entry is None:
            return True
        if cache_entry.cached_at + self._CONTINUE_CACHE_TTL <= now:
            return True

        user_data = item.user_data
        timestamps = (
            item.date_last_saved,
            item.date_last_media_added,
            item.date_created,
            user_data.last_played_date if user_data else None,
        )
        return any(
            timestamp is not None and timestamp > cache_entry.cached_at
            for timestamp in (normalize_local_datetime(value) for value in timestamps)
        )

    async def _load_continue_cache_entry(
        self, section_id: str, now: datetime
    ) -> _ContinueWatchingCacheEntry:
        """Load and cache the current Next Up deck for a section."""
        page = await self._request(
            "/Shows/NextUp",
            params={
                "UserId": self._require_user_id(),
                "EnableUserData": "false",
                "ParentId": section_id,
            },
            model=EmbyItemPage,
        )
        cache_entry = _ContinueWatchingCacheEntry(
            keys=frozenset(
                str(key)
                for item in page.items
                for key in (item.id, item.series_id, item.season_id)
                if key is not None
            ),
            cached_at=now,
        )
        self._continue_cache[section_id] = cache_entry
        return cache_entry

    def _require_user_id(self) -> str:
        """Return the initialized Emby user id."""
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._user_id

    async def _request[T](
        self,
        path: str,
        *,
        model: Any,
        params: Mapping[str, object] | None = None,
    ) -> T:
        """Issue an authenticated JSON request and decode it into `model`."""
        session = self._require_session()
        async with session.get(
            f"{self._base_url}{path}",
            params=self._query_params(params),
        ) as response:
            raw = await response.read()
        return cast(T, msgspec.json.decode(raw or b"{}", type=model))

    def _query_params(self, params: Mapping[str, object] | None) -> dict[str, str]:
        """Return query params in aiohttp's accepted shape."""
        query: dict[str, str] = {}
        for key, value in (params or {}).items():
            if value is None:
                continue
            query[key] = str(value)
        return query

    def _require_session(self) -> aiohttp.ClientSession:
        """Return the initialized HTTP session."""
        if self._session is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._session

    @staticmethod
    def _metadata_fetchers(
        ordered_fetchers: Sequence[str],
        enabled_fetchers: Sequence[str],
    ) -> tuple[str, ...]:
        """Return enabled metadata fetchers in configured priority order."""
        enabled = set(enabled_fetchers) if enabled_fetchers else None
        fetchers: list[str] = []
        for fetcher in (*ordered_fetchers, *enabled_fetchers):
            if not fetcher or fetcher in fetchers:
                continue
            if enabled is not None and fetcher not in enabled:
                continue
            fetchers.append(fetcher)
        return tuple(fetchers)

    @staticmethod
    def _user_agent() -> str:
        """Return the package user agent."""
        try:
            metadata = importlib.metadata.metadata(_EMBY_PACKAGE_NAME)
            name = metadata.get("Name", _EMBY_PACKAGE_NAME)
            version = importlib.metadata.version(_EMBY_PACKAGE_NAME)
        except importlib.metadata.PackageNotFoundError:
            name = _EMBY_PACKAGE_NAME
            version = "0"
        return f"{name}/{version}"
