"""Emby client abstractions consumed by the library provider."""

import asyncio
import importlib.metadata
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from urllib.parse import urlencode

import emby_client
from anibridge.utils.datetime import normalize_local_datetime
from anibridge.utils.types import ProviderLogger
from emby_client.models.base_item_dto import BaseItemDto
from emby_client.models.user_item_data_dto import UserItemDataDto

__all__ = ["EmbyClient"]


@dataclass(frozen=True, slots=True)
class _FrozenCacheEntry:
    """Immutable Next Up deck cache entry with a creation timestamp."""

    keys: frozenset[str]
    cached_at: datetime


class EmbyClient:
    """High-level Emby client wrapper used by the library provider."""

    ITEM_FIELDS: tuple[str, ...] = (
        "SortName",
        "DateCreated",
        "ProviderIds",
        "ParentId",
    )

    _CONTINUE_CACHE_TTL = timedelta(seconds=55)

    def __init__(
        self,
        *,
        logger: ProviderLogger,
        url: str,
        token: str,
        user: str,
        section_filter: Sequence[str] | None = None,
        genre_filter: Sequence[str] | None = None,
    ) -> None:
        """Initialize the client wrapper.

        Args:
            logger (ProviderLogger): Logger for client operations.
            url (str): Base URL of the Emby server.
            token (str): API token for authentication.
            user (str): User identifier (id or name) to operate as.
            section_filter (Sequence[str] | None): Optional list of section names to
                include.
            genre_filter (Sequence[str] | None): Optional list of genres to include.
        """
        self.log = logger
        self._url = url
        self._token = token
        self._user = user
        self._section_filter = {value.lower() for value in section_filter or ()}
        self._genre_filter = {value.lower() for value in genre_filter or ()}

        self._api_client: emby_client.ApiClient | None = None
        self._items_api: emby_client.ItemsServiceApi | None = None
        self._user_api: emby_client.UserServiceApi | None = None
        self._user_library_api: emby_client.UserLibraryServiceApi | None = None
        self._user_views_api: emby_client.UserViewsServiceApi | None = None
        self._library_structure_api: emby_client.LibraryStructureServiceApi | None = (
            None
        )
        self._tv_shows_api: emby_client.TvShowsServiceApi | None = None
        self._user_id: str | None = None
        self._user_name: str | None = None
        self._base_url = url.rstrip("/")
        self._sections: list[BaseItemDto] = []
        self._show_metadata_fetcher_by_section_id: dict[str, str] = {}
        self._continue_cache: dict[str, _FrozenCacheEntry] = {}

    async def initialize(self) -> None:
        """Authenticate and populate server metadata."""
        await asyncio.to_thread(self._configure_client)
        user = await asyncio.to_thread(self._resolve_user)

        self._user_id = str(user.id)
        self._user_name = str(user.name or user.id)
        self._sections = await asyncio.to_thread(self._load_sections)
        self._show_metadata_fetcher_by_section_id = await asyncio.to_thread(
            self._load_show_metadata_fetchers
        )

    async def close(self) -> None:
        """Release any held resources."""
        self._api_client = None
        self._items_api = None
        self._user_api = None
        self._user_library_api = None
        self._user_views_api = None
        self._library_structure_api = None
        self._tv_shows_api = None
        self._user_id = None
        self._user_name = None
        self._sections.clear()
        self._show_metadata_fetcher_by_section_id.clear()
        self.clear_cache()

    def user_id(self) -> str:
        """Get the Emby user id for the session.

        Returns:
            str: The Emby user id.
        """
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._user_id

    def user_name(self) -> str:
        """GET the Emby user display name for the session.

        Returns:
            str: The Emby user display name.
        """
        if self._user_name is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._user_name

    def auth_headers(self) -> dict[str, str]:
        """Get request headers for authenticated Emby calls.

        Returns:
            dict[str, str]: Headers including the API token for authentication.
        """
        return {"X-Emby-Token": self._token}

    def sections(self) -> Sequence[BaseItemDto]:
        """Get the cached Emby library sections.

        Returns:
            Sequence[BaseItemDto]: The library sections available to the user.
        """
        return tuple(self._sections)

    def show_metadata_fetcher_for_section(self, section_id: str) -> str | None:
        """Return the top-priority TV metadata fetcher for a section if known.

        Args:
            section_id (str): The Emby section id.

        Returns:
            str | None: The metadata fetcher name or None if not known.
        """
        return self._show_metadata_fetcher_by_section_id.get(section_id)

    async def list_section_items(
        self,
        section: BaseItemDto,
        *,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: Sequence[str] | None = None,
    ) -> Sequence[BaseItemDto]:
        """Fetch Emby items for the provided section with filtering applied.

        Args:
            section (BaseItemDto): The Emby library section to fetch items from.
            min_last_modified (datetime | None): Optional minimum last modified
                timestamp to filter items by.
            require_watched (bool): If true, only include items with user data
                indicating they have been watched.
            keys (Sequence[str] | None): Optional list of item ids to include
                (after other filters are applied).

        Returns:
            Sequence[BaseItemDto]: The filtered list of items from the section.
        """
        items = await asyncio.to_thread(
            self._fetch_section_items,
            section,
            min_last_modified=normalize_local_datetime(min_last_modified),
            require_watched=require_watched,
            keys=keys,
        )
        filtered = list(items)

        if keys is not None:
            normalized_keys = {str(key) for key in keys}
            filtered = [
                item
                for item in filtered
                if item.id is not None and item.id in normalized_keys
            ]

        return tuple(filtered)

    def list_show_seasons(self, show_id: str) -> Sequence[BaseItemDto]:
        """Return the seasons for an Emby show.

        Args:
            show_id (str): The Emby item id of the show to list seasons for.

        Returns:
            Sequence[BaseItemDto]: The seasons for the specified show.
        """
        if self._items_api is None:
            raise RuntimeError("Emby client has not been initialized")
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")

        response = self._items_api.get_users_by_userid_items(
            self._user_id,
            parent_id=show_id,
            include_item_types="Season",
            recursive=True,
            fields=",".join(self.ITEM_FIELDS),
            enable_user_data=True,
            enable_images=True,
        )
        return tuple(self._extract_items(response))

    def list_show_episodes(
        self, *, show_id: str, season_id: str | None = None
    ) -> Sequence[BaseItemDto]:
        """Return the episodes for an Emby show.

        Args:
            show_id (str): The Emby item id of the show to list episodes for.
            season_id (str | None): Optional Emby item id of the season to list
                episodes for. If not provided, episodes from all seasons will be
                returned.

        Returns:
            Sequence[BaseItemDto]: The episodes for the specified show (and season
                if provided).
        """
        if self._items_api is None:
            raise RuntimeError("Emby client has not been initialized")
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")

        response = self._items_api.get_users_by_userid_items(
            self._user_id,
            parent_id=season_id or show_id,
            include_item_types="Episode",
            recursive=True,
            fields=",".join(self.ITEM_FIELDS),
            enable_user_data=True,
            enable_images=True,
        )
        return tuple(self._extract_items(response))

    def get_item(self, item_id: str) -> BaseItemDto:
        """Fetch metadata for a single Emby item.

        Args:
            item_id (str): The Emby item id to fetch.

        Returns:
            BaseItemDto: The metadata for the specified item.
        """
        if self._user_library_api is None:
            raise RuntimeError("Emby client has not been initialized")
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")
        return self._user_library_api.get_users_by_userid_items_by_id(
            self._user_id, item_id
        )

    async def fetch_history(self, item: BaseItemDto) -> Sequence[tuple[str, datetime]]:
        """Return play history tuples for an item (item id, played timestamp).

        Args:
            item (BaseItemDto): The Emby item to fetch history for.

        Returns:
            Sequence[tuple[str, datetime]]: A list of tuples containing item ids and
                their corresponding last played timestamps.
        """
        if item.id is None:
            return ()

        item_type = (item.type or "").lower()
        if item_type in {"season", "series"}:
            episodes = self.list_show_episodes(
                show_id=item.id,
                season_id=item.id if item_type == "season" else None,
            )
            history: list[tuple[str, datetime]] = []
            for episode in episodes:
                if not episode.id:
                    continue
                last_played = normalize_local_datetime(
                    episode.user_data.last_played_date if episode.user_data else None
                )
                if last_played is None:
                    continue
                history.append((episode.id, last_played))
            return tuple(history)

        last_played = normalize_local_datetime(
            item.user_data.last_played_date if item.user_data else None
        )
        if last_played is None:
            return ()
        return ((item.id, last_played),)

    def is_on_continue_watching(self, section: BaseItemDto, item: BaseItemDto) -> bool:
        """Determine whether the item appears in Emby's Next Up deck.

        Args:
            section (BaseItemDto): The Emby section containing the item.
            item (BaseItemDto): The Emby item to check.

        Returns:
            bool: True if the item appears in the Next Up deck, False otherwise.
        """
        if self._tv_shows_api is None or self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")
        if section.id is None or (item.type or "").lower() == "movie":
            return False

        now = datetime.now(tz=UTC)
        series_id: str | None = item.series_id or item.id
        if not series_id:
            return False

        section_id = str(section.id)
        cache_entry = self._continue_cache.get(section_id)
        user_data = cast(UserItemDataDto, item.user_data)
        # Refresh when no cache exists, TTL expired, or an item timestamp is newer
        # than the cache. The TTL acts as a safety net because Jellyfin doesn't
        # always propagate episode activity to show-level timestamps.
        should_refresh = (
            cache_entry is None
            or (cache_entry.cached_at + self._CONTINUE_CACHE_TTL <= now)
            or any(
                timestamp is not None and timestamp > cache_entry.cached_at
                for timestamp in (
                    normalize_local_datetime(item.date_created),
                    normalize_local_datetime(
                        user_data.last_played_date if user_data else None,
                    ),
                )
            )
        )

        if should_refresh:
            # Load continue watching items for this section
            if self._tv_shows_api is None or self._user_id is None:
                raise RuntimeError("Emby client has not been initialized")

            series_ids: set[str] = set()
            try:
                next_up_response = self._tv_shows_api.get_shows_nextup(
                    self._user_id,
                    limit=1000,
                    enable_user_data=False,
                    parent_id=section_id,
                )
                items = self._extract_items(next_up_response)
                for next_up_item in items:
                    next_series_id = getattr(next_up_item, "series_id", None)
                    if next_series_id is None:
                        next_series_id = getattr(next_up_item, "id", None)
                    if next_series_id is not None:
                        series_ids.add(str(next_series_id))
            except Exception:
                self.log.error(
                    "Failed to load continue watching items for section %s", section_id
                )
                raise

            cache_entry = _FrozenCacheEntry(
                keys=frozenset(series_ids),
                cached_at=now,
            )
            self._continue_cache[section_id] = cache_entry

        assert cache_entry is not None
        return series_id in cache_entry.keys

    def is_on_watchlist(self, item: BaseItemDto) -> bool:
        """Determine whether the item is on the user's favorites list.

        Args:
            item (BaseItemDto): The Emby item to check.

        Returns:
            bool: True if the item is on the user's favorites list, False otherwise.
        """
        user_data = item.user_data
        return bool(user_data.is_favorite if user_data else False)

    def build_image_url(
        self, item_id: str, *, image_type: str = "Primary", tag: str | None = None
    ) -> str:
        """Construct an image URL.

        Args:
            item_id (str): The Emby item id the image belongs to.
            image_type (str): The type of image to fetch (e.g. "Primary", "Backdrop").
            tag (str | None): Optional image tag to fetch a specific image version.

        Returns:
            str: The constructed image URL.
        """
        params = {
            "width": 92,
            "quality": 90,
            "api_key": self._token,
        }
        if tag:
            params["tag"] = tag
        return (
            f"{self._base_url}/Items/{item_id}/Images/{image_type}?{urlencode(params)}"
        )

    def build_item_url(self, item_id: str) -> str:
        """Construct an Emby web URL for an item details page.

        Args:
            item_id (str): The Emby item id to construct the URL for.

        Returns:
            str: The constructed Emby web URL for the item.
        """
        params = urlencode({"id": item_id})
        return f"{self._base_url}/web/index.html#!/item?{params}"

    def clear_cache(self) -> None:
        """Clear cached metadata."""
        self._continue_cache.clear()

    def _configure_client(self) -> None:
        """Set up the Emby API client and service instances."""
        configuration = emby_client.Configuration()
        configuration.host = self._base_url
        configuration.api_key["api_key"] = self._token
        configuration.user_agent = (
            importlib.metadata.metadata("anibridge-emby-provider").get(
                "Name", "anibridge-emby-provider"
            )
            + "/"
            + importlib.metadata.version("anibridge-emby-provider")
        )

        self._api_client = emby_client.ApiClient(configuration)
        self._items_api = emby_client.ItemsServiceApi(self._api_client)
        self._user_api = emby_client.UserServiceApi(self._api_client)
        self._user_library_api = emby_client.UserLibraryServiceApi(self._api_client)
        self._user_views_api = emby_client.UserViewsServiceApi(self._api_client)
        self._library_structure_api = emby_client.LibraryStructureServiceApi(
            self._api_client
        )
        self._tv_shows_api = emby_client.TvShowsServiceApi(self._api_client)

    def _resolve_user(self):
        """Locate the Emby user matching the configured identifier."""
        if self._user_api is None:
            raise RuntimeError("Emby client has not been initialized")

        response = self._user_api.get_users_query()
        users = self._extract_items(response)
        target = self._user.strip()
        if not target:
            raise ValueError("Emby provider requires a non-empty user value")

        for user in users:
            if str(user.id or "").lower() == target.lower():
                return user
            if str(user.name or "").lower() == target.lower():
                return user

        raise ValueError(f"Unable to locate Emby user: {self._user}")

    def _load_sections(self) -> list[BaseItemDto]:
        """Fetch and filter the Emby library sections available to the user."""
        if self._user_views_api is None:
            raise RuntimeError("Emby client has not been initialized")
        if self._user_id is None:
            raise RuntimeError("Emby client has not been initialized")

        response = self._user_views_api.get_users_by_userid_views(
            self._user_id, include_external_content=False
        )
        items = self._extract_items(response)

        sections: list[BaseItemDto] = []
        for item in items:
            if (item.collection_type or "").lower() not in {"movies", "tvshows"}:
                continue
            if (
                self._section_filter
                and (item.name or "").lower() not in self._section_filter
            ):
                continue
            sections.append(item)
        return sections

    def _fetch_section_items(
        self,
        section: BaseItemDto,
        *,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: Sequence[str] | None = None,
    ) -> list[BaseItemDto]:
        """Fetch items from a section with optional filtering."""
        if self._user_id is None or self._items_api is None:
            raise RuntimeError("Emby client has not been initialized")

        is_movies = (section.collection_type or "").lower() == "movies"
        include_types = "Movie" if is_movies else "Series"
        genres = "|".join(self._genre_filter) if self._genre_filter else None
        ids_filter: list[str] | None = self._parse_id_keys(keys)
        items_api = self._items_api

        def _get_items(
            *,
            include_item_types: str,
            parent_id: str | None = None,
            ids: list[str] | None = None,
            is_played: bool | None = None,
            min_date_last_saved_for_user: datetime | None = None,
            enable_user_data: bool = False,
            enable_images: bool = False,
            limit: int | None = None,
        ) -> list[BaseItemDto]:
            params: dict[str, object] = {
                "include_item_types": include_item_types,
                "recursive": True,
                "fields": ",".join(self.ITEM_FIELDS),
            }
            for k, v in (
                ("parent_id", parent_id),
                ("is_played", is_played),
                (
                    "min_date_last_saved_for_user",
                    min_date_last_saved_for_user.isoformat()
                    if min_date_last_saved_for_user is not None
                    else None,
                ),
                ("enable_user_data", enable_user_data or None),
                ("enable_images", enable_images or None),
                ("genres", genres),
                ("limit", limit),
            ):
                if v is not None:
                    params[k] = v
            if ids is not None:
                params["ids"] = ",".join(ids)
            return self._extract_items(
                items_api.get_users_by_userid_items(self._user_id, **params)
            )

        # Movies can be filtered directly with the "IsPlayed" param
        if not require_watched or is_movies:
            return self._filter_items_by_last_modified(
                _get_items(
                    include_item_types=include_types,
                    parent_id=section.id,
                    enable_user_data=True,
                    enable_images=True,
                    ids=ids_filter,
                    is_played=True if require_watched else None,
                ),
                min_last_modified,
            )

        # For shows, we need to check the activity of individual episodes since
        # show level user data isn't always updated reliably.
        if ids_filter is not None:
            raw = _get_items(include_item_types="Series,Season,Episode", ids=ids_filter)
            series_to_check: set[str] = set()
            for item in raw:
                sid = (
                    item.id
                    if (item.type or "").lower() == "series"
                    else (item.series_id or item.parent_id)
                )
                if sid:
                    series_to_check.add(sid)
            confirmed = [
                sid
                for sid in series_to_check
                if self._filter_items_by_last_modified(
                    _get_items(
                        include_item_types="Episode",
                        parent_id=sid,
                        is_played=True,
                        limit=1,
                    ),
                    min_last_modified,
                )
            ]
        else:
            watched = _get_items(
                include_item_types="Episode",
                parent_id=section.id,
                is_played=True,
                min_date_last_saved_for_user=min_last_modified,
            )
            series_ids = {ep.series_id for ep in watched if ep.series_id}
            season_ids = {
                ep.parent_id for ep in watched if ep.parent_id and not ep.series_id
            }
            if season_ids:
                series_ids |= {
                    s.parent_id
                    for s in _get_items(
                        include_item_types="Season", ids=list(season_ids)
                    )
                    if s.parent_id
                }
            confirmed = list(series_ids)

        if not confirmed:
            return []
        return _get_items(
            include_item_types=include_types,
            ids=confirmed,
            enable_user_data=True,
            enable_images=True,
        )

    def _filter_items_by_last_modified(
        self, items: Sequence[BaseItemDto], min_last_modified: datetime | None
    ) -> list[BaseItemDto]:
        """Filter items by date-created and user activity timestamps."""
        if min_last_modified is None:
            return list(items)

        filtered: list[BaseItemDto] = []
        for item in items:
            user_data = item.user_data
            candidate_datetimes = (
                item.date_created,
                user_data.last_played_date if user_data else None,
            )
            for value in candidate_datetimes:
                normalized = normalize_local_datetime(value)
                if normalized is not None and normalized >= min_last_modified:
                    filtered.append(item)
                    break

        return filtered

    def _parse_id_keys(self, keys: Sequence[str] | None) -> list[str] | None:
        """Parse and normalize item id keys for filtering."""
        if not keys:
            return None
        parsed_ids = [str(key).strip() for key in keys if str(key).strip()]
        return parsed_ids or None

    def _load_show_metadata_fetchers(self) -> dict[str, str]:
        """Get the top-priority TV metadata fetcher for each section if known."""
        if self._library_structure_api is None:
            raise RuntimeError("Emby client has not been initialized")

        section_metadata_fetchers: dict[str, str] = {}
        response = self._library_structure_api.get_library_virtualfolders_query()
        virtual_folders = self._extract_items(response)

        for folder in virtual_folders:
            section_id = str(folder.item_id or "")
            if not section_id or (folder.collection_type or "").lower() != "tvshows":
                continue

            library_options = folder.library_options
            type_options = library_options.type_options if library_options else None
            if not type_options:
                continue

            metadata_fetcher: str | None = None
            for option in type_options:
                if str(option.type or "").lower() != "series":
                    continue

                ordered_fetchers = option.metadata_fetcher_order or []
                enabled_fetchers = option.metadata_fetchers
                enabled_set = set(enabled_fetchers) if enabled_fetchers else None

                if ordered_fetchers:
                    for fetcher in ordered_fetchers:
                        if not fetcher:
                            continue
                        if enabled_set is not None and fetcher not in enabled_set:
                            continue
                        metadata_fetcher = fetcher
                        break
                else:
                    for fetcher in enabled_fetchers or []:
                        if fetcher:
                            metadata_fetcher = fetcher
                            break

                if metadata_fetcher:
                    break

            if metadata_fetcher:
                section_metadata_fetchers[section_id] = metadata_fetcher

        return section_metadata_fetchers

    @staticmethod
    def _has_user_activity(user_data: UserItemDataDto | None) -> bool:
        """Return true when user data indicates any relevant user activity."""
        if user_data is None:
            return False
        return bool(
            user_data.played
            or user_data.play_count
            or user_data.playback_position_ticks
            or user_data.is_favorite
        )

    @staticmethod
    def _extract_items(response: object) -> list:
        """Return list-like content from SDK responses with `.items` payloads."""
        if response is None:
            return []
        items = getattr(response, "items", None)
        if isinstance(items, list):
            return items
        if isinstance(response, list):
            return response
        return []
