"""Tests for the Emby library provider integration."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import emby_client
import pytest
from anibridge.utils.types import ProviderLogger

import anibridge.providers.library.emby as library_module
import anibridge.providers.library.emby.library as library_impl
from anibridge.providers.library.emby.client import EmbyClient


def _test_logger() -> ProviderLogger:
    logger = logging.getLogger("tests.anibridge.emby.client")
    logger.handlers = []
    logger.addHandler(logging.NullHandler())
    return cast(ProviderLogger, logger)


@dataclass(slots=True)
class FakeUserData:
    played: bool = False
    play_count: int = 0
    rating: float | None = None
    is_favorite: bool = False
    playback_position_ticks: int = 0
    last_played_date: str | None = None


@dataclass(slots=True)
class FakeItem:
    id: str
    name: str
    type: str
    provider_ids: dict[str, str] | None = None
    user_data: FakeUserData | None = None
    date_created: str | None = None
    image_tags: dict[str, str] | None = None
    collection_type: str | None = None
    series_id: str | None = None
    season_id: str | None = None
    index_number: int | None = None
    parent_index_number: int | None = None
    parent_id: str | None = None


class FakeEmbyClient:
    """Stub for an Emby client session."""

    def __init__(
        self,
        *,
        sections: list[FakeItem],
        items: dict[str, list[FakeItem]],
        show_metadata_fetchers_by_section: dict[str, str] | None = None,
    ):
        self._sections = sections
        self._items = items
        self._show_metadata_fetchers_by_section = (
            show_metadata_fetchers_by_section or {}
        )
        self._user_id = "user-1"
        self._user_name = "Demo User"
        self.closed = False

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    def user_id(self) -> str:
        return self._user_id

    def user_name(self) -> str:
        return self._user_name

    def sections(self):
        return tuple(self._sections)

    async def list_section_items(
        self,
        section: FakeItem,
        *,
        min_last_modified: datetime | None = None,
        require_watched: bool = False,
        keys: list[str] | None = None,
    ):
        items = list(self._items.get(section.id, []))
        if min_last_modified is not None:
            items = [
                item
                for item in items
                if _parse_date(item.date_created) >= min_last_modified
            ]
        if require_watched:
            items = [
                item
                for item in items
                if int((item.user_data.play_count if item.user_data else 0) or 0) > 0
            ]
        if keys is not None:
            allowed = set(keys)
            items = [item for item in items if item.id in allowed]
        return tuple(items)

    def list_show_seasons(self, show_id: str):
        return tuple(self._items.get(f"seasons:{show_id}", []))

    def list_show_episodes(self, *, show_id: str, season_id: str | None = None):
        key = f"episodes:{season_id or show_id}"
        return tuple(self._items.get(key, []))

    def get_item(self, item_id: str):
        for items in self._items.values():
            for item in items:
                if item.id == item_id:
                    return item
        raise KeyError(item_id)

    async def fetch_history(self, item: FakeItem):
        if item.type in {"Season", "Series"}:
            episodes = self._items.get("episodes:season-1", [])
            history = [_history_tuple(ep) for ep in episodes]
        else:
            history = [_history_tuple(item)]
        return tuple(entry for entry in history if entry is not None)

    def is_on_continue_watching(self, section: FakeItem, item: FakeItem) -> bool:
        user_data = item.user_data
        return bool(
            user_data and not user_data.played and user_data.playback_position_ticks
        )

    def is_on_watchlist(self, item: FakeItem) -> bool:
        user_data = item.user_data
        return bool(user_data and user_data.is_favorite)

    def build_image_url(
        self, item_id: str, *, image_type: str = "Primary", tag: str | None = None
    ) -> str:
        return f"http://example.invalid/{item_id}/{image_type}?tag={tag or ''}"

    def build_item_url(self, item_id: str) -> str:
        return f"http://emby/web/index.html#!/item?id={item_id}"

    def clear_cache(self) -> None:
        return None

    def show_metadata_fetcher_for_section(self, section_id: str) -> str | None:
        return self._show_metadata_fetchers_by_section.get(section_id)


def _parse_date(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _history_tuple(item: FakeItem):
    last_played = item.user_data.last_played_date if item.user_data else None
    if not last_played:
        return None
    return (item.id, _parse_date(last_played))


class RecordingItemsApi:
    """Record Emby item query kwargs for regression assertions."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_users_by_userid_items(self, user_id: str, **kwargs):
        self.calls.append({"user_id": user_id, **kwargs})
        return SimpleNamespace(items=[])


class FakeRequest:
    """Lightweight request stub for webhook parsing tests."""

    def __init__(
        self,
        *,
        payload: dict[str, object] | str,
        content_type: str = "application/json",
    ) -> None:
        self.headers = {"content-type": content_type}
        self._payload = payload

    async def json(self):
        return self._payload

    async def form(self):
        return {"data": self._payload}


def _webhook_payload(
    *,
    event: str,
    item: dict[str, object] | None = None,
    user: dict[str, object] | None = None,
    session: dict[str, object] | None = None,
    playback_info: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "Title": "demo webhook",
        "Date": "2026-04-16T17:57:44.5525618Z",
        "Event": event,
        "Severity": "Info",
        "Server": {"Name": "emby", "Id": "server-1", "Version": "4.9.3.0"},
    }
    if item is not None:
        payload["Item"] = item
    if user is not None:
        payload["User"] = user
    if session is not None:
        payload["Session"] = session
    if playback_info is not None:
        payload["PlaybackInfo"] = playback_info
    return payload


@pytest.fixture()
def library_setup(monkeypatch: pytest.MonkeyPatch):
    """Set up an EmbyLibraryProvider with stubbed dependencies."""
    movie = FakeItem(
        id="movie-1",
        name="Movie One",
        type="Movie",
        provider_ids={
            "AniDb": "1111",
            "AniList": "2222",
            "Imdb": "tt123",
            "Tmdb": "789",
        },
        user_data=FakeUserData(
            played=True,
            play_count=2,
            rating=8.0,
            is_favorite=True,
            playback_position_ticks=0,
            last_played_date="2025-01-01T12:00:00Z",
        ),
        date_created="2025-01-05T12:00:00Z",
        image_tags={"Primary": "tag"},
    )
    show = FakeItem(
        id="show-1",
        name="Show One",
        type="Series",
        provider_ids={"AniDb": "3333", "AniList": "4444", "Tvdb": "55"},
        user_data=FakeUserData(
            played=False,
            play_count=0,
            is_favorite=False,
            playback_position_ticks=123,
        ),
        date_created="2025-01-10T12:00:00Z",
    )
    season = FakeItem(
        id="season-1",
        name="Season 1",
        type="Season",
        series_id="show-1",
        index_number=1,
    )
    episode = FakeItem(
        id="episode-1",
        name="Episode 1",
        type="Episode",
        series_id="show-1",
        season_id="season-1",
        index_number=1,
        parent_index_number=1,
        user_data=FakeUserData(last_played_date="2025-01-11T12:00:00Z"),
    )

    sections = [
        FakeItem(
            id="sec-movies",
            name="Movies",
            type="CollectionFolder",
            collection_type="movies",
        ),
        FakeItem(
            id="sec-shows",
            name="Shows",
            type="CollectionFolder",
            collection_type="tvshows",
        ),
    ]
    items = {
        "sec-movies": [movie],
        "sec-shows": [show],
        "seasons:show-1": [season],
        "episodes:season-1": [episode],
    }

    fake_client = FakeEmbyClient(
        sections=sections,
        items=items,
        show_metadata_fetchers_by_section={"sec-shows": "AniDB"},
    )

    monkeypatch.setattr(
        library_module.EmbyLibraryProvider,
        "_create_client",
        lambda self: fake_client,
    )

    provider = library_module.EmbyLibraryProvider(
        config={
            "url": "http://emby",
            "token": "token",
            "user": "demo",
            "strict": False,
        },
        logger=_test_logger(),
    )
    return provider, fake_client, movie


@pytest.mark.asyncio
async def test_get_sections_returns_movie_and_show_sections(library_setup):
    """The provider exposes Emby view sections."""
    provider, _client, _movie = library_setup
    await provider.initialize()
    sections = await provider.get_sections()
    assert len(sections) == 2
    assert [section.title for section in sections] == ["Movies", "Shows"]


@pytest.mark.asyncio
async def test_list_items_supports_common_filters(library_setup):
    """Query filters should trim the dataset as expected."""
    provider, _client, movie = library_setup
    await provider.initialize()
    movie_section = (await provider.get_sections())[0]

    cutoff = datetime.now(UTC) + timedelta(days=1)
    recent = await provider.list_items(movie_section, min_last_modified=cutoff)
    assert len(recent) == 0

    watched_only = await provider.list_items(movie_section, require_watched=True)
    assert [item.key for item in watched_only] == [movie.id]

    subset = await provider.list_items(movie_section, keys=(movie.id,))
    assert [item.key for item in subset] == [movie.id]


@pytest.mark.asyncio
async def test_mapping_descriptors_and_watch_state(library_setup):
    """Mapping descriptors should mirror provider ids, and watch state is surfaced."""
    provider, _client, _movie = library_setup
    await provider.initialize()
    movie_section, show_section = await provider.get_sections()

    movie_item = (await provider.list_items(movie_section))[0]
    show_item = (await provider.list_items(show_section))[0]

    assert movie_item.mapping_descriptors() == (
        ("anidb", "1111", "R"),
        ("anilist", "2222", None),
        ("imdb_movie", "tt123", None),
        ("tmdb_movie", "789", None),
    )
    assert show_item.mapping_descriptors() == (
        ("anidb", "3333", None),
        ("anilist", "4444", None),
        ("tvdb_show", "55", None),
    )
    assert show_item.on_watching is True
    assert movie_item.on_watchlist is True
    assert movie_item.user_rating == 80
    assert movie_item.view_count == 2


@pytest.mark.asyncio
async def test_season_and_episode_mapping_scopes(library_setup):
    """Season and episode entries should scope mappings to the season index."""
    provider, _client, _movie = library_setup
    await provider.initialize()
    _movie_section, show_section = await provider.get_sections()

    show_item = (await provider.list_items(show_section))[0]
    seasons = show_item.seasons()
    assert len(seasons) == 1
    season = seasons[0]
    assert season.index == 1

    descriptors = season.mapping_descriptors()
    assert descriptors == (
        ("anidb", "3333", "R"),
        ("anilist", "4444", None),
        ("tvdb_show", "55", "s1"),
    )

    episodes = season.episodes()
    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.mapping_descriptors() == descriptors


def test_fetch_section_items_omits_ids_when_no_key_filter():
    """The Emby SDK must not receive unset query params as stringified None."""
    client = EmbyClient(
        logger=_test_logger(),
        url="http://emby",
        token="token",
        user="demo",
    )
    items_api = RecordingItemsApi()
    client._items_api = cast(emby_client.ItemsServiceApi, items_api)
    client._user_id = "user-1"

    section = cast(
        emby_client.models.base_item_dto.BaseItemDto,
        FakeItem(
            id="sec-shows",
            name="Shows",
            type="CollectionFolder",
            collection_type="tvshows",
        ),
    )

    watched_items = client._fetch_section_items(section, require_watched=True)
    all_items = client._fetch_section_items(section)

    assert watched_items == []
    assert all_items == []
    assert len(items_api.calls) == 2

    watched_call, all_items_call = items_api.calls
    assert watched_call["parent_id"] == section.id
    assert watched_call["is_played"] is True
    assert "ids" not in watched_call
    assert "genres" not in watched_call

    assert all_items_call["parent_id"] == section.id
    assert "is_played" not in all_items_call
    assert "ids" not in all_items_call
    assert "genres" not in all_items_call


@pytest.mark.asyncio
async def test_strict_mode_filters_show_mappings(monkeypatch: pytest.MonkeyPatch):
    """Strict mode should keep only descriptors matching section metadata fetcher."""
    show = FakeItem(
        id="show-1",
        name="Show One",
        type="Series",
        provider_ids={"AniDb": "3333", "AniList": "4444", "Tvdb": "55"},
        user_data=FakeUserData(playback_position_ticks=123),
    )
    section = FakeItem(
        id="sec-shows",
        name="Shows",
        type="CollectionFolder",
        collection_type="tvshows",
    )
    fake_client = FakeEmbyClient(
        sections=[section],
        items={"sec-shows": [show], "seasons:show-1": [], "episodes:show-1": []},
        show_metadata_fetchers_by_section={"sec-shows": "AniDB"},
    )

    monkeypatch.setattr(
        library_module.EmbyLibraryProvider,
        "_create_client",
        lambda self: fake_client,
    )
    provider = library_module.EmbyLibraryProvider(
        config={
            "url": "http://emby",
            "token": "token",
            "user": "demo",
            "strict": True,
        },
        logger=_test_logger(),
    )
    await provider.initialize()

    section_wrapper = (await provider.get_sections())[0]
    show_item = (await provider.list_items(section_wrapper))[0]
    assert show_item.mapping_descriptors() == (("anidb", "3333", None),)


@pytest.mark.asyncio
async def test_media_helpers_and_history(
    library_setup, monkeypatch: pytest.MonkeyPatch
):
    """Media URLs, poster behavior, and history conversion should be stable."""
    provider, _client, movie = library_setup
    await provider.initialize()
    movie_section = (await provider.get_sections())[0]
    movie_item = (await provider.list_items(movie_section))[0]

    monkeypatch.setattr(
        library_impl,
        "fetch_image_as_data_url",
        lambda *_args, **_kwargs: "data:image/png;base64,AA==",
    )

    assert movie_item.media().external_url.endswith(movie.id)
    assert movie_item.media().poster_image is not None

    movie.image_tags = None
    assert movie_item.media().poster_image is None

    movie.image_tags = {"Primary": "tag"}
    monkeypatch.setattr(
        library_impl,
        "fetch_image_as_data_url",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert movie_item.media().poster_image is None

    history = await movie_item.history()
    assert history and history[0].library_key == movie.id


@pytest.mark.asyncio
async def test_parse_webhook_behaviors(library_setup):
    """Webhook parsing should support sync events and reject unsupported payloads."""
    provider, _client, movie = library_setup
    await provider.initialize()

    should_sync, keys = await provider.parse_webhook(
        FakeRequest(
            payload=_webhook_payload(
                event="playback.stop",
                user={"Id": "user-1", "Name": "Demo User"},
                item={
                    "Name": "Episode 1",
                    "ServerId": "server-1",
                    "Id": "episode-1",
                    "Path": "/library/show-1/season-1/episode-1.mkv",
                    "Type": "Episode",
                    "SeriesId": "show-1",
                },
                session={
                    "Client": "Emby Web",
                    "DeviceName": "Firefox Windows",
                    "DeviceId": "device-1",
                    "ApplicationVersion": "4.9.3.0",
                    "Id": "session-1",
                },
                playback_info={"PlayedToCompletion": True},
            )
        )
    )
    assert should_sync is True and keys == ("show-1",)

    should_sync, keys = await provider.parse_webhook(
        FakeRequest(
            payload=_webhook_payload(
                event="item.rate",
                user={"Id": "user-1", "Name": "Demo User"},
                item={
                    "Name": "Episode 1",
                    "ServerId": "server-1",
                    "Id": "episode-1",
                    "Path": "/library/show-1/season-1/episode-1.mkv",
                    "Type": "Episode",
                    "SeriesId": "show-1",
                    "UserData": {"IsFavorite": True, "Played": False},
                },
            )
        )
    )
    assert should_sync is True and keys == ("show-1",)

    should_sync, keys = await provider.parse_webhook(
        FakeRequest(
            payload=_webhook_payload(
                event="item.rate",
                user={"Id": "user-1", "Name": "Demo User"},
                item={
                    "Name": "Season 1",
                    "ServerId": "server-1",
                    "Id": "season-1",
                    "ParentId": "show-1",
                    "Type": "Season",
                    "UserData": {"IsFavorite": True, "Played": False},
                },
            )
        )
    )
    assert should_sync is True and keys == ("show-1",)

    should_sync, keys = await provider.parse_webhook(
        FakeRequest(
            payload=_webhook_payload(
                event="playback.stop",
                user={"Id": "another-user", "Name": "Someone Else"},
                item={
                    "Name": movie.name,
                    "ServerId": "server-1",
                    "Id": movie.id,
                    "Path": "/library/movie-1.mkv",
                    "Type": "Movie",
                },
                session={
                    "Client": "Emby Web",
                    "DeviceName": "Firefox Windows",
                    "DeviceId": "device-1",
                    "ApplicationVersion": "4.9.3.0",
                    "Id": "session-1",
                },
                playback_info={"PlayedToCompletion": True},
            )
        )
    )
    assert should_sync is False and keys == ()

    with pytest.raises(ValueError, match="No supported event type found"):
        await provider.parse_webhook(
            FakeRequest(
                payload=_webhook_payload(
                    event="session.start",
                    user={"Id": "user-1", "Name": "Demo User"},
                    item={
                        "Name": movie.name,
                        "ServerId": "server-1",
                        "Id": movie.id,
                        "Path": "/library/movie-1.mkv",
                        "Type": "Movie",
                    },
                )
            )
        )

    with pytest.raises(ValueError):
        await provider.parse_webhook(FakeRequest(payload={"ItemId": movie.id}))
    with pytest.raises(ValueError):
        await provider.parse_webhook(
            FakeRequest(
                payload=_webhook_payload(
                    event="playback.stop",
                    user={"Id": "user-1", "Name": "Demo User"},
                )
            )
        )


@pytest.mark.asyncio
async def test_list_items_and_wrap_entry_error_paths(library_setup):
    """Provider should reject non-Emby sections and unsupported media types."""
    provider, _client, _movie = library_setup
    await provider.initialize()

    with pytest.raises(TypeError):
        await provider.list_items(cast(Any, object()))

    section = (await provider.get_sections())[0]
    with pytest.raises(TypeError):
        provider._wrap_entry(
            section,
            cast(
                emby_client.models.base_item_dto.BaseItemDto,
                FakeItem(id="x", name="X", type="Unknown"),
            ),
        )


@pytest.mark.asyncio
async def test_episode_and_season_parent_resolution_errors(library_setup):
    """Episode/season wrappers should raise errors when parent ids are missing."""
    provider, _client, _movie = library_setup
    await provider.initialize()
    section = (await provider.get_sections())[1]

    season = library_impl.EmbyLibrarySeason(
        provider,
        section,
        cast(
            emby_client.models.base_item_dto.BaseItemDto,
            FakeItem(id="season-x", name="Season", type="Season", series_id=None),
        ),
    )
    with pytest.raises(RuntimeError):
        season.show()

    episode = library_impl.EmbyLibraryEpisode(
        provider,
        section,
        cast(
            emby_client.models.base_item_dto.BaseItemDto,
            FakeItem(
                id="ep-x",
                name="Episode",
                type="Episode",
                series_id=None,
                season_id=None,
            ),
        ),
    )
    with pytest.raises(RuntimeError):
        episode.show()
    with pytest.raises(RuntimeError):
        episode.season()
