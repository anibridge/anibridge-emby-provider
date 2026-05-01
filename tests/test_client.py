"""Tests for Emby client internals."""

from datetime import UTC, datetime, timedelta
from logging import getLogger
from types import SimpleNamespace
from typing import Any, cast

import pytest

from anibridge.providers.library.emby.client import EmbyClient, _FrozenCacheEntry


def _test_logger():
    return getLogger("tests.emby.client")


class _FakeItemsApi:
    def __init__(self, responses: list[list[Any]] | None = None) -> None:
        self.responses = responses or []
        self.calls: list[dict[str, Any]] = []

    def get_users_by_userid_items(self, user_id: str, **kwargs: Any):
        self.calls.append({"user_id": user_id, **kwargs})
        items = self.responses.pop(0) if self.responses else []
        return SimpleNamespace(items=items)


class _FakeTvShowsLookupApi:
    def __init__(
        self,
        *,
        season_responses: list[list[Any]] | None = None,
        episode_responses: list[list[Any]] | None = None,
    ) -> None:
        self.season_responses = season_responses or []
        self.episode_responses = episode_responses or []
        self.season_calls: list[dict[str, Any]] = []
        self.episode_calls: list[dict[str, Any]] = []

    def get_shows_by_id_seasons(self, user_id: str, show_id: str, **kwargs: Any):
        self.season_calls.append({"user_id": user_id, "show_id": show_id, **kwargs})
        items = self.season_responses.pop(0) if self.season_responses else []
        return SimpleNamespace(items=items)

    def get_shows_by_id_episodes(self, show_id: str, **kwargs: Any):
        self.episode_calls.append({"show_id": show_id, **kwargs})
        items = self.episode_responses.pop(0) if self.episode_responses else []
        return SimpleNamespace(items=items)


class _FakeSystemApi:
    def __init__(self, version: str | None = None, *, raises: Exception | None = None):
        self.version = version
        self.raises = raises
        self.calls = 0

    def get_system_info(self):
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return SimpleNamespace(version=self.version)


@pytest.fixture()
def emby_client_instance() -> EmbyClient:
    return EmbyClient(
        logger=cast(Any, _test_logger()),
        url="http://emby",
        token="token",
        user="demo",
    )


@pytest.mark.asyncio
async def test_initialize_and_close(
    monkeypatch: pytest.MonkeyPatch, emby_client_instance: EmbyClient
) -> None:
    """initialize should populate state and close should clear it."""
    client = emby_client_instance

    async def inline_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(
        "anibridge.providers.library.emby.client.asyncio.to_thread", inline_to_thread
    )
    monkeypatch.setattr(client, "_configure_client", lambda: None)
    monkeypatch.setattr(
        client, "_resolve_user", lambda: SimpleNamespace(id="u-1", name="Demo")
    )
    monkeypatch.setattr(client, "_load_sections", lambda: [SimpleNamespace(id="sec-1")])
    monkeypatch.setattr(
        client,
        "_load_show_metadata_fetcher_orders",
        lambda: {"sec-1": ("AniDb", "AniList")},
    )

    await client.initialize()
    assert client.user_id() == "u-1"
    assert client.user_name() == "Demo"
    assert len(client.sections()) == 1
    assert client.show_metadata_fetchers_for_section("sec-1") == ("AniDb", "AniList")
    assert client.show_metadata_fetcher_for_section("sec-1") == "AniDb"

    await client.close()
    assert client.sections() == ()
    with pytest.raises(RuntimeError):
        client.user_id()


def test_runtime_guards_before_initialize(emby_client_instance: EmbyClient) -> None:
    """Uninitialized API calls should raise runtime errors."""
    client = emby_client_instance

    with pytest.raises(RuntimeError):
        client.user_id()
    with pytest.raises(RuntimeError):
        client.user_name()
    with pytest.raises(RuntimeError):
        client.list_show_seasons("show")
    with pytest.raises(RuntimeError):
        client.list_show_episodes(show_id="show")
    with pytest.raises(RuntimeError):
        client.get_item("item")
    with pytest.raises(RuntimeError):
        client.is_on_continue_watching(
            cast(Any, SimpleNamespace(id="sec-1")),
            cast(Any, SimpleNamespace(type="Series", id="x", series_id=None)),
        )


def test_header_and_url_helpers(emby_client_instance: EmbyClient) -> None:
    """Header/image/item URL helpers should include expected values."""
    client = emby_client_instance
    assert client.auth_headers() == {"X-Emby-Token": "token"}

    image_url = client.build_image_url("item-1", tag="abc")
    assert "/Items/item-1/Images/Primary" in image_url
    assert "api_key=token" in image_url
    assert "tag=abc" in image_url

    assert client.build_item_url("item-1").endswith("id=item-1")
    assert client.clear_cache() is None


def test_show_episode_and_item_lookup_success(emby_client_instance: EmbyClient) -> None:
    """Season/episode/item methods should proxy through Emby APIs."""
    client = emby_client_instance
    client._user_id = "user-1"
    client._items_api = cast(
        Any,
        _FakeItemsApi(
            responses=[
                [SimpleNamespace(id="season-1")],
                [SimpleNamespace(id="episode-1")],
            ]
        ),
    )
    client._user_library_api = cast(
        Any,
        SimpleNamespace(
            get_users_by_userid_items_by_id=lambda user_id, item_id: SimpleNamespace(
                id=item_id
            )
        ),
    )

    seasons = client.list_show_seasons("show-1")
    episodes = client.list_show_episodes(show_id="show-1", season_id="season-1")
    item = client.get_item("movie-1")

    assert [s.id for s in seasons] == ["season-1"]
    assert [e.id for e in episodes] == ["episode-1"]
    assert item.id == "movie-1"


def test_show_lookup_uses_items_queries(
    emby_client_instance: EmbyClient,
) -> None:
    """Season and episode traversal should use the generic items endpoint."""
    client = emby_client_instance
    client._user_id = "user-1"
    client._items_api = cast(
        Any,
        _FakeItemsApi(
            responses=[
                [SimpleNamespace(id="season-items")],
                [SimpleNamespace(id="episode-items")],
            ]
        ),
    )

    seasons = client.list_show_seasons("show-1")
    episodes = client.list_show_episodes(show_id="show-1", season_id="season-1")

    assert [s.id for s in seasons] == ["season-items"]
    assert [e.id for e in episodes] == ["episode-items"]
    assert client._items_api.calls == [
        {
            "user_id": "user-1",
            "parent_id": "show-1",
            "include_item_types": "Season",
            "recursive": True,
            "fields": ",".join(client.ITEM_FIELDS),
            "enable_user_data": True,
            "enable_images": True,
        },
        {
            "user_id": "user-1",
            "parent_id": "season-1",
            "include_item_types": "Episode",
            "recursive": True,
            "fields": ",".join(client.ITEM_FIELDS),
            "enable_user_data": True,
            "enable_images": True,
        },
    ]


def test_show_lookup_returns_empty_when_items_query_has_no_items(
    emby_client_instance: EmbyClient,
) -> None:
    """Empty items query responses should be returned as-is without fallback."""
    client = emby_client_instance
    client._user_id = "user-1"
    client._items_api = cast(
        Any,
        _FakeItemsApi(responses=[[], []]),
    )
    seasons = client.list_show_seasons("show-1")
    episodes = client.list_show_episodes(show_id="show-1", season_id="season-1")

    assert seasons == ()
    assert episodes == ()


@pytest.mark.asyncio
async def test_initialize_warns_when_server_major_version_is_not_supported(
    monkeypatch: pytest.MonkeyPatch,
    emby_client_instance: EmbyClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Initialization should warn when the Emby server major version is not 4."""
    client = emby_client_instance

    async def inline_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(
        "anibridge.providers.library.emby.client.asyncio.to_thread", inline_to_thread
    )
    monkeypatch.setattr(client, "_configure_client", lambda: None)
    monkeypatch.setattr(client, "_load_server_version", lambda: "5.0.0.1")
    monkeypatch.setattr(
        client, "_resolve_user", lambda: SimpleNamespace(id="u-1", name="Demo")
    )
    monkeypatch.setattr(client, "_load_sections", lambda: [SimpleNamespace(id="sec-1")])
    monkeypatch.setattr(
        client,
        "_load_show_metadata_fetcher_orders",
        lambda: {"sec-1": ("AniDb", "AniList")},
    )

    with caplog.at_level("WARNING"):
        await client.initialize()

    assert client.server_version() == "5.0.0.1"
    assert "only tested with major version 4.x" in caplog.text


def test_load_server_version_returns_none_when_system_info_fails(
    emby_client_instance: EmbyClient,
) -> None:
    """Server version probing should be non-fatal when the endpoint fails."""
    client = emby_client_instance
    client._system_api = cast(Any, _FakeSystemApi(raises=RuntimeError("boom")))

    assert client._load_server_version() is None


@pytest.mark.asyncio
async def test_fetch_history_for_series_and_movie(
    emby_client_instance: EmbyClient,
) -> None:
    """Should aggregate episode history for shows."""
    client = emby_client_instance

    now = datetime.now(UTC)
    client.list_show_episodes = cast(
        Any,
        lambda **kwargs: [
            SimpleNamespace(
                id="ep-1",
                user_data=SimpleNamespace(last_played_date=now),
            )
        ],
    )

    series = SimpleNamespace(id="show-1", type="Series")
    movie = SimpleNamespace(
        id="movie-1",
        type="Movie",
        user_data=SimpleNamespace(last_played_date=now),
    )

    series_history = await client.fetch_history(cast(Any, series))
    movie_history = await client.fetch_history(cast(Any, movie))

    assert series_history and series_history[0][0] == "ep-1"
    assert movie_history and movie_history[0][0] == "movie-1"


def test_is_on_continue_watching_variants(emby_client_instance: EmbyClient) -> None:
    """Continue-watching should handle type mapping, caching, and API errors."""
    client = emby_client_instance
    client._user_id = "user-1"

    class _FakeTvShowsApi:
        def __init__(self) -> None:
            self.calls = 0

        def get_shows_nextup(self, user_id: str, **kwargs: Any):
            self.calls += 1
            if self.calls == 2:
                raise TypeError("bad")
            if self.calls == 3:
                return SimpleNamespace(items=[])
            return SimpleNamespace(items=[SimpleNamespace(id="ep", series_id="show-1")])

    tv_api = _FakeTvShowsApi()
    client._tv_shows_api = cast(Any, tv_api)
    section = SimpleNamespace(id="sec-1")

    series = SimpleNamespace(
        type="Series",
        id="show-1",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    season = SimpleNamespace(
        type="Season",
        id="season-1",
        series_id="show-1",
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    episode = SimpleNamespace(
        type="Episode",
        id="ep-1",
        series_id="show-1",
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    bad = SimpleNamespace(
        type="Series",
        id="bad",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    empty = SimpleNamespace(
        type="Series",
        id="empty",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )

    assert client.is_on_continue_watching(cast(Any, section), cast(Any, series)) is True
    assert client.is_on_continue_watching(cast(Any, section), cast(Any, series)) is True
    assert client.is_on_continue_watching(cast(Any, section), cast(Any, season)) is True
    assert (
        client.is_on_continue_watching(cast(Any, section), cast(Any, episode)) is True
    )
    assert client.is_on_continue_watching(cast(Any, section), cast(Any, empty)) is False
    assert client.is_on_continue_watching(cast(Any, section), cast(Any, bad)) is False
    assert tv_api.calls == 1


def test_is_on_continue_watching_uses_stale_deck_on_refresh_failure(
    emby_client_instance: EmbyClient,
) -> None:
    """Refresh failures should reuse stale cached Next Up deck when available."""
    client = emby_client_instance
    client._user_id = "user-1"

    class _FakeTvShowsApi:
        def __init__(self) -> None:
            self.calls = 0

        def get_shows_nextup(self, user_id: str, **kwargs: Any):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(
                    items=[SimpleNamespace(id="ep", series_id="show-stale")]
                )
            raise TypeError("bad")

    tv_api = _FakeTvShowsApi()
    client._tv_shows_api = cast(Any, tv_api)
    section = SimpleNamespace(id="sec-1")

    initial_item = SimpleNamespace(
        type="Series",
        id="show-stale",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    assert (
        client.is_on_continue_watching(cast(Any, section), cast(Any, initial_item))
        is True
    )

    refresh_item = SimpleNamespace(
        type="Series",
        id="show-stale",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(
            last_played_date=datetime.now(UTC) + timedelta(days=1)
        ),
    )
    with pytest.raises(TypeError, match="bad"):
        client.is_on_continue_watching(cast(Any, section), cast(Any, refresh_item))
    assert tv_api.calls == 2


def test_is_on_continue_watching_cache_is_section_scoped(
    emby_client_instance: EmbyClient,
) -> None:
    """Cache should be keyed per section and not shared across sections."""
    client = emby_client_instance
    client._user_id = "user-1"

    class _FakeTvShowsApi:
        def __init__(self) -> None:
            self.calls = 0

        def get_shows_nextup(self, user_id: str, **kwargs: Any):
            self.calls += 1
            parent_id = kwargs.get("parent_id")
            if parent_id == "sec-1":
                return SimpleNamespace(
                    items=[SimpleNamespace(id="ep", series_id="show-1")]
                )
            return SimpleNamespace(items=[SimpleNamespace(id="ep", series_id="show-2")])

    tv_api = _FakeTvShowsApi()
    client._tv_shows_api = cast(Any, tv_api)

    section_one = SimpleNamespace(id="sec-1")
    section_two = SimpleNamespace(id="sec-2")
    show_one = SimpleNamespace(
        type="Series",
        id="show-1",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    show_two = SimpleNamespace(
        type="Series",
        id="show-2",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )

    assert client.is_on_continue_watching(cast(Any, section_one), cast(Any, show_one))
    assert client.is_on_continue_watching(cast(Any, section_two), cast(Any, show_two))
    assert tv_api.calls == 2


def test_is_on_continue_watching_refreshes_when_item_activity_is_newer(
    emby_client_instance: EmbyClient,
) -> None:
    """Cache entries should refresh if item activity is newer than cache time."""
    client = emby_client_instance
    client._user_id = "user-1"

    class _FakeTvShowsApi:
        def __init__(self) -> None:
            self.calls = 0

        def get_shows_nextup(self, user_id: str, **kwargs: Any):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(items=[])
            return SimpleNamespace(items=[SimpleNamespace(id="ep", series_id="show-2")])

    tv_api = _FakeTvShowsApi()
    client._tv_shows_api = cast(Any, tv_api)
    section = SimpleNamespace(id="sec-1")

    first_item = SimpleNamespace(
        type="Series",
        id="show-2",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )
    assert (
        client.is_on_continue_watching(cast(Any, section), cast(Any, first_item))
        is False
    )

    refreshed_item = SimpleNamespace(
        type="Series",
        id="show-2",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(
            last_played_date=datetime.now(UTC) + timedelta(days=1)
        ),
    )
    assert (
        client.is_on_continue_watching(cast(Any, section), cast(Any, refreshed_item))
        is True
    )
    assert tv_api.calls == 2


def test_is_on_continue_watching_refreshes_when_cache_ttl_expires(
    emby_client_instance: EmbyClient,
) -> None:
    """Expired cache entries should refresh even without newer item timestamps."""
    client = emby_client_instance
    client._user_id = "user-1"

    class _FakeTvShowsApi:
        def __init__(self) -> None:
            self.calls = 0

        def get_shows_nextup(self, user_id: str, **kwargs: Any):
            self.calls += 1
            return SimpleNamespace(items=[SimpleNamespace(id="ep", series_id="show-2")])

    tv_api = _FakeTvShowsApi()
    client._tv_shows_api = cast(Any, tv_api)

    section = SimpleNamespace(id="sec-1")
    client._continue_cache[str(section.id)] = _FrozenCacheEntry(
        keys=frozenset({"show-1"}),
        cached_at=datetime.now(UTC) - client._CONTINUE_CACHE_TTL - timedelta(seconds=1),
    )

    item = SimpleNamespace(
        type="Series",
        id="show-2",
        series_id=None,
        date_created=None,
        user_data=SimpleNamespace(last_played_date=None),
    )

    assert client.is_on_continue_watching(cast(Any, section), cast(Any, item)) is True
    assert tv_api.calls == 1


def test_watchlist_and_helper_utilities(emby_client_instance: EmbyClient) -> None:
    """Watchlist and helper methods should handle edge values."""
    client = emby_client_instance

    assert client.is_on_watchlist(cast(Any, SimpleNamespace(user_data=None))) is False
    assert (
        client.is_on_watchlist(
            cast(Any, SimpleNamespace(user_data=SimpleNamespace(is_favorite=True)))
        )
        is True
    )

    assert client._parse_id_keys(None) is None
    assert client._parse_id_keys(["  ", "id-1", "id-2"]) == ["id-1", "id-2"]

    assert client._has_user_activity(None) is False
    assert (
        client._has_user_activity(
            cast(
                Any,
                SimpleNamespace(
                    played=False,
                    play_count=0,
                    playback_position_ticks=0,
                    is_favorite=False,
                ),
            )
        )
        is False
    )
    assert (
        client._has_user_activity(
            cast(
                Any,
                SimpleNamespace(
                    played=False,
                    play_count=1,
                    playback_position_ticks=0,
                    is_favorite=False,
                ),
            )
        )
        is True
    )

    assert client._extract_items(None) == []
    assert client._extract_items(SimpleNamespace(items=[1, 2])) == [1, 2]
    assert client._extract_items([3, 4]) == [3, 4]


def test_resolve_user_and_load_sections(emby_client_instance: EmbyClient) -> None:
    """User and section resolvers should match ids/names and enforce filters."""
    client = emby_client_instance
    users = [
        SimpleNamespace(id="u-1", name="Demo"),
        SimpleNamespace(id="u-2", name="Other"),
    ]
    client._user_api = cast(
        Any, SimpleNamespace(get_users_query=lambda: SimpleNamespace(items=users))
    )

    resolved = client._resolve_user()
    assert resolved.id == "u-1"

    client._user = "  "
    with pytest.raises(ValueError):
        client._resolve_user()

    client._user = "missing"
    with pytest.raises(ValueError):
        client._resolve_user()

    client._user = "demo"
    client._user_id = "u-1"
    section_items = [
        SimpleNamespace(id="sec-1", name="Movies", collection_type="movies"),
        SimpleNamespace(id="sec-2", name="Shows", collection_type="tvshows"),
        SimpleNamespace(id="sec-3", name="Music", collection_type="music"),
    ]
    client._section_filter = {"shows"}
    client._user_views_api = cast(
        Any,
        SimpleNamespace(
            get_users_by_userid_views=lambda user_id, include_external_content=False: (
                SimpleNamespace(items=section_items)
            )
        ),
    )

    sections = client._load_sections()
    assert [s.id for s in sections] == ["sec-2"]


def test_load_show_metadata_fetchers_and_item_filters(
    emby_client_instance: EmbyClient,
) -> None:
    """Metadata fetchers and section item filtering should cover watched branches."""
    client = emby_client_instance
    client._user_id = "u-1"

    virtual_folders = [
        SimpleNamespace(
            item_id="sec-shows",
            collection_type="tvshows",
            library_options=SimpleNamespace(
                type_options=[
                    SimpleNamespace(
                        type="Series",
                        metadata_fetcher_order=["AniDb", "AniList"],
                        metadata_fetchers=["AniList"],
                    )
                ]
            ),
        )
    ]
    client._library_structure_api = cast(
        Any,
        SimpleNamespace(
            get_library_virtualfolders_query=lambda: SimpleNamespace(
                items=virtual_folders
            )
        ),
    )
    assert client._load_show_metadata_fetcher_orders() == {"sec-shows": ("AniList",)}
    assert client._load_show_metadata_fetchers() == {"sec-shows": "AniList"}

    now = datetime.now(UTC)
    section = SimpleNamespace(id="sec-shows", collection_type="tvshows")
    watched_episode = SimpleNamespace(
        id="ep-1",
        series_id="show-1",
        parent_id=None,
        user_data=SimpleNamespace(last_played_date=(now - timedelta(minutes=1))),
        date_created=(now - timedelta(minutes=1)),
    )
    show = SimpleNamespace(
        id="show-1",
        series_id=None,
        parent_id=None,
        user_data=None,
        date_created=(now - timedelta(minutes=1)),
    )
    client._items_api = cast(Any, _FakeItemsApi(responses=[[watched_episode], [show]]))

    items = client._fetch_section_items(
        cast(Any, section),
        require_watched=True,
        min_last_modified=now - timedelta(hours=1),
    )
    assert [item.id for item in items] == ["show-1"]


def test_fetch_section_items_uses_native_user_timestamp_for_polled_tv_items(
    emby_client_instance: EmbyClient,
) -> None:
    """Poll filtering should use Emby's user timestamp query support."""
    client = emby_client_instance
    client._user_id = "u-1"

    now = datetime.now(UTC)
    section = SimpleNamespace(id="sec-shows", collection_type="tvshows")
    watched_episode = SimpleNamespace(
        id="ep-1",
        type="Episode",
        series_id="show-1",
        parent_id=None,
        user_data=SimpleNamespace(played=True, play_count=0, last_played_date=None),
        date_created=now - timedelta(days=30),
    )
    show = SimpleNamespace(
        id="show-1",
        type="Series",
        user_data=None,
        date_created=now - timedelta(days=30),
    )
    client._items_api = cast(Any, _FakeItemsApi(responses=[[watched_episode], [show]]))

    items = client._fetch_section_items(
        cast(Any, section),
        require_watched=True,
        min_last_modified=now - timedelta(hours=1),
    )

    assert [item.id for item in items] == ["show-1"]
    assert (
        client._items_api.calls[0]["min_date_last_saved_for_user"]
        == (now - timedelta(hours=1)).isoformat()
    )


def test_filter_items_by_last_modified(emby_client_instance: EmbyClient) -> None:
    """Last modified filtering should consider created and user-played timestamps."""
    now = datetime.now(UTC)

    stale = SimpleNamespace(
        id="stale",
        date_created=(now - timedelta(days=2)),
        user_data=None,
    )
    fresh_created = SimpleNamespace(
        id="fresh-created",
        date_created=now,
        user_data=None,
    )
    fresh_played = SimpleNamespace(
        id="fresh-played",
        date_created=(now - timedelta(days=10)),
        user_data=SimpleNamespace(last_played_date=now),
    )

    filtered = emby_client_instance._filter_items_by_last_modified(
        cast(Any, [stale, fresh_created, fresh_played]),
        now - timedelta(hours=1),
    )

    assert [item.id for item in filtered] == ["fresh-created", "fresh-played"]
