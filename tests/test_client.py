"""Tests for the direct Emby REST client."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any, cast

import msgspec
import pytest

from anibridge.providers.emby.client import EmbyClient, _ContinueWatchingCacheEntry
from anibridge.providers.emby.models import EmbyItem, EmbyUserData


def _client() -> EmbyClient:
    return EmbyClient(
        logger=cast(Any, getLogger("tests.emby.client")),
        url="http://emby",
        token="token",
        user="Demo",
    )


class _JsonResponse:
    def __init__(
        self,
        payload: object,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.payload = payload
        self.headers = dict(headers or {"Content-Type": "application/json"})

    async def __aenter__(self) -> _JsonResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def read(self) -> bytes:
        return msgspec.json.encode(self.payload)


class _ImageResponse(_JsonResponse):
    async def read(self) -> bytes:
        return cast(bytes, self.payload)


class _Session:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []
        self.closed = False

    def request(self, method: str, url: str, **kwargs: object) -> _JsonResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return _JsonResponse(self.responses.pop(0))

    def get(self, url: str, **kwargs: object) -> _JsonResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        if self.responses:
            return _JsonResponse(self.responses.pop(0))
        return _ImageResponse(b"poster", headers={"Content-Type": "image/png"})

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_initialize_loads_account_sections_and_fetcher_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Initialization should use Emby REST endpoints without the generated SDK."""
    client = _client()
    session = _Session(
        [
            {"Version": "4.9.3.0"},
            [{"Id": "u-1", "Name": "Demo"}],
            {
                "Items": [
                    {"Id": "sec-movies", "Name": "Movies", "CollectionType": "movies"},
                    {"Id": "sec-tv", "Name": "Anime", "CollectionType": "tvshows"},
                    {"Id": "music", "Name": "Music", "CollectionType": "music"},
                ]
            },
            [
                {
                    "ItemId": "sec-tv",
                    "CollectionType": "tvshows",
                    "LibraryOptions": {
                        "TypeOptions": [
                            {
                                "Type": "Series",
                                "MetadataFetcherOrder": ["AniDB", "AniList"],
                                "MetadataFetchers": ["AniList"],
                            }
                        ]
                    },
                }
            ],
        ]
    )
    monkeypatch.setattr(
        client,
        "_configure_client",
        lambda: setattr(client, "_session", session),
    )

    await client.initialize()

    assert client.user_id() == "u-1"
    assert client.user_name() == "Demo"
    assert [section.id for section in client.sections()] == ["sec-movies", "sec-tv"]
    assert client.show_metadata_fetchers_for_section("sec-tv") == ("AniList",)
    assert [call["url"] for call in session.calls] == [
        "http://emby/System/Info",
        "http://emby/Users",
        "http://emby/Users/u-1/Views",
        "http://emby/Library/VirtualFolders",
    ]


@pytest.mark.asyncio
async def test_list_section_items_uses_server_filters() -> None:
    """Section scans should translate provider filters into Emby query params."""
    client = _client()
    session = _Session(
        [
            {
                "Items": [
                    {
                        "Id": "movie-1",
                        "Name": "Movie",
                        "Type": "Movie",
                        "UserData": {"Played": True, "PlayCount": 1},
                    }
                ]
            }
        ]
    )
    client._session = cast(Any, session)
    client._user_id = "u-1"

    items = await client.list_section_items(
        EmbyItem(id="sec", collection_type="movies"),
        require_watched=True,
        keys=("movie-1",),
    )

    assert [item.id for item in items] == ["movie-1"]
    params = cast(dict[str, object], session.calls[0]["params"])
    assert params["IncludeItemTypes"] == "Movie"
    assert params["IsPlayed"] == "true"
    assert params["Ids"] == "movie-1"


@pytest.mark.asyncio
async def test_continue_watching_cache_and_image_fetch() -> None:
    """Next Up and image hydration should use the aiohttp session."""
    client = _client()
    session = _Session(
        [
            {
                "Items": [
                    {
                        "Id": "ep-current",
                        "Type": "Episode",
                        "SeriesId": "show-1",
                        "SeasonId": "season-1",
                    }
                ]
            }
        ]
    )
    client._session = cast(Any, session)
    client._user_id = "u-1"
    section = EmbyItem(id="sec", collection_type="tvshows")

    assert await client.is_on_continue_watching(
        section,
        EmbyItem(id="show-1", type="Series"),
    )
    assert await client.is_on_continue_watching(
        section,
        EmbyItem(id="season-1", type="Season"),
    )
    assert not await client.is_on_continue_watching(
        section,
        EmbyItem(id="other", type="Series"),
    )

    image = await client.fetch_image_as_data_url("http://emby/image.png")
    assert image == "data:image/png;base64,cG9zdGVy"


@pytest.mark.asyncio
async def test_close_releases_session_and_guards_runtime() -> None:
    """Close should release resources and reset initialized state."""
    client = _client()
    session = _Session([])
    client._session = cast(Any, session)
    client._user_id = "u-1"
    client._user_name = "Demo"
    client._sections = [EmbyItem(id="sec")]
    client._continue_cache["sec"] = _ContinueWatchingCacheEntry(
        keys=frozenset({"show"}),
        cached_at=datetime.now(UTC) - timedelta(minutes=5),
    )

    await client.close()

    assert session.closed
    assert client.sections() == ()
    with pytest.raises(RuntimeError):
        client.user_id()


def test_item_json_and_url_helpers() -> None:
    """JSON conversion should normalize the provider-native item shape."""
    client = _client()
    item = msgspec.json.decode(
        msgspec.json.encode(
            {
                "Id": "movie-1",
                "Name": "Movie",
                "Type": "Movie",
                "ProviderIds": {"Tmdb": 123},
                "UserData": {"Played": True, "PlayCount": 2, "Rating": 8.5},
            }
        ),
        type=EmbyItem,
    )

    assert item.type == "Movie"
    assert item.provider_ids == {"Tmdb": 123}
    assert item.user_data == EmbyUserData(played=True, play_count=2, rating=8.5)
    assert client.auth_headers() == {"X-Emby-Token": "token"}
    assert client.build_item_url("movie-1").endswith("id=movie-1")
    assert "api_key=token" in client.build_image_url("movie-1", tag="abc")
