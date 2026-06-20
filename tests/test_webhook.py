"""Tests for Emby webhook parser helpers."""

import json
from typing import cast

_USER_ID = "user-1"
_PRIMARY_SERIES_ID = "series-1"
_SECONDARY_SERIES_ID = "series-2"


def _sample_json(payload: dict[str, object]) -> str:
    return json.dumps(payload)


def _item_payload(
    *,
    item_id: str,
    series_id: str,
    item_name: str,
    is_favorite: bool,
    played: bool,
) -> dict[str, object]:
    return {
        "Name": item_name,
        "ServerId": "server-1",
        "Id": item_id,
        "Path": f"/library/{series_id}/{item_id}.mkv",
        "Type": "Episode",
        "ParentId": f"season-{series_id}",
        "SeriesName": f"Series {series_id}",
        "SeriesId": series_id,
        "SeasonId": f"season-{series_id}",
        "UserData": {
            "PlayCount": 1 if played else 0,
            "IsFavorite": is_favorite,
            "Played": played,
        },
    }


_MARK_PLAYED_SAMPLE = _sample_json(
    {
        "Title": "Sample User marked Episode 1 as played",
        "Date": "2026-04-17T04:17:33.3548468Z",
        "Event": "item.markplayed",
        "Severity": "Info",
        "User": {"Name": "Sample User", "Id": _USER_ID},
        "Item": _item_payload(
            item_id="episode-1",
            series_id=_PRIMARY_SERIES_ID,
            item_name="Episode 1",
            is_favorite=False,
            played=True,
        ),
        "Server": {"Name": "test-server", "Id": "server-1", "Version": "4.9.3.0"},
    }
)

_RATE_SAMPLE = _sample_json(
    {
        "Title": "Sample User updated a favorite entry",
        "Date": "2026-04-16T18:10:43.4463506Z",
        "Event": "item.rate",
        "Severity": "Info",
        "User": {"Name": "Sample User", "Id": _USER_ID},
        "Item": _item_payload(
            item_id="episode-2",
            series_id=_PRIMARY_SERIES_ID,
            item_name="Episode 2",
            is_favorite=True,
            played=False,
        ),
        "Server": {"Name": "test-server", "Id": "server-1", "Version": "4.9.3.0"},
    }
)

_SUMMER_RATE_SAMPLE = _sample_json(
    {
        "Title": "Sample User updated another favorite entry",
        "Date": "2026-04-17T04:25:54.0738804Z",
        "Event": "item.rate",
        "Severity": "Info",
        "User": {"Name": "Sample User", "Id": _USER_ID},
        "Item": _item_payload(
            item_id="episode-3",
            series_id=_SECONDARY_SERIES_ID,
            item_name="Episode 3",
            is_favorite=True,
            played=True,
        ),
        "Server": {"Name": "test-server", "Id": "server-1", "Version": "4.9.3.0"},
    }
)


def _decoded_payload(raw: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads(raw))
