"""Tests for Emby webhook parser helpers."""

import json
from typing import cast

import pytest
from starlette.requests import Request

from anibridge.providers.library.emby.webhook import (
    EmbyWebhookEventType,
    WebhookParser,
)

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


class _FakeRequest:
    def __init__(
        self, *, headers: dict[str, str], json_payload=None, form_payload=None
    ):
        self.headers = headers
        self._json_payload = json_payload
        self._form_payload = form_payload or {}

    async def json(self):
        if isinstance(self._json_payload, Exception):
            raise self._json_payload
        return self._json_payload

    async def form(self):
        return self._form_payload


def _json_payload(raw: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads(raw))


@pytest.mark.asyncio
async def test_webhook_parses_markplayed_sample() -> None:
    """The mark-played sample should normalize to the series sync key."""
    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload=_json_payload(_MARK_PLAYED_SAMPLE),
    )

    webhook = await WebhookParser.from_request(cast(Request, request))
    assert webhook.event == "item.markplayed"
    assert webhook.event_type == EmbyWebhookEventType.ITEM_MARK_PLAYED
    assert webhook.account_id == _USER_ID
    assert webhook.top_level_item_id == _PRIMARY_SERIES_ID


@pytest.mark.parametrize(
    ("raw_payload", "expected_top_level_item_id"),
    [
        (_RATE_SAMPLE, _PRIMARY_SERIES_ID),
        (_SUMMER_RATE_SAMPLE, _SECONDARY_SERIES_ID),
    ],
)
@pytest.mark.asyncio
async def test_webhook_parses_item_rate_samples(
    raw_payload: str,
    expected_top_level_item_id: str,
) -> None:
    """The item.rate samples should parse as syncable favorite/rating events."""
    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload=_json_payload(raw_payload),
    )

    webhook = await WebhookParser.from_request(cast(Request, request))
    assert webhook.event == "item.rate"
    assert webhook.event_type == EmbyWebhookEventType.ITEM_RATE
    assert webhook.account_id == _USER_ID
    assert webhook.top_level_item_id == expected_top_level_item_id


@pytest.mark.asyncio
async def test_webhook_from_form_data_field_json_string() -> None:
    """Form payloads should accept the JSON string Emby posts in the data field."""
    request = _FakeRequest(
        headers={"content-type": "multipart/form-data; boundary=abc"},
        form_payload={"data": _MARK_PLAYED_SAMPLE},
    )

    webhook = await WebhookParser.from_request(cast(Request, request))
    assert webhook.event_type == EmbyWebhookEventType.ITEM_MARK_PLAYED
    assert webhook.top_level_item_id == _PRIMARY_SERIES_ID


@pytest.mark.asyncio
async def test_webhook_form_payload_field_is_rejected() -> None:
    """The legacy payload form field should no longer be accepted."""
    request = _FakeRequest(
        headers={"content-type": "multipart/form-data; boundary=abc"},
        form_payload={"payload": _MARK_PLAYED_SAMPLE},
    )

    with pytest.raises(ValueError, match="Missing 'data' field"):
        await WebhookParser.from_request(cast(Request, request))


@pytest.mark.asyncio
async def test_webhook_string_json_body_is_decoded() -> None:
    """JSON bodies that deserialize to a stringified payload should still parse."""
    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload=_MARK_PLAYED_SAMPLE,
    )

    webhook = await WebhookParser.from_request(cast(Request, request))
    assert webhook.event_type == EmbyWebhookEventType.ITEM_MARK_PLAYED
    assert webhook.top_level_item_id == _PRIMARY_SERIES_ID


@pytest.mark.asyncio
async def test_webhook_legacy_payload_rejected() -> None:
    """Legacy flat webhook payloads should be rejected."""
    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload={
            "NotificationType": "PlaybackStop",
            "ItemType": "Movie",
            "ItemId": "m1",
        },
    )

    with pytest.raises(ValueError, match="Invalid Emby webhook payload"):
        await WebhookParser.from_request(cast(Request, request))


@pytest.mark.asyncio
async def test_webhook_invalid_form_payload_raises() -> None:
    """Invalid JSON in multipart payload should raise ValueError."""
    request = _FakeRequest(
        headers={"content-type": "application/x-www-form-urlencoded"},
        form_payload={"data": "{not-valid-json"},
    )

    with pytest.raises(ValueError, match="Invalid Emby payload JSON"):
        await WebhookParser.from_request(cast(Request, request))


@pytest.mark.asyncio
async def test_webhook_form_payload_missing_payload_field_raises() -> None:
    """Form requests must send the nested webhook in the data payload field."""
    request = _FakeRequest(
        headers={"content-type": "application/x-www-form-urlencoded"},
        form_payload={"Event": "playback.stop"},
    )

    with pytest.raises(ValueError, match="Missing 'data' field"):
        await WebhookParser.from_request(cast(Request, request))


@pytest.mark.asyncio
async def test_webhook_invalid_json_body_raises() -> None:
    """Invalid JSON body should raise ValueError."""
    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload=RuntimeError("bad body"),
    )

    with pytest.raises(ValueError, match="Invalid JSON body"):
        await WebhookParser.from_request(cast(Request, request))


@pytest.mark.asyncio
async def test_webhook_invalid_payload_structure_raises() -> None:
    """Non-object payloads should be rejected."""
    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload=["not", "an", "object"],
    )

    with pytest.raises(ValueError, match="Invalid payload structure"):
        await WebhookParser.from_request(cast(Request, request))


@pytest.mark.asyncio
async def test_webhook_unknown_event_parses_as_untyped_event() -> None:
    """Unsupported event values should parse and surface a None event_type."""
    payload = _json_payload(_MARK_PLAYED_SAMPLE)
    payload["Event"] = "item.custom-event"

    request = _FakeRequest(
        headers={"content-type": "application/json"},
        json_payload=payload,
    )

    webhook = await WebhookParser.from_request(cast(Request, request))
    assert webhook.event == "item.custom-event"
    assert webhook.event_type is None
