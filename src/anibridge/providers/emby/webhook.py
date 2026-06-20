"""Emby webhook implementation."""

from collections.abc import Mapping
from enum import StrEnum

import msgspec


class EmbyWebhookServer(msgspec.Struct, rename="pascal"):
    """Minimal Emby server payload."""

    id: str


class EmbyWebhookUser(msgspec.Struct, rename="pascal"):
    """Minimal Emby user payload."""

    id: str | None = None


class EmbyWebhookItem(
    msgspec.Struct,
    rename="pascal",
):
    """Subset of Emby item fields used by AniBridge webhook logic."""

    id: str | None = None
    type: str | None = None
    series_id: str | None = None
    parent_id: str | None = None


class EmbyWebhookPayload(
    msgspec.Struct,
    rename="pascal",
):
    """Emby Webhook."""

    event: str
    server: EmbyWebhookServer
    item: EmbyWebhookItem | None = None
    user: EmbyWebhookUser | None = None


class EmbyWebhookEventType(StrEnum):
    """Enumeration of normalized Emby webhook event types."""

    ITEM_MARK_FAVORITE = "item.markfavorite"
    ITEM_MARK_PLAYED = "item.markplayed"
    ITEM_MARK_UNFAVORITE = "item.markunfavorite"
    ITEM_MARK_UNPLAYED = "item.markunplayed"
    ITEM_RATE = "item.rate"
    LIBRARY_DELETED = "library.deleted"
    LIBRARY_NEW = "library.new"
    PLAYBACK_PAUSE = "playback.pause"
    PLAYBACK_START = "playback.start"
    PLAYBACK_STOP = "playback.stop"
    PLAYBACK_UNPAUSE = "playback.unpause"
    PLUGIN_INSTALLED = "plugins.plugininstalled"
    SCHEDULED_TASK_COMPLETED = "scheduledtasks.completed"
    SYSTEM_NOTIFICATION_TEST = "system.notificationtest"
    SYSTEM_SERVER_RESTART_REQUIRED = "system.serverrestartrequired"
    SYSTEM_SERVER_STARTUP = "system.serverstartup"
    USER_AUTHENTICATED = "user.authenticated"
    USER_AUTHENTICATION_FAILED = "user.authenticationfailed"
    USER_DELETED = "user.deleted"
    USER_PASSWORD_CHANGED = "user.passwordchanged"
    USER_POLICY_UPDATED = "user.policyupdated"


class EmbyWebhook(msgspec.Struct):
    """Represents a normalized Emby webhook payload."""

    payload: EmbyWebhookPayload

    @property
    def event(self) -> str:
        """Raw event string from Emby."""
        return self.payload.event

    @property
    def event_type(self) -> EmbyWebhookEventType | None:
        """Webhook event type normalized to enum values."""
        try:
            return EmbyWebhookEventType(self.payload.event)
        except ValueError:
            return None

    @property
    def account_id(self) -> str | None:
        """The webhook user's Emby account ID, if present."""
        return self.payload.user.id if self.payload.user else None

    @property
    def top_level_item_id(self) -> str | None:
        """The top-level media item ID for the payload."""
        item = self.payload.item
        if not item:
            return None
        return item.series_id or item.parent_id or item.id


class WebhookParser:
    """Parser for incoming Emby webhooks."""

    @staticmethod
    def media_type(content_type: str | None) -> str:
        """Read the media type portion of a Content-Type header."""
        if not content_type:
            return ""
        return content_type.split(";", 1)[0].strip()

    @classmethod
    def from_inbound(
        cls,
        *,
        headers: Mapping[str, str],
        body: bytes,
    ) -> EmbyWebhook:
        """Create a webhook instance from provider-base inbound request parts."""
        content_type = cls.media_type(headers.get("content-type"))
        if content_type not in ("application/json", ""):
            raise ValueError("Provider-base Emby webhooks require a JSON payload")

        try:
            payload = msgspec.json.decode(body, type=EmbyWebhookPayload)
        except Exception as exc:
            raise ValueError(f"Invalid Emby webhook payload: {exc}") from exc

        return EmbyWebhook(payload=payload)
