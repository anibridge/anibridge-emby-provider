"""Emby webhook implementation."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field
from starlette.requests import Request


class EmbyWebhookServer(BaseModel):
    """Minimal Emby server payload."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(..., alias="Id")


class EmbyWebhookUser(BaseModel):
    """Minimal Emby user payload."""

    model_config = ConfigDict(extra="ignore")

    id: str | None = Field(None, alias="Id")


class EmbyWebhookItem(BaseModel):
    """Subset of Emby item fields used by AniBridge webhook logic."""

    model_config = ConfigDict(extra="ignore")

    id: str | None = Field(None, alias="Id")
    type: str | None = Field(None, alias="Type")
    series_id: str | None = Field(None, alias="SeriesId")
    parent_id: str | None = Field(None, alias="ParentId")


class EmbyWebhookPayload(BaseModel):
    """Emby Webhook."""

    model_config = ConfigDict(extra="ignore")

    event: str = Field(..., alias="Event")
    server: EmbyWebhookServer = Field(..., alias="Server")
    item: EmbyWebhookItem | None = Field(None, alias="Item")
    user: EmbyWebhookUser | None = Field(None, alias="User")


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


class EmbyWebhook(BaseModel):
    """Represents a normalized Emby webhook payload."""

    model_config = ConfigDict(extra="ignore")

    payload: EmbyWebhookPayload

    @property
    def event(self) -> str:
        """Raw event string from Emby."""
        return self.payload.event

    @property
    def event_type(self) -> EmbyWebhookEventType | None:
        """Webhook event type normalized to enum values."""
        raw = (self.payload.event or "").strip().lower()
        try:
            return EmbyWebhookEventType(raw)
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
        return content_type.split(";", 1)[0].strip().lower()

    @classmethod
    async def from_request(cls, request: Request) -> EmbyWebhook:
        """Create an Emby webhook instance from an incoming HTTP request."""
        content_type = cls.media_type(request.headers.get("content-type"))

        if content_type in ("multipart/form-data", "application/x-www-form-urlencoded"):
            form = await request.form()
            payload_raw = form.get("data")

            if not payload_raw:
                raise ValueError("Missing 'data' field in form request")

            if isinstance(payload_raw, bytes):
                payload_raw = payload_raw.decode("utf-8", "replace")

            try:
                payload = EmbyWebhookPayload.model_validate_json(str(payload_raw))
            except Exception as e:
                raise ValueError(
                    f"Invalid Emby payload JSON in 'data' field: {e}"
                ) from e

            return EmbyWebhook(payload=payload)

        if content_type == "application/json":
            try:
                data = await request.json()
            except Exception as e:
                raise ValueError(f"Invalid JSON body: {e}") from e

            if isinstance(data, str):
                try:
                    payload = EmbyWebhookPayload.model_validate_json(data)
                except Exception as e:
                    raise ValueError(f"Invalid Emby webhook payload: {e}") from e
                return EmbyWebhook(payload=payload)

            if not isinstance(data, dict):
                raise ValueError("Invalid payload structure: expected JSON object")

            try:
                payload = EmbyWebhookPayload.model_validate(data)
            except Exception as e:
                raise ValueError(f"Invalid Emby webhook payload: {e}") from e

            return EmbyWebhook(payload=payload)

        raise ValueError(
            f"Unsupported content type '{content_type}' for Emby webhook "
            "(expected multipart/form-data or application/json)"
        )
