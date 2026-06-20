"""Emby API models."""

from datetime import datetime

import msgspec


class EmbyUserData(msgspec.Struct, rename="pascal", kw_only=True):
    """Subset of Emby user data used by AniBridge."""

    played: bool | None = None
    play_count: int | None = None
    playback_position_ticks: int | None = None
    is_favorite: bool | None = None
    rating: float | None = None
    last_played_date: datetime | None = None


class EmbyItem(msgspec.Struct, rename="pascal", kw_only=True):
    """Subset of Emby item metadata used by AniBridge."""

    id: str | None = None
    name: str | None = None
    type: str | None = None
    collection_type: str | None = None
    parent_id: str | None = None
    series_id: str | None = None
    season_id: str | None = None
    index_number: int | None = None
    parent_index_number: int | None = None
    production_year: int | None = None
    provider_ids: dict[str, object] | None = None
    image_tags: dict[str, str] | None = None
    user_data: EmbyUserData | None = None
    date_created: datetime | None = None
    date_last_media_added: datetime | None = None
    date_last_saved: datetime | None = None


class EmbyUser(msgspec.Struct, rename="pascal", kw_only=True):
    """Subset of Emby user metadata used by AniBridge."""

    id: str | None = None
    name: str | None = None


class EmbyTypeOptions(msgspec.Struct, rename="pascal", kw_only=True):
    """Media-type library options relevant to provider id ordering."""

    type: str | None = None
    metadata_fetcher_order: tuple[str, ...] = ()
    metadata_fetchers: tuple[str, ...] = ()


class EmbyLibraryOptions(msgspec.Struct, rename="pascal", kw_only=True):
    """Library options relevant to provider id ordering."""

    type_options: tuple[EmbyTypeOptions, ...] = ()


class EmbyVirtualFolder(msgspec.Struct, rename="pascal", kw_only=True):
    """Subset of Emby virtual folder metadata used by AniBridge."""

    item_id: str | None = None
    collection_type: str | None = None
    library_options: EmbyLibraryOptions | None = None


class EmbyItemPage(msgspec.Struct, rename="pascal", kw_only=True):
    """Paged Emby item response."""

    items: tuple[EmbyItem, ...] = ()
    total_record_count: int | None = None


class EmbySectionItemsPage(msgspec.Struct, frozen=True):
    """One window of top-level Emby section items."""

    items: tuple[EmbyItem, ...] = ()
    next_offset: int | None = None
