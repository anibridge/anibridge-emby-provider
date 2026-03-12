"""Emby provider configuration."""

from pydantic import BaseModel, Field


class EmbyProviderConfig(BaseModel):
    """Configuration for the Emby provider."""

    url: str = Field(
        default=...,
        description="The base URL of the Emby server.",
    )
    token: str = Field(
        default=...,
        description="The Emby API token.",
    )
    user: str = Field(
        default=...,
        description="The Emby user to synchronize.",
    )
    sections: list[str] = Field(
        default_factory=list,
        description=(
            "A list of Emby library section names to constrain synchronization to."
        ),
    )
    genres: list[str] = Field(
        default_factory=list,
        description="A list of genres to constrain synchronization to.",
    )
    strict: bool = Field(
        default=True,
        description="Whether to enforce strict matching when resolving mappings.",
    )
