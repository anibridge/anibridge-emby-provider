"""Emby provider configuration."""

from typing import Annotated

import msgspec


class EmbyProviderConfig(msgspec.Struct, kw_only=True):
    """Configuration for the Emby provider."""

    url: Annotated[
        str,
        msgspec.Meta(description="The base URL of the Emby server."),
    ]
    token: Annotated[
        str,
        msgspec.Meta(description="The Emby API token."),
    ]
    user: Annotated[
        str,
        msgspec.Meta(description="The Emby user to synchronize."),
    ]
    sections: Annotated[
        list[str],
        msgspec.Meta(
            description=(
                "A list of Emby library section names to constrain synchronization to."
            )
        ),
    ] = msgspec.field(default_factory=list)
    genres: Annotated[
        list[str],
        msgspec.Meta(description="A list of genres to constrain synchronization to."),
    ] = msgspec.field(default_factory=list)
    strict: Annotated[
        bool,
        msgspec.Meta(
            description="Whether to enforce strict matching when resolving mappings."
        ),
    ] = True
