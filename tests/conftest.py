"""Pytest fixtures shared across the provider test-suite."""

from collections.abc import AsyncGenerator
from logging import getLogger
from typing import cast

import pytest
import pytest_asyncio
from anibridge.utils.types import ProviderLogger

from anibridge.providers.library.emby import EmbyLibraryProvider


@pytest.fixture()
def library_provider() -> EmbyLibraryProvider:
    """Return a fresh library provider instance."""
    return EmbyLibraryProvider(
        config={
            "url": "http://emby.example",
            "token": "token",
            "user": "demo",
        },
        logger=cast(ProviderLogger, getLogger("anibridge.providers.library.emby.test")),
    )


@pytest_asyncio.fixture()
async def initialized_library_provider(
    library_provider: EmbyLibraryProvider,
) -> AsyncGenerator[EmbyLibraryProvider]:
    """Return a provider that has run its async initialize hook."""
    await library_provider.initialize()
    yield library_provider
    await library_provider.close()


@pytest_asyncio.fixture()
async def library_section(initialized_library_provider: EmbyLibraryProvider):
    """Return the first available section exposed by the provider."""
    sections = await initialized_library_provider.get_sections()
    assert len(sections)
    return sections[0]
