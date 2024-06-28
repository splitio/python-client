import sys
try:
    import asyncio
    import aiohttp
    import aiofiles
except ImportError:
    def missing_asyncio_dependencies(*_, **__):
        """Fail if missing dependencies are used."""
        raise NotImplementedError(
            'Missing aiohttp dependency. '
            'Please use `pip install splitio_client[asyncio]` to install the sdk with asyncio support'
        )
    aiohttp = missing_asyncio_dependencies
    asyncio = missing_asyncio_dependencies
    aiofiles = missing_asyncio_dependencies

async def _anext(it):
    return await it.__anext__()
