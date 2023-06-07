try:
    import asyncio
    import aiohttp
except ImportError:
    def missing_asyncio_dependencies(*_, **__):
        """Fail if missing dependencies are used."""
        raise NotImplementedError(
            'Missing aiohttp dependency. '
            'Please use `pip install splitio_client[asyncio]` to install the sdk with asyncio support'
        )
    aiohttp = missing_asyncio_dependencies
    asyncio = missing_asyncio_dependencies
