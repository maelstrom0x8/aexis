
import asyncio
import logging
import os
import sys

import redis.asyncio as redis
import uvicorn

from aexis.api.routes import SystemAPI
from aexis.core.logging_config import setup_logging
from aexis.core.message_bus import MessageBus

logger = logging.getLogger(__name__)

async def main():

    setup_logging("api")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_password = os.getenv("REDIS_PASSWORD")

    if not redis_password:
        logger.error("REDIS_PASSWORD environment variable is required")
        return

    redis_client = redis.from_url(
        redis_url,
        password=redis_password,
        decode_responses=True,
        socket_connect_timeout=10,
        retry_on_timeout=True,
    )
    try:
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.error(f"Cannot connect to Redis at {redis_url}: {e}")
        return

    message_bus = MessageBus(redis_url=redis_url, password=redis_password)
    if not await message_bus.connect():
        logger.error("Failed to connect MessageBus")
        return

    bus_task = asyncio.create_task(message_bus.start_listening())

    api = SystemAPI(redis_client, message_bus)
    await api.start_listeners()
    app = api.get_app()

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8001"))
    logger.info(f"Starting AEXIS API on {host}:{port}")

    config = uvicorn.Config(app=app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        bus_task.cancel()
        await message_bus.stop_listening()
        await message_bus.disconnect()
        await redis_client.aclose()
        logger.info("API shut down")

if __name__ == "__main__":
    asyncio.run(main())
