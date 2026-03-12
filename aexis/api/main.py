import asyncio
import logging
import os
import sys

import uvicorn
from aexis.api.routes import SystemAPI
from aexis.core.system import AexisSystem, SystemContext

# Configure logging
logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(
        sys.stdout), logging.FileHandler("aexis_core.log")],
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point for Core System & API"""
    try:
        # Check environment
        required_vars = ["REDIS_PASSWORD"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            logger.error(
                f"Missing required environment variables: {missing_vars}")
            return

        logger.info("Starting AEXIS Core System...")

        context = await SystemContext.initialize('/home/godelhaze/dev/megalith/aexis/aexis/aexis.json')

        # Initialize System
        system = AexisSystem(context)
        if not await system.initialize():
            logger.error("Failed to initialize system")
            return

        # Start System Logic (background task)
        system_task = asyncio.create_task(system.start())

        # Initialize API
        api = SystemAPI(system)
        app = api.get_app()

        # API Configuration
        host = os.getenv("API_HOST", "0.0.0.0")
        port = int(os.getenv("API_PORT", "8001"))

        logger.warning(
            f"Starting System API on {host}:{port} from {os.getcwd()}")

        config = uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level="warning",
        )
        server = uvicorn.Server(config)

        # Run server and system concurrently
        # We need to manage the shutdown sequence carefully

        try:
            await server.serve()
        finally:
            logger.info("Stopping system...")
            await system.shutdown()
            await system_task

    except Exception as e:
        logger.error(f"Core System failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
