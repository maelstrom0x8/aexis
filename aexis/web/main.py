import asyncio
import logging
import os
import sys

import uvicorn

from aexis.web.dashboard import WebDashboard

# Configure logging
logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point for Dashboard Service"""
    try:
        host = os.getenv("UI_HOST", "0.0.0.0")
        port = int(os.getenv("UI_PORT", "8000"))
        api_url = os.getenv("API_URL", "http://localhost:8001")

        logger.info(f"Starting Dashboard Service on {host}:{port}")
        logger.info(f"Connecting to API at: {api_url}")

        dashboard = WebDashboard(api_base_url=api_url)
        app = dashboard.get_app()

        config = uvicorn.Config(app=app, host=host, port=port, log_level="warning")
        server = uvicorn.Server(config)
        await server.serve()

    except Exception as e:
        logger.error(f"Dashboard Service failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
