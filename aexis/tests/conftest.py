import os
import sys

# Add project root to path (3 levels up from tests/conftest.py)
# tests/ -> aexis/ -> repo_root/
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import asyncio

import pytest
from aexis.core.system import AexisSystem


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def load_env():
    """Simple .env loader"""
    env_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        ".env",
    )
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value


@pytest.fixture
async def system_with_mock_redis():
    """Create a system instance with proper config"""
    load_env()

    # Override config for testing
    os.environ["POD_COUNT"] = "5"
    os.environ["STATION_COUNT"] = "4"
    os.environ["AI_PROVIDER"] = "mock"

    system = AexisSystem()

    # Start the system loop in background (start() calls initialize() internally)
    system_task = asyncio.create_task(system.start())

    # Give it time to spin up and subscribe to channels
    await asyncio.sleep(3)
    print(f"Fixture ready: system.running={system.running}")

    yield system

    # Teardown
    await system.shutdown()
    system_task.cancel()
    try:
        await system_task
    except asyncio.CancelledError:
        pass
