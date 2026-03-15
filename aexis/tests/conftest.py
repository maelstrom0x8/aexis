"""Shared test fixtures for AEXIS microservices unit tests.

Uses fakeredis for Redis operations — no external Redis needed.
Uses LocalMessageBus for event pub/sub.
"""

import asyncio
import json
import os
import sys

import pytest
import fakeredis.aioredis

from aexis.core.message_bus import LocalMessageBus
from aexis.core.model import Coordinate, EdgeSegment
from aexis.core.network import NetworkContext
from aexis.core.station_client import StationClient


# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def redis_client():
    """In-memory Redis for deterministic, fast unit tests."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
async def station_client(redis_client):
    return StationClient(redis_client)


@pytest.fixture
async def message_bus():
    bus = LocalMessageBus()
    await bus.connect()
    yield bus
    await bus.disconnect()


@pytest.fixture
def network_context():
    """Minimal 4-station network for testing."""
    test_network = {
        "nodes": [
            {
                "id": "1",
                "label": "1",
                "coordinate": {"x": 0, "y": 0},
                "adj": [
                    {"node_id": "2", "weight": 1.0},
                    {"node_id": "3", "weight": 1.5},
                ],
            },
            {
                "id": "2",
                "label": "2",
                "coordinate": {"x": 100, "y": 0},
                "adj": [
                    {"node_id": "1", "weight": 1.0},
                    {"node_id": "4", "weight": 1.0},
                ],
            },
            {
                "id": "3",
                "label": "3",
                "coordinate": {"x": 0, "y": 100},
                "adj": [
                    {"node_id": "1", "weight": 1.5},
                    {"node_id": "4", "weight": 1.0},
                ],
            },
            {
                "id": "4",
                "label": "4",
                "coordinate": {"x": 100, "y": 100},
                "adj": [
                    {"node_id": "2", "weight": 1.0},
                    {"node_id": "3", "weight": 1.0},
                ],
            },
        ]
    }
    ctx = NetworkContext(test_network)
    NetworkContext.set_instance(ctx)
    yield ctx


def seed_passenger(redis_client, station_id: str, passenger_id: str, dest: str):
    """Helper to seed a passenger directly into a Redis hash (sync wrapper)."""
    import asyncio

    async def _seed():
        key = f"aexis:station:{station_id}:passengers"
        data = json.dumps({
            "passenger_id": passenger_id,
            "destination": dest,
            "priority": 3,
            "arrival_time": "2026-01-01T00:00:00",
        })
        await redis_client.hset(key, passenger_id, data)

    asyncio.get_event_loop().run_until_complete(_seed())


async def async_seed_passenger(
    redis_client, station_id: str, passenger_id: str, dest: str
):
    key = f"aexis:station:{station_id}:passengers"
    data = json.dumps({
        "passenger_id": passenger_id,
        "destination": dest,
        "priority": 3,
        "arrival_time": "2026-01-01T00:00:00",
    })
    await redis_client.hset(key, passenger_id, data)


async def async_seed_cargo(
    redis_client, station_id: str, request_id: str, dest: str, weight: float
):
    key = f"aexis:station:{station_id}:cargo"
    data = json.dumps({
        "request_id": request_id,
        "destination": dest,
        "weight": weight,
        "arrival_time": "2026-01-01T00:00:00",
    })
    await redis_client.hset(key, request_id, data)
