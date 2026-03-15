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


# ------------------------------------------------------------------
# Topology-varied network fixtures
# ------------------------------------------------------------------

@pytest.fixture
def linear_network():
    """5 stations in a chain: 1→2→3→4→5 (no shortcuts)."""
    nodes = []
    for i in range(1, 6):
        adj = []
        if i > 1:
            adj.append({"node_id": str(i - 1), "weight": 1.0})
        if i < 5:
            adj.append({"node_id": str(i + 1), "weight": 1.0})
        nodes.append({
            "id": str(i),
            "label": str(i),
            "coordinate": {"x": (i - 1) * 100, "y": 0},
            "adj": adj,
        })
    ctx = NetworkContext({"nodes": nodes})
    NetworkContext.set_instance(ctx)
    yield ctx


@pytest.fixture
def star_network():
    """Hub 1 + spokes 2,3,4,5 — all traffic routes through center."""
    hub = {
        "id": "1",
        "label": "1",
        "coordinate": {"x": 0, "y": 0},
        "adj": [
            {"node_id": str(i), "weight": 1.0} for i in range(2, 6)
        ],
    }
    spokes = []
    positions = [(100, 0), (0, 100), (-100, 0), (0, -100)]
    for i, (x, y) in enumerate(positions, start=2):
        spokes.append({
            "id": str(i),
            "label": str(i),
            "coordinate": {"x": x, "y": y},
            "adj": [{"node_id": "1", "weight": 1.0}],
        })
    ctx = NetworkContext({"nodes": [hub] + spokes})
    NetworkContext.set_instance(ctx)
    yield ctx


@pytest.fixture
def ring_network():
    """5 stations in a cycle: 1→2→3→4→5→1."""
    nodes = []
    import math
    for i in range(1, 6):
        angle = 2 * math.pi * (i - 1) / 5
        x = round(100 * math.cos(angle), 2)
        y = round(100 * math.sin(angle), 2)
        prev_id = str(5 if i == 1 else i - 1)
        next_id = str(1 if i == 5 else i + 1)
        nodes.append({
            "id": str(i),
            "label": str(i),
            "coordinate": {"x": x, "y": y},
            "adj": [
                {"node_id": prev_id, "weight": 1.0},
                {"node_id": next_id, "weight": 1.0},
            ],
        })
    ctx = NetworkContext({"nodes": nodes})
    NetworkContext.set_instance(ctx)
    yield ctx


@pytest.fixture
def disconnected_network():
    """Two isolated clusters: A(1↔2), B(3↔4). No cross-edges."""
    nodes = [
        {
            "id": "1", "label": "1",
            "coordinate": {"x": 0, "y": 0},
            "adj": [{"node_id": "2", "weight": 1.0}],
        },
        {
            "id": "2", "label": "2",
            "coordinate": {"x": 100, "y": 0},
            "adj": [{"node_id": "1", "weight": 1.0}],
        },
        {
            "id": "3", "label": "3",
            "coordinate": {"x": 500, "y": 0},
            "adj": [{"node_id": "4", "weight": 1.0}],
        },
        {
            "id": "4", "label": "4",
            "coordinate": {"x": 600, "y": 0},
            "adj": [{"node_id": "3", "weight": 1.0}],
        },
    ]
    ctx = NetworkContext({"nodes": nodes})
    NetworkContext.set_instance(ctx)
    yield ctx


@pytest.fixture
def single_station_network():
    """Degenerate case: 1 node, 0 edges."""
    nodes = [{
        "id": "1", "label": "1",
        "coordinate": {"x": 0, "y": 0},
        "adj": [],
    }]
    ctx = NetworkContext({"nodes": nodes})
    NetworkContext.set_instance(ctx)
    yield ctx


@pytest.fixture
def large_network():
    """50 stations in a grid (roughly 7×7+1) for scale-up tests.

    Each station connects to its grid neighbours (up/down/left/right).
    """
    grid_size = 7
    total = grid_size * grid_size + 1  # 50 stations
    nodes = []
    for idx in range(1, total + 1):
        row = (idx - 1) // grid_size
        col = (idx - 1) % grid_size
        adj = []
        # right neighbour
        right = idx + 1
        if col < grid_size - 1 and right <= total:
            adj.append({"node_id": str(right), "weight": 1.0})
        # left neighbour
        left = idx - 1
        if col > 0 and left >= 1:
            adj.append({"node_id": str(left), "weight": 1.0})
        # down neighbour
        down = idx + grid_size
        if down <= total:
            adj.append({"node_id": str(down), "weight": 1.0})
        # up neighbour
        up = idx - grid_size
        if up >= 1:
            adj.append({"node_id": str(up), "weight": 1.0})
        nodes.append({
            "id": str(idx),
            "label": str(idx),
            "coordinate": {"x": col * 100, "y": row * 100},
            "adj": adj,
        })
    ctx = NetworkContext({"nodes": nodes})
    NetworkContext.set_instance(ctx)
    yield ctx


# ------------------------------------------------------------------
# Pod factory helpers
# ------------------------------------------------------------------

def make_passenger_pod(message_bus, redis_client, station_client, pod_id="1",
                       station_id="1"):
    """Create a PassengerPod docked at the given station."""
    from aexis.pod import PassengerPod
    from aexis.core.model import LocationDescriptor, Coordinate
    from aexis.core.network import NetworkContext

    pod = PassengerPod(message_bus, redis_client, pod_id, station_client)
    nc = NetworkContext.get_instance()
    pos = nc.station_positions.get(station_id, (0, 0))
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id=station_id,
        coordinate=Coordinate(pos[0], pos[1]),
    )
    return pod


def make_cargo_pod(message_bus, redis_client, station_client, pod_id="1",
                   station_id="1"):
    """Create a CargoPod docked at the given station."""
    from aexis.pod import CargoPod
    from aexis.core.model import LocationDescriptor, Coordinate
    from aexis.core.network import NetworkContext

    pod = CargoPod(message_bus, redis_client, pod_id, station_client)
    nc = NetworkContext.get_instance()
    pos = nc.station_positions.get(station_id, (0, 0))
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id=station_id,
        coordinate=Coordinate(pos[0], pos[1]),
    )
    return pod


# ------------------------------------------------------------------
# Data seeding helpers
# ------------------------------------------------------------------

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
