"""Unit tests for Station Redis queue operations.

Tests passenger/cargo addition, pickup removal, state snapshot accuracy,
and congestion level calculation.
"""

import json

import pytest

from aexis.core.message_bus import MessageBus
from aexis.core.model import Priority, StationStatus
from aexis.station import Station


class TestPassengerQueue:
    """Passenger queue operations via Redis."""

    async def test_passenger_arrival_writes_to_redis(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_passenger_arrival({
            "station_id": "1",
            "passenger_id": "p_001",
            "destination": "2",
            "priority": Priority.NORMAL.value,
        })

        raw = await redis_client.hget(
            "aexis:station:1:passengers", "p_001"
        )
        assert raw is not None
        data = json.loads(raw)
        assert data["destination"] == "2"
        assert station._passenger_count == 1

    async def test_ignores_arrivals_for_other_stations(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_passenger_arrival({
            "station_id": "2",
            "passenger_id": "p_001",
            "destination": "3",
        })

        count = await redis_client.hlen("aexis:station:1:passengers")
        assert count == 0
        assert station._passenger_count == 0

    async def test_passenger_pickup_removes_from_queue(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        # Add passenger
        await station._handle_passenger_arrival({
            "station_id": "1",
            "passenger_id": "p_001",
            "destination": "2",
        })

        # Pickup
        await station._handle_passenger_pickup({
            "station_id": "1",
            "passenger_id": "p_001",
        })

        remaining = await redis_client.hlen(
            "aexis:station:1:passengers"
        )
        assert remaining == 0
        assert station.total_passengers_processed == 1

    async def test_multiple_passengers_independent(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        for i in range(5):
            await station._handle_passenger_arrival({
                "station_id": "1",
                "passenger_id": f"p_{i:03d}",
                "destination": "2",
            })

        assert station._passenger_count == 5
        count = await redis_client.hlen(
            "aexis:station:1:passengers"
        )
        assert count == 5

        # Pick up just one
        await station._handle_passenger_pickup({
            "station_id": "1",
            "passenger_id": "p_002",
        })

        remaining = await redis_client.hlen(
            "aexis:station:1:passengers"
        )
        assert remaining == 4


class TestCargoQueue:
    """Cargo queue operations via Redis."""

    async def test_cargo_request_writes_to_redis(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_cargo_request({
            "origin": "1",
            "request_id": "c_001",
            "destination": "3",
            "weight": 75.0,
        })

        raw = await redis_client.hget(
            "aexis:station:1:cargo", "c_001"
        )
        assert raw is not None
        data = json.loads(raw)
        assert data["weight"] == 75.0
        assert station._cargo_count == 1

    async def test_cargo_loading_removes_from_queue(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_cargo_request({
            "origin": "1",
            "request_id": "c_001",
            "destination": "3",
            "weight": 50.0,
        })

        await station._handle_cargo_loading({
            "station_id": "1",
            "request_id": "c_001",
        })

        remaining = await redis_client.hlen("aexis:station:1:cargo")
        assert remaining == 0
        assert station.total_cargo_processed == 1


class TestCongestionLevel:
    """Congestion calculation and threshold behavior."""

    async def test_congestion_increases_with_queue(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")
        assert station.congestion_level == 0.0

        for i in range(15):
            await station._handle_passenger_arrival({
                "station_id": "1",
                "passenger_id": f"p_{i:03d}",
                "destination": "2",
            })

        # 15 passengers / 20 max = 0.75 * 0.4 (weight) = 0.3 contribution
        # Plus bay utilization: all 4 bays free = 0 * 0.3 = 0
        # Total ~0.3
        assert station.congestion_level > 0.2

    async def test_congestion_threshold_changes_status(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")

        # Flood with passengers and cargo to push congestion > 0.8
        for i in range(25):
            await station._handle_passenger_arrival({
                "station_id": "1",
                "passenger_id": f"p_{i:03d}",
                "destination": "2",
            })
        for i in range(15):
            await station._handle_cargo_request({
                "origin": "1",
                "request_id": f"c_{i:03d}",
                "destination": "3",
                "weight": 10.0,
            })
            
        station.available_bays = 0
        station._update_congestion_level()

        assert station.status == StationStatus.CONGESTED

    async def test_bay_utilization_affects_congestion(
        self, message_bus, redis_client
    ):
        station = Station(message_bus, redis_client, "1")
        station.loading_bays = 4
        station.available_bays = 0  # All bays occupied

        station._update_congestion_level()
        # Bay utilization: 1.0 * 0.3 = 0.3
        assert station.congestion_level >= 0.3


class TestStateSnapshot:
    """State snapshot written to Redis for API reads."""

    async def test_state_written_to_redis(self, message_bus, redis_client):
        station = Station(message_bus, redis_client, "1")
        station.connected_stations = ["2", "3"]

        await station._publish_state_snapshot()

        raw = await redis_client.get("aexis:station:1:state")
        assert raw is not None
        state = json.loads(raw)
        assert state["station_id"] == "1"
        assert state["status"] == "operational"
        assert "2" in state["connected_stations"]

    async def test_state_includes_queue_counts(self, message_bus, redis_client):
        station = Station(message_bus, redis_client, "1")

        for i in range(3):
            await station._handle_passenger_arrival({
                "station_id": "1",
                "passenger_id": f"p_{i}",
                "destination": "2",
            })

        raw = await redis_client.get("aexis:station:1:state")
        state = json.loads(raw)
        assert state["queues"]["passengers"]["waiting"] == 3
