"""CargoPod-specific tests: pickup, delivery, claim eligibility, arrival flow.

Covers every branch in CargoPod that differs from PassengerPod:
- Weight-based pickup (pre-claimed, opportunistic, overweight skip, duplicate skip)
- Weight decrement on delivery
- Claim eligibility matrix (IDLE/EN_ROUTE × on/off-route × weight-ok/full)
- Arrival flow (lock guard, PodArrival event, 2s docking, deliver→pickup→decide)
- Event filtering (non-CargoRequest events ignored)
- Decision context builder (station / edge / no-segment fallback)
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from aexis.core.model import (
    Coordinate,
    LocationDescriptor,
    PodStatus,
    Route,
)
from aexis.pod import CargoPod
from aexis.tests.conftest import async_seed_cargo, make_cargo_pod


# ======================================================================
# Cargo pickup
# ======================================================================


class TestCargoPickup:
    """_execute_pickup weight logic and event publishing."""

    async def test_preclaimed_cargo_loaded(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        await async_seed_cargo(redis_client, "1", "c_001", "2", 50.0)
        await station_client.claim_cargo("1", "c_001", pod.pod_id)

        await pod._execute_pickup("1")
        assert len(pod.cargo) == 1
        assert pod.cargo[0]["request_id"] == "c_001"
        assert pod.current_weight == 50.0

    async def test_opportunistic_cargo_loaded(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        await async_seed_cargo(redis_client, "1", "c_opp", "2", 30.0)
        # Not pre-claimed — should be opportunistically picked up
        await pod._execute_pickup("1")
        assert any(c["request_id"] == "c_opp" for c in pod.cargo)
        assert pod.current_weight == 30.0

    async def test_overweight_cargo_skipped(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.current_weight = 490.0
        await async_seed_cargo(redis_client, "1", "c_heavy", "2", 20.0)
        await station_client.claim_cargo("1", "c_heavy", pod.pod_id)

        await pod._execute_pickup("1")
        # 490 + 20 > 500 → skipped
        assert not any(c["request_id"] == "c_heavy" for c in pod.cargo)

    async def test_duplicate_cargo_not_loaded_twice(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.cargo = [{"request_id": "c_dup", "destination": "2", "weight": 50.0}]
        pod.current_weight = 50.0

        await async_seed_cargo(redis_client, "1", "c_dup", "2", 50.0)
        await station_client.claim_cargo("1", "c_dup", pod.pod_id)

        await pod._execute_pickup("1")
        dup_count = sum(1 for c in pod.cargo if c["request_id"] == "c_dup")
        assert dup_count == 1

    async def test_zero_weight_cargo_skipped(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Cargo with weight <= 0 is rejected by the pickup logic."""
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        await async_seed_cargo(redis_client, "1", "c_zero", "2", 0.0)
        await station_client.claim_cargo("1", "c_zero", pod.pod_id)

        await pod._execute_pickup("1")
        assert not any(c["request_id"] == "c_zero" for c in pod.cargo)

    async def test_no_remaining_weight_returns_early(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.current_weight = 500.0
        await async_seed_cargo(redis_client, "1", "c_any", "2", 10.0)
        await pod._execute_pickup("1")
        assert len(pod.cargo) == 0

    async def test_multiple_items_summing_to_boundary(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Multiple items that exactly fill remaining capacity."""
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.current_weight = 400.0
        for i in range(4):
            await async_seed_cargo(
                redis_client, "1", f"c_{i}", "2", 25.0
            )
        await pod._execute_pickup("1")
        # 400 + 4*25 = 500 exactly
        assert pod.current_weight == 500.0
        assert len(pod.cargo) == 4


# ======================================================================
# Cargo delivery
# ======================================================================


class TestCargoDelivery:
    """_execute_delivery weight decrement and event publishing."""

    async def test_matching_cargo_delivered(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.cargo = [
            {"request_id": "c_1", "destination": "2", "weight": 60.0},
            {"request_id": "c_2", "destination": "3", "weight": 40.0},
        ]
        pod.current_weight = 100.0

        await pod._execute_delivery("2")
        assert not any(c["request_id"] == "c_1" for c in pod.cargo)
        assert any(c["request_id"] == "c_2" for c in pod.cargo)
        assert pod.current_weight == 40.0

    async def test_no_matching_cargo_returns_early(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.cargo = [{"request_id": "c_1", "destination": "3", "weight": 50.0}]
        pod.current_weight = 50.0
        pod.status = PodStatus.IDLE

        await pod._execute_delivery("2")
        assert len(pod.cargo) == 1
        assert pod.current_weight == 50.0
        assert pod.status == PodStatus.IDLE  # no status transition

    async def test_delivery_status_transitions(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Status should go UNLOADING during delivery, then EN_ROUTE after."""
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.cargo = [{"request_id": "c_1", "destination": "2", "weight": 50.0}]
        pod.current_weight = 50.0

        statuses_seen = []
        original_publish = pod._publish_status_update

        async def tracking_publish():
            statuses_seen.append(pod.status)
            await original_publish()

        pod._publish_status_update = tracking_publish

        await pod._execute_delivery("2")
        assert PodStatus.UNLOADING in statuses_seen
        assert pod.status == PodStatus.EN_ROUTE


# ======================================================================
# Claim eligibility matrix
# ======================================================================


class TestCargoClaimEligibility:
    """Parameterized eligibility for cargo request broadcasts."""

    @pytest.mark.parametrize("status,on_route,weight_ok,has_fields,expected", [
        (PodStatus.IDLE, False, True, True, True),
        (PodStatus.EN_ROUTE, True, True, True, True),
        (PodStatus.EN_ROUTE, False, True, True, False),
        (PodStatus.IDLE, False, False, True, False),      # overweight
        (PodStatus.IDLE, False, True, False, False),       # missing origin
        (PodStatus.LOADING, False, True, True, False),
        (PodStatus.UNLOADING, False, True, True, False),
        (PodStatus.MAINTENANCE, False, True, True, False),
    ])
    async def test_eligibility(
        self, message_bus, redis_client, station_client, network_context,
        status, on_route, weight_ok, has_fields, expected,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.status = status
        if not weight_ok:
            pod.current_weight = 500.0  # full

        if status == PodStatus.EN_ROUTE:
            if on_route:
                pod.current_route = Route(
                    route_id="r", stations=["1", "2"], estimated_duration=5
                )
            else:
                pod.current_route = Route(
                    route_id="r", stations=["3", "4"], estimated_duration=5
                )

        origin = "1" if has_fields else ""
        request_id = "c_elig" if has_fields else ""

        request = {
            "type": "cargo",
            "request_id": request_id,
            "origin": origin,
            "destination": "2",
            "weight": 10.0,
        }

        if has_fields:
            await async_seed_cargo(redis_client, "1", "c_elig", "2", 10.0)

        decision_called = False

        async def mock_decision():
            nonlocal decision_called
            decision_called = True

        pod.make_decision = mock_decision
        await pod._handle_request_broadcast(request)
        assert decision_called == expected


# ======================================================================
# Arrival flow
# ======================================================================


class TestCargoArrivalFlow:
    """_handle_station_arrival orchestration."""

    async def test_arrival_sets_idle_and_publishes_pod_arrival(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.EN_ROUTE

        events_published = []
        original_publish = pod._publish_event

        async def tracking_publish(event):
            events_published.append(event.event_type)
            await original_publish(event)

        pod._publish_event = tracking_publish

        await pod._handle_station_arrival("1")
        assert pod.status in (PodStatus.IDLE, PodStatus.EN_ROUTE)
        assert "PodArrival" in events_published

    async def test_arrival_lock_prevents_concurrent(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.EN_ROUTE

        events_published = []
        original_publish = pod._publish_event

        async def tracking_publish(event):
            events_published.append(getattr(event, 'event_type', str(event)))
            await original_publish(event)

        pod._publish_event = tracking_publish

        await asyncio.gather(
            pod._handle_station_arrival("1"),
            pod._handle_station_arrival("1"),
        )

        # Only one PodArrival event should have been published
        pod_arrivals = [e for e in events_published if e == "PodArrival"]
        assert len(pod_arrivals) == 1


# ======================================================================
# Event filtering
# ======================================================================


class TestCargoEventFiltering:
    """Non-CargoRequest events should be silently ignored."""

    @pytest.mark.parametrize("event_type", [
        "CargoLoaded",
        "CargoDelivered",
        "SomeRandomEvent",
        "",
    ])
    async def test_non_request_events_ignored(
        self, message_bus, redis_client, station_client, network_context,
        event_type,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        broadcast_called = False

        async def mock_broadcast(req):
            nonlocal broadcast_called
            broadcast_called = True

        pod._handle_request_broadcast = mock_broadcast

        await pod._handle_cargo_event({
            "message": {
                "event_type": event_type,
                "origin": "1",
                "request_id": "c_001",
            }
        })
        assert not broadcast_called


# ======================================================================
# Decision context
# ======================================================================


class TestCargoDecisionContext:
    """_build_decision_context resolves location correctly."""

    async def test_at_station(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(
            message_bus, redis_client, station_client, station_id="3"
        )
        pod._available_requests = []
        ctx = await pod._build_decision_context()
        assert ctx.current_location == "3"
        assert ctx.pod_type == "cargo"
        assert ctx.weight_available == 500.0

    async def test_on_edge_with_segment(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.location_descriptor = LocationDescriptor(
            location_type="edge", edge_id="1->2",
            coordinate=Coordinate(50, 0),
        )
        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod._available_requests = []
        ctx = await pod._build_decision_context()
        assert ctx.current_location == "2"

    async def test_on_edge_no_segment_nearest_station(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.location_descriptor = LocationDescriptor(
            location_type="edge", edge_id="1->2",
            coordinate=Coordinate(90, 0),
        )
        pod.current_segment = None
        pod._available_requests = []
        ctx = await pod._build_decision_context()
        # (90,0) is closest to station 2 at (100,0)
        assert ctx.current_location == "2"
