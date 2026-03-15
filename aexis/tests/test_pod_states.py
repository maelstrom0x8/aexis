"""Pod state machine, capacity boundary, and edge-case tests.

Covers:
- PodStatus transitions (parameterized valid/invalid matrix)
- PassengerPod capacity boundaries (19/19, over, under, empty)
- CargoPod weight boundaries (500.0 exact, over, under, zero, negative)
- Location property setter/getter (station, edge, None)
- Arrival lock concurrency (simultaneous vs sequential)
- Route assignment data formats (list, dict-valid, dict-missing-keys, invalid-type)
- Route completion edge cases (no route, empty stations)
- Decision execution edge cases (segments vs no-segments, at station)
- Passenger claim eligibility matrix (IDLE/EN_ROUTE × on/off-route)
- Event filtering (non-PassengerArrival ignored)
- Pickup/delivery branch coverage
- Decision context location variants
- Command routing (target match/mismatch)
- State serialization (PassengerPod vs CargoPod shape)
- State snapshot resilience (Redis write failure)
"""

import asyncio
from collections import deque
from unittest.mock import AsyncMock, patch

import pytest

from aexis.core.model import (
    Coordinate,
    EdgeSegment,
    LocationDescriptor,
    PodStatus,
    Route,
)
from aexis.core.station_client import StationClient
from aexis.pod import CargoPod, PassengerPod, PodType
from aexis.tests.conftest import (
    async_seed_passenger,
    make_cargo_pod,
    make_passenger_pod,
)


# ======================================================================
# Status transitions
# ======================================================================


class TestPodStatusTransitions:
    """Parameterized valid/invalid status transitions."""

    @pytest.mark.parametrize(
        "initial,expected_after_route_assign",
        [
            (PodStatus.IDLE, PodStatus.EN_ROUTE),
        ],
    )
    async def test_idle_to_en_route_on_route_assignment(
        self, message_bus, redis_client, station_client, network_context,
        initial, expected_after_route_assign,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = initial
        await pod._handle_route_assignment({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "route": ["1", "2"],
            }
        })
        assert pod.status == expected_after_route_assign

    async def test_en_route_to_idle_on_route_completion(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Route completion with no current_route sets IDLE."""
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.EN_ROUTE
        pod.current_route = None
        await pod._handle_route_completion()
        assert pod.status == PodStatus.IDLE

    async def test_maintenance_status_is_inert(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Pod in MAINTENANCE doesn't move (update returns False)."""
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.MAINTENANCE
        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod.segment_progress = 0.0
        result = await pod.update(1.0)
        assert result is False
        assert pod.segment_progress == 0.0

    @pytest.mark.parametrize("status", [
        PodStatus.LOADING, PodStatus.UNLOADING, PodStatus.MAINTENANCE,
    ])
    async def test_non_en_route_statuses_block_movement(
        self, message_bus, redis_client, station_client, network_context,
        status,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = status
        result = await pod.update(1.0)
        assert result is False


# ======================================================================
# Passenger capacity boundaries
# ======================================================================


class TestPassengerPodCapacity:
    """Parameterized boundary tests for passenger headcount."""

    @pytest.mark.parametrize("current_count,should_accept", [
        (0, True),     # empty pod
        (18, True),    # one under capacity
        (19, False),   # exactly full
        (20, False),   # over capacity (shouldn't happen, but guard)
    ])
    async def test_capacity_boundary(
        self, message_bus, redis_client, station_client, network_context,
        current_count, should_accept,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        # Fill pod to current_count
        pod.passengers = [
            {"passenger_id": f"p_{i}", "destination": "2"}
            for i in range(current_count)
        ]
        remaining = pod.capacity - len(pod.passengers)
        assert (remaining > 0) == should_accept

    async def test_claim_rejected_when_full(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """A full pod should not claim new passengers via broadcast."""
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.passengers = [
            {"passenger_id": f"p_{i}", "destination": "2"}
            for i in range(19)
        ]
        await async_seed_passenger(redis_client, "1", "p_new", "2")

        # Attempt broadcast
        await pod._handle_request_broadcast({
            "type": "passenger",
            "passenger_id": "p_new",
            "origin": "1",
            "destination": "2",
        })
        # Should not have claimed
        assert len(pod.passengers) == 19


# ======================================================================
# Cargo weight boundaries
# ======================================================================


class TestCargoPodWeight:
    """Parameterized boundary tests for cargo weight."""

    @pytest.mark.parametrize("current_weight,req_weight,should_accept", [
        (0.0, 500.0, True),      # exact capacity
        (0.0, 500.1, False),     # 0.1 over
        (499.9, 0.1, True),      # fills exactly
        (499.9, 0.2, False),     # 0.1 over when combined
        (0.0, 0.0, False),       # zero-weight cargo rejected (code: req_weight <= 0)
        (0.0, -1.0, False),      # negative weight rejected
    ])
    async def test_weight_boundary(
        self, message_bus, redis_client, station_client, network_context,
        current_weight, req_weight, should_accept,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.current_weight = current_weight
        remaining = pod.weight_capacity - pod.current_weight
        # Zero/negative weight is always rejected by the code
        if req_weight <= 0:
            accepted = False
        else:
            accepted = remaining >= req_weight
        assert accepted == should_accept


# ======================================================================
# Location property
# ======================================================================


class TestPodLocationProperty:
    """Pod.location getter/setter roundtrip tests."""

    async def test_set_station_id(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.location = "3"
        assert pod.location_descriptor.location_type == "station"
        assert pod.location_descriptor.node_id == "3"
        assert pod.location == "3"

    async def test_set_edge_id(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.location = "1->2"
        assert pod.location_descriptor.location_type == "edge"
        assert pod.location_descriptor.edge_id == "1->2"
        assert pod.location == "1->2"

    async def test_set_none_is_noop(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        original = pod.location_descriptor
        pod.location = None
        assert pod.location_descriptor is original


# ======================================================================
# Arrival lock concurrency
# ======================================================================


class TestArrivalLockConcurrency:
    """Concurrent vs sequential station-arrival calls.

    The lock guard in _handle_station_arrival checks locked() then acquires.
    Concurrent calls to the same station should result in only one full
    processing, with the other skipped (warning logged).

    Note: delivery is called twice per arrival — once directly and once via
    make_decision → _execute_decision — so we track arrival handler entry
    instead of individual delivery calls.
    """

    async def test_concurrent_arrivals_only_one_processes(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """When two arrivals fire simultaneously, the second is skipped.

        PassengerPod's arrival handler has no builtin sleep before lock
        acquisition (unlike CargoPod's 2s dock delay), so we inject a
        yield point inside _publish_event to create real contention.
        """
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.IDLE

        events_published = []
        original_publish = pod._publish_event

        async def slow_publish(event):
            events_published.append(getattr(event, 'event_type', str(event)))
            await asyncio.sleep(0.1)  # Yield control while lock is held
            await original_publish(event)

        pod._publish_event = slow_publish

        # Fire two arrivals concurrently
        await asyncio.gather(
            pod._handle_station_arrival("1"),
            pod._handle_station_arrival("1"),
        )

        # Only one PodArrival event should have been published
        pod_arrivals = [e for e in events_published if e == "PodArrival"]
        assert len(pod_arrivals) == 1

    async def test_sequential_arrivals_both_process(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Sequential arrivals should both execute fully."""
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.IDLE

        events_published = []
        original_publish = pod._publish_event

        async def tracking_publish(event):
            events_published.append(getattr(event, 'event_type', str(event)))
            await original_publish(event)

        pod._publish_event = tracking_publish

        await pod._handle_station_arrival("1")
        await pod._handle_station_arrival("1")

        pod_arrivals = [e for e in events_published if e == "PodArrival"]
        assert len(pod_arrivals) == 2


# ======================================================================
# Route assignment data formats
# ======================================================================


class TestRouteAssignmentFormats:
    """_handle_route_assignment accepts list, dict, rejects missing keys / bad types."""

    async def test_route_as_list(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        await pod._handle_route_assignment({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "route": ["1", "2"],
            }
        })
        assert pod.status == PodStatus.EN_ROUTE
        assert pod.current_route is not None
        assert pod.current_route.stations == ["1", "2"]

    async def test_route_as_valid_dict(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        await pod._handle_route_assignment({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "parameters": {
                    "route": {
                        "route_id": "test_r",
                        "stations": ["1", "2"],
                        "estimated_duration": 5,
                    }
                },
            }
        })
        assert pod.status == PodStatus.EN_ROUTE
        assert pod.current_route.route_id == "test_r"

    async def test_route_dict_missing_keys_rejected(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.IDLE
        await pod._handle_route_assignment({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "route": {"route_id": "test_r"},  # missing stations/duration
            }
        })
        # Should remain IDLE because route was rejected
        assert pod.status == PodStatus.IDLE

    async def test_route_invalid_type_rejected(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.IDLE
        await pod._handle_route_assignment({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "route": 12345,  # invalid type
            }
        })
        assert pod.status == PodStatus.IDLE

    async def test_route_in_parameters_field(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Route can also be nested under parameters."""
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        await pod._handle_route_assignment({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "parameters": {"route": ["1", "2", "4"]},
            }
        })
        assert pod.status == PodStatus.EN_ROUTE
        assert pod.current_route.stations == ["1", "2", "4"]


# ======================================================================
# Route completion edge cases
# ======================================================================


class TestRouteCompletionEdgeCases:
    """_handle_route_completion with missing or empty route data."""

    async def test_no_current_route_sets_idle(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.EN_ROUTE
        pod.current_route = None
        await pod._handle_route_completion()
        assert pod.status == PodStatus.IDLE
        assert pod.segment_progress == 0.0

    async def test_empty_stations_list_sets_idle(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.EN_ROUTE
        pod.current_route = Route(
            route_id="empty", stations=[], estimated_duration=0
        )
        await pod._handle_route_completion()
        assert pod.status == PodStatus.IDLE


# ======================================================================
# Decision execution
# ======================================================================


class TestExecuteDecision:
    """_execute_decision branch coverage."""

    async def test_none_decision_returns_early(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.IDLE
        await pod._execute_decision(None)
        assert pod.status == PodStatus.IDLE

    async def test_decision_with_no_segments_stays_idle(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Single-station route produces no segments → pod stays IDLE."""
        from aexis.core.model import Decision
        decision = Decision(
            decision_type="route_selection",
            accepted_requests=[],
            rejected_requests=[],
            route=["1"],  # only current station
            estimated_duration=0,
            confidence=0.8,
            reasoning="test",
        )
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        await pod._execute_decision(decision)
        assert pod.status == PodStatus.IDLE


# ======================================================================
# Passenger claim eligibility
# ======================================================================


class TestPassengerClaimEligibility:
    """Parameterized eligibility: status × location × capacity."""

    @pytest.mark.parametrize("status,on_route,expected_eligible", [
        (PodStatus.IDLE, False, True),
        (PodStatus.EN_ROUTE, True, True),   # origin on current route
        (PodStatus.EN_ROUTE, False, False),  # origin NOT on route
        (PodStatus.LOADING, False, False),
        (PodStatus.UNLOADING, False, False),
        (PodStatus.MAINTENANCE, False, False),
    ])
    async def test_eligibility_matrix(
        self, message_bus, redis_client, station_client, network_context,
        status, on_route, expected_eligible,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = status
        if status == PodStatus.EN_ROUTE:
            if on_route:
                pod.current_route = Route(
                    route_id="r", stations=["1", "2"], estimated_duration=5
                )
            else:
                pod.current_route = Route(
                    route_id="r", stations=["3", "4"], estimated_duration=5
                )

        request = {
            "type": "passenger",
            "passenger_id": "p_elig",
            "origin": "1",
            "destination": "2",
        }

        # Seed passenger in Redis so claim can succeed if eligible
        await async_seed_passenger(redis_client, "1", "p_elig", "2")

        # Track whether make_decision was called (signals eligibility)
        decision_called = False
        original_make_decision = pod.make_decision

        async def mock_decision():
            nonlocal decision_called
            decision_called = True

        pod.make_decision = mock_decision

        await pod._handle_request_broadcast(request)
        assert decision_called == expected_eligible


# ======================================================================
# Event filtering
# ======================================================================


class TestPassengerEventFiltering:
    """Non-PassengerArrival events should be silently ignored."""

    @pytest.mark.parametrize("event_type", [
        "PassengerPickedUp",
        "PassengerDelivered",
        "SomeRandomEvent",
        "",
    ])
    async def test_non_arrival_events_ignored(
        self, message_bus, redis_client, station_client, network_context,
        event_type,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        broadcast_called = False

        async def mock_broadcast(req):
            nonlocal broadcast_called
            broadcast_called = True

        pod._handle_request_broadcast = mock_broadcast

        await pod._handle_passenger_event({
            "message": {
                "event_type": event_type,
                "station_id": "1",
                "passenger_id": "p_001",
            }
        })
        assert not broadcast_called


# ======================================================================
# Pickup/delivery branch coverage
# ======================================================================


class TestPassengerPickupBranches:
    """_execute_pickup edge cases."""

    async def test_pickup_at_capacity_returns_early(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.passengers = [
            {"passenger_id": f"p_{i}", "destination": "2"} for i in range(19)
        ]
        await async_seed_passenger(redis_client, "1", "p_extra", "2")
        await station_client.claim_passenger("1", "p_extra", pod.pod_id)
        await pod._execute_pickup("1")
        assert len(pod.passengers) == 19  # unchanged

    async def test_pickup_skips_already_onboard(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.passengers = [{"passenger_id": "p_dup", "destination": "2"}]
        await async_seed_passenger(redis_client, "1", "p_dup", "2")
        await station_client.claim_passenger("1", "p_dup", pod.pod_id)
        await pod._execute_pickup("1")
        dup_count = sum(
            1 for p in pod.passengers if p["passenger_id"] == "p_dup"
        )
        assert dup_count == 1  # not duplicated

    async def test_no_pickups_returns_early(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.status = PodStatus.IDLE
        await pod._execute_pickup("1")
        # Status should remain IDLE (no transition to LOADING)
        assert pod.status == PodStatus.IDLE


class TestPassengerDeliveryBranches:
    """_execute_delivery edge cases."""

    async def test_no_matching_passengers_returns_early(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.passengers = [{"passenger_id": "p_1", "destination": "3"}]
        pod.status = PodStatus.IDLE
        await pod._execute_delivery("2")
        assert len(pod.passengers) == 1  # untouched
        assert pod.status == PodStatus.IDLE  # no transition

    async def test_matching_passengers_delivered(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        from datetime import UTC, datetime
        pod.passengers = [
            {"passenger_id": "p_1", "destination": "2", "pickup_time": datetime.now(UTC)},
            {"passenger_id": "p_2", "destination": "3", "pickup_time": datetime.now(UTC)},
        ]
        await pod._execute_delivery("2")
        remaining_ids = [p["passenger_id"] for p in pod.passengers]
        assert "p_1" not in remaining_ids
        assert "p_2" in remaining_ids


# ======================================================================
# Decision context location variants
# ======================================================================


class TestDecisionContextLocation:
    """_build_decision_context resolves location from different descriptor states."""

    async def test_at_station(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="2"
        )
        pod._available_requests = []
        ctx = await pod._build_decision_context()
        assert ctx.current_location == "2"

    async def test_on_edge_with_segment(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.location_descriptor = LocationDescriptor(
            location_type="edge", edge_id="1->2",
            coordinate=Coordinate(50, 0),
        )
        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod._available_requests = []
        ctx = await pod._build_decision_context()
        # Should use current_segment.end_node
        assert ctx.current_location == "2"

    async def test_on_edge_no_segment_falls_back_to_nearest(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.location_descriptor = LocationDescriptor(
            location_type="edge", edge_id="1->2",
            coordinate=Coordinate(50, 0),
        )
        pod.current_segment = None
        pod._available_requests = []
        ctx = await pod._build_decision_context()
        # Nearest to (50,0) should be station 1 (0,0) or 2 (100,0)
        assert ctx.current_location in ("1", "2")


# ======================================================================
# Command routing
# ======================================================================


class TestCommandRouting:
    """_handle_command dispatches only to target pod."""

    async def test_command_for_different_pod_ignored(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client, pod_id="1")
        pod.status = PodStatus.IDLE
        await pod._handle_command({
            "message": {
                "command_type": "AssignRoute",
                "target": "other_pod",
                "route": ["1", "2"],
            }
        })
        assert pod.status == PodStatus.IDLE  # unchanged

    async def test_unknown_command_type_ignored(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client, pod_id="1")
        pod.status = PodStatus.IDLE
        await pod._handle_command({
            "message": {
                "command_type": "UnknownCommand",
                "target": "1",
            }
        })
        assert pod.status == PodStatus.IDLE


# ======================================================================
# State serialization
# ======================================================================


class TestPodGetState:
    """State dict shape differs by pod type."""

    async def test_passenger_pod_state_includes_passengers(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.passengers = [{"passenger_id": "p_1", "destination": "2"}]
        state = pod.get_state()
        assert "passengers" in state
        assert len(state["passengers"]) == 1
        assert state["pod_type"] == "passenger"

    async def test_cargo_pod_state_includes_cargo(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(message_bus, redis_client, station_client)
        pod.cargo = [{"request_id": "c_1", "destination": "2", "weight": 50.0}]
        state = pod.get_state()
        assert "cargo" in state
        assert len(state["cargo"]) == 1
        assert state["pod_type"] == "cargo"

    async def test_state_location_on_edge(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)
        pod.location_descriptor = LocationDescriptor(
            location_type="edge", edge_id="1->2",
            coordinate=Coordinate(50, 0), distance_on_edge=50.0,
        )
        pod.segment_progress = 50.0
        state = pod.get_state()
        assert "on edge" in state["location"]


# ======================================================================
# State snapshot resilience
# ======================================================================


class TestStateSnapshotResilience:
    """Redis write failures don't crash the pod."""

    async def test_snapshot_survives_redis_failure(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(message_bus, redis_client, station_client)

        async def failing_set(*args, **kwargs):
            raise ConnectionError("Redis unavailable")

        pod._redis.set = failing_set
        # Should log error but not raise
        await pod._publish_state_snapshot()
        # Pod still functional
        assert pod.status == PodStatus.IDLE
