"""Proactive tests exposing Bug 1 (idle pods) and Bug 2 (stale status snapshots).

These tests assert what the CORRECT behavior should be. They are expected
to FAIL against the current implementation, proving the bugs exist.

Bug 1: Pods docked idle while payloads wait at other stations.
  Root cause: _available_requests is only populated via broadcast events
  that can be missed, and _get_idle_route routes to geometrically nearest
  station instead of station with demand.

Bug 2: EN_ROUTE pods display "idle" in status bar.
  Root cause: _handle_route_completion sets current_route = None AFTER
  the arrival handler already called make_decision and set a NEW route.
  The new route is destroyed.
"""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from aexis.core.model import (
    Coordinate,
    DecisionContext,
    EdgeSegment,
    LocationDescriptor,
    PodStatus,
    Route,
)
from aexis.core.routing import OfflineRouter, OfflineRoutingStrategy, RoutingProvider
from aexis.tests.conftest import async_seed_passenger, make_passenger_pod


# ======================================================================
# Bug 2: Route completion / status snapshot fidelity
# ======================================================================


class TestRouteCompletionDoesNotNullifyNewRoute:
    """Bug 2: After _handle_route_completion, if make_decision set a new
    route, current_route should NOT be None.
    """

    async def test_route_completion_does_not_nullify_new_route(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_b2", station_id="1",
        )

        # Give the pod a passenger going to station 2 so make_decision
        # at arrival will produce a real route (not idle)
        await async_seed_passenger(redis_client, "2", "p_target", "3")
        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_target",
             "origin": "2", "destination": "3"},
        ]

        # Set up a route 1→2 so we can complete it
        pod.current_route = Route(
            route_id="test_rt", stations=["1", "2"], estimated_duration=5,
        )
        await pod._hydrate_route(["1", "2"])
        pod.status = PodStatus.EN_ROUTE

        # Simulate route completion at station 2
        await pod._handle_route_completion()

        # EXPECTED: make_decision was called inside _handle_station_arrival,
        # and it should have set a new current_route. The route completion
        # code must NOT nullify it afterward.
        #
        # BUG: current_route is set to None at L403 AFTER make_decision
        # already set a new route.
        assert pod.current_route is not None, (
            "current_route was nullified after make_decision set a new route"
        )


class TestSnapshotStatusMatchesActualStatus:
    """Bug 2: After _handle_route_completion + snapshot, the status in Redis
    must match the pod's actual in-memory status.
    """

    async def test_snapshot_status_matches_actual_status(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_snap", station_id="1",
        )

        # Give pod a reason to re-route after arriving at 2
        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_rr",
             "origin": "3", "destination": "4"},
        ]

        pod.current_route = Route(
            route_id="test_rt", stations=["1", "2"], estimated_duration=5,
        )
        await pod._hydrate_route(["1", "2"])
        pod.status = PodStatus.EN_ROUTE

        await pod._handle_route_completion()

        # Read the snapshot from Redis
        raw = await redis_client.get("aexis:pod:pod_snap:state")
        assert raw is not None, "No snapshot written after route completion"
        snapshot = json.loads(raw)

        # The snapshot status must match the actual pod status
        assert snapshot["status"] == pod.status.value, (
            f"Snapshot has status={snapshot['status']} but pod.status={pod.status.value}"
        )


class TestExecuteDecisionSnapshotReflectsEnRoute:
    """Bug 2: After _execute_decision with valid segments, the snapshot in
    Redis must have status=en_route.
    """

    async def test_execute_decision_snapshot_reflects_en_route(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_edr", station_id="1",
        )
        from aexis.core.model import Decision

        decision = Decision(
            decision_type="route_selection",
            accepted_requests=[],
            rejected_requests=[],
            route=["1", "2"],
            estimated_duration=5,
            confidence=0.8,
            reasoning="test",
            fallback_used=False,
        )

        await pod._execute_decision(decision)

        # Pod should be EN_ROUTE
        assert pod.status == PodStatus.EN_ROUTE

        # Snapshot must also say EN_ROUTE
        raw = await redis_client.get("aexis:pod:pod_edr:state")
        assert raw is not None
        snapshot = json.loads(raw)
        assert snapshot["status"] == "en_route", (
            f"Snapshot has status={snapshot['status']} but expected en_route"
        )

    async def test_execute_decision_snapshot_reflects_idle_when_no_segments(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Regression guard: single-station route → status=idle in snapshot."""
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_edi", station_id="1",
        )
        from aexis.core.model import Decision

        decision = Decision(
            decision_type="route_selection",
            accepted_requests=[],
            rejected_requests=[],
            route=["1"],  # single station → no segments
            estimated_duration=0,
            confidence=1.0,
            reasoning="idle",
            fallback_used=False,
        )

        await pod._execute_decision(decision)

        assert pod.status == PodStatus.IDLE
        raw = await redis_client.get("aexis:pod:pod_edi:state")
        assert raw is not None
        snapshot = json.loads(raw)
        assert snapshot["status"] == "idle"


class TestRouteCompletionArrivalDecisionOrdering:
    """Bug 2: Full sequence — route completes → arrival → decision →
    verify final state is consistent (status matches route presence).
    """

    async def test_route_completion_arrival_decision_ordering(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_ord", station_id="1",
        )

        # Pod has a passenger to deliver at station 2
        pod.passengers = [
            {"passenger_id": "p_del", "destination": "2",
             "pickup_time": datetime.now(UTC)},
        ]

        pod.current_route = Route(
            route_id="test_rt", stations=["1", "2"], estimated_duration=5,
        )
        await pod._hydrate_route(["1", "2"])
        pod.status = PodStatus.EN_ROUTE

        await pod._handle_route_completion()

        # After completion: if pod has a new route, status must be EN_ROUTE
        # if pod has no route, status must be IDLE
        # They must be CONSISTENT — not EN_ROUTE with no route, or IDLE with a route
        has_route = (
            pod.current_route is not None
            and pod.current_route.stations
            and len(pod.current_route.stations) > 1
        )
        has_segments = pod.current_segment is not None or len(pod.route_queue) > 0

        if has_route or has_segments:
            assert pod.status == PodStatus.EN_ROUTE, (
                f"Pod has route/segments but status is {pod.status.value}"
            )
        else:
            assert pod.status == PodStatus.IDLE, (
                f"Pod has no route/segments but status is {pod.status.value}"
            )


# ======================================================================
# Bug 1: Pods idle while payloads wait at other stations
# ======================================================================


class TestPodRoutesToStationWithWaitingPassengers:
    """Bug 1: Pod at station 1, passengers waiting at station 3 →
    pod's next route must include station 3, not nearest empty station.
    """

    async def test_pod_routes_to_station_with_waiting_passengers(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_dem", station_id="1",
        )
        pod.status = PodStatus.IDLE

        # Passengers waiting at station 3 (not the nearest station)
        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_wait",
             "origin": "3", "destination": "4"},
        ]

        await pod.make_decision()

        # Pod should have a route that targets station 3
        assert pod.current_route is not None, "Pod has no route after make_decision"
        assert "3" in pod.current_route.stations, (
            f"Route {pod.current_route.stations} doesn't include station 3 "
            f"where passengers are waiting"
        )


class TestIdlePodReactsToBroadcast:
    """Bug 1: IDLE pod receives PassengerArrival → claims → re-routes."""

    async def test_idle_pod_reacts_to_broadcast(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_bcast", station_id="1",
        )
        pod.status = PodStatus.IDLE
        await pod._setup_subscriptions()

        # Seed a passenger at station 3
        await async_seed_passenger(redis_client, "3", "p_bcast", "4")

        # Simulate a PassengerArrival broadcast
        from aexis.core.message_bus import MessageBus
        from aexis.core.model import PassengerArrival

        event = PassengerArrival(
            passenger_id="p_bcast",
            station_id="3",
            destination="4",
            priority=3,
            group_size=1,
            special_needs=[],
            wait_time_limit=45,
        )
        await message_bus.publish_event(
            MessageBus.CHANNELS["PASSENGER_EVENTS"], event,
        )

        # Allow event processing
        await asyncio.sleep(0.5)

        # Pod should have claimed the passenger and set a route
        assert len(pod._available_requests) >= 1 or pod.current_route is not None, (
            "IDLE pod did not react to broadcast — no available requests and no route"
        )

        await pod._cleanup_subscriptions()


class TestBroadcastDuringArrivalHandlerNotLost:
    """Bug 1: Broadcast fires while pod is in _handle_station_arrival →
    event should still be processed.
    """

    async def test_broadcast_during_arrival_handler_not_lost(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_race", station_id="1",
        )
        await pod._setup_subscriptions()

        # Seed passenger at station 3
        await async_seed_passenger(redis_client, "3", "p_race", "4")

        # Track whether the broadcast event was received
        broadcast_received = []
        original_handler = pod._handle_request_broadcast

        async def tracking_handler(request):
            broadcast_received.append(request)
            await original_handler(request)

        pod._handle_request_broadcast = tracking_handler

        # Fire the broadcast while arrival handler is running
        async def fire_broadcast_during_arrival():
            # Small delay to ensure arrival handler is in progress
            await asyncio.sleep(0.05)
            from aexis.core.message_bus import MessageBus
            from aexis.core.model import PassengerArrival

            event = PassengerArrival(
                passenger_id="p_race",
                station_id="3",
                destination="4",
                priority=3,
                group_size=1,
                special_needs=[],
                wait_time_limit=45,
            )
            await message_bus.publish_event(
                MessageBus.CHANNELS["PASSENGER_EVENTS"], event,
            )

        # Run arrival handler and broadcast concurrently
        await asyncio.gather(
            pod._handle_station_arrival("1"),
            fire_broadcast_during_arrival(),
        )

        # Allow event processing
        await asyncio.sleep(0.5)

        # The broadcast event must have been received
        assert len(broadcast_received) >= 1, (
            "Broadcast event during arrival handler was lost"
        )

        await pod._cleanup_subscriptions()


class TestAvailableRequestsNotStaleAfterPickup:
    """Bug 1: After _execute_pickup removes claimed requests from
    _available_requests, other unclaimed requests must remain.
    """

    async def test_available_requests_not_stale_after_pickup(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_stale", station_id="1",
        )

        # Two requests: one at station 1 (will be picked up), one at station 3
        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_local",
             "origin": "1", "destination": "2"},
            {"type": "passenger", "passenger_id": "p_remote",
             "origin": "3", "destination": "4"},
        ]

        # Seed the local passenger so pickup succeeds
        await async_seed_passenger(redis_client, "1", "p_local", "2")

        await pod._execute_pickup("1")

        # p_local should have been removed from _available_requests (claimed + boarded)
        remaining_ids = [r.get("passenger_id") for r in pod._available_requests]

        # p_remote should STILL be in _available_requests — it wasn't picked up
        assert "p_remote" in remaining_ids, (
            f"Available requests after pickup: {remaining_ids} — "
            f"p_remote was incorrectly removed"
        )


class TestConcurrentBroadcastAndArrival:
    """Bug 1: Simultaneous arrival handler + broadcast event →
    pod doesn't lose the event.
    """

    async def test_concurrent_broadcast_and_arrival(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_conc", station_id="2",
        )
        await pod._setup_subscriptions()

        # Seed passenger at station 3 and fake station state so get_all_station_ids finds it
        await async_seed_passenger(redis_client, "3", "p_conc", "4")
        await redis_client.set("aexis:station:3:state", "{}")

        # Prepare the broadcast event
        from aexis.core.message_bus import MessageBus
        from aexis.core.model import PassengerArrival

        event = PassengerArrival(
            passenger_id="p_conc",
            station_id="3",
            destination="4",
            priority=3,
            group_size=1,
            special_needs=[],
            wait_time_limit=45,
        )

        # Fire both concurrently
        await asyncio.gather(
            pod._handle_station_arrival("2"),
            message_bus.publish_event(
                MessageBus.CHANNELS["PASSENGER_EVENTS"], event,
            ),
        )

        # Allow processing
        await asyncio.sleep(0.5)

        # After both complete, the pod should either:
        # 1. Have claimed p_conc in _available_requests, OR
        # 2. Have a route that includes station 3
        has_request = any(
            r.get("passenger_id") == "p_conc" for r in pod._available_requests
        )
        has_route_to_3 = (
            pod.current_route is not None
            and "3" in pod.current_route.stations
        )

        assert has_request or has_route_to_3, (
            f"Pod lost the broadcast event during concurrent arrival. "
            f"available_requests={pod._available_requests}, "
            f"current_route={pod.current_route}"
        )

        await pod._cleanup_subscriptions()
