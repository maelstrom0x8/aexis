
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

class TestRouteCompletionDoesNotNullifyNewRoute:

    async def test_route_completion_does_not_nullify_new_route(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_b2", station_id="1",
        )

        await async_seed_passenger(redis_client, "2", "p_target", "3")
        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_target",
             "origin": "2", "destination": "3"},
        ]

        pod.current_route = Route(
            route_id="test_rt", stations=["1", "2"], estimated_duration=5,
        )
        await pod._hydrate_route(["1", "2"])
        pod.status = PodStatus.EN_ROUTE

        await pod._handle_route_completion()

        assert pod.current_route is not None, (
            "current_route was nullified after make_decision set a new route"
        )

class TestSnapshotStatusMatchesActualStatus:

    async def test_snapshot_status_matches_actual_status(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_snap", station_id="1",
        )

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

        raw = await redis_client.get("aexis:pod:pod_snap:state")
        assert raw is not None, "No snapshot written after route completion"
        snapshot = json.loads(raw)

        assert snapshot["status"] == pod.status.value, (
            f"Snapshot has status={snapshot['status']} but pod.status={pod.status.value}"
        )

class TestExecuteDecisionSnapshotReflectsEnRoute:

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

        assert pod.status == PodStatus.EN_ROUTE

        raw = await redis_client.get("aexis:pod:pod_edr:state")
        assert raw is not None
        snapshot = json.loads(raw)
        assert snapshot["status"] == "en_route", (
            f"Snapshot has status={snapshot['status']} but expected en_route"
        )

    async def test_execute_decision_snapshot_reflects_idle_when_no_segments(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_edi", station_id="1",
        )
        from aexis.core.model import Decision

        decision = Decision(
            decision_type="route_selection",
            accepted_requests=[],
            rejected_requests=[],
            route=["1"],
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

    async def test_route_completion_arrival_decision_ordering(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_ord", station_id="1",
        )

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

class TestPodRoutesToStationWithWaitingPassengers:

    async def test_pod_routes_to_station_with_waiting_passengers(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_dem", station_id="1",
        )
        pod.status = PodStatus.IDLE

        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_wait",
             "origin": "3", "destination": "4"},
        ]

        await pod.make_decision()

        assert pod.current_route is not None, "Pod has no route after make_decision"
        assert "3" in pod.current_route.stations, (
            f"Route {pod.current_route.stations} doesn't include station 3 "
            f"where passengers are waiting"
        )

class TestIdlePodReactsToBroadcast:

    async def test_idle_pod_reacts_to_broadcast(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_bcast", station_id="1",
        )
        pod.status = PodStatus.IDLE
        await pod._setup_subscriptions()

        await async_seed_passenger(redis_client, "3", "p_bcast", "4")

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

        await asyncio.sleep(0.5)

        assert len(pod._available_requests) >= 1 or pod.current_route is not None, (
            "IDLE pod did not react to broadcast — no available requests and no route"
        )

        await pod._cleanup_subscriptions()

class TestBroadcastDuringArrivalHandlerNotLost:

    async def test_broadcast_during_arrival_handler_not_lost(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_race", station_id="1",
        )
        await pod._setup_subscriptions()

        await async_seed_passenger(redis_client, "3", "p_race", "4")

        broadcast_received = []
        original_handler = pod._handle_request_broadcast

        async def tracking_handler(request):
            broadcast_received.append(request)
            await original_handler(request)

        pod._handle_request_broadcast = tracking_handler

        async def fire_broadcast_during_arrival():

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

        await asyncio.gather(
            pod._handle_station_arrival("1"),
            fire_broadcast_during_arrival(),
        )

        await asyncio.sleep(0.5)

        assert len(broadcast_received) >= 1, (
            "Broadcast event during arrival handler was lost"
        )

        await pod._cleanup_subscriptions()

class TestAvailableRequestsNotStaleAfterPickup:

    async def test_available_requests_not_stale_after_pickup(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_stale", station_id="1",
        )

        pod._available_requests = [
            {"type": "passenger", "passenger_id": "p_local",
             "origin": "1", "destination": "2"},
            {"type": "passenger", "passenger_id": "p_remote",
             "origin": "3", "destination": "4"},
        ]

        await async_seed_passenger(redis_client, "1", "p_local", "2")

        await pod._execute_pickup("1")

        remaining_ids = [r.get("passenger_id") for r in pod._available_requests]

        assert "p_remote" in remaining_ids, (
            f"Available requests after pickup: {remaining_ids} — "
            f"p_remote was incorrectly removed"
        )

class TestConcurrentBroadcastAndArrival:

    async def test_concurrent_broadcast_and_arrival(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_conc", station_id="2",
        )
        await pod._setup_subscriptions()

        await async_seed_passenger(redis_client, "3", "p_conc", "4")
        await redis_client.set("aexis:station:3:state", "{}")

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

        await asyncio.gather(
            pod._handle_station_arrival("2"),
            message_bus.publish_event(
                MessageBus.CHANNELS["PASSENGER_EVENTS"], event,
            ),
        )

        await asyncio.sleep(0.5)

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
