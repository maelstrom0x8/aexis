
import asyncio
import json
from datetime import UTC, datetime

import pytest

from aexis.core.model import (
    Coordinate,
    Decision,
    LocationDescriptor,
    PodStatus,
    Route,
    StationStatus,
)
from aexis.core.station_client import StationClient
from aexis.station import Station
from aexis.tests.conftest import (
    async_seed_cargo,
    async_seed_passenger,
    make_cargo_pod,
    make_passenger_pod,
)

class TestPassengerEndToEnd:

    async def test_passenger_pickup_and_delivery(
        self, message_bus, redis_client, station_client, network_context,
    ):

        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_arrival({
            "station_id": "1",
            "passenger_id": "p_e2e",
            "destination": "2",
        })
        assert station._passenger_count == 1

        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        claim_ok = await station_client.claim_passenger("1", "p_e2e", pod.pod_id)
        assert claim_ok is True

        await pod._execute_pickup("1")
        assert len(pod.passengers) == 1
        assert pod.passengers[0]["passenger_id"] == "p_e2e"

        await pod._execute_delivery("2")
        assert len(pod.passengers) == 0

        pending = await station_client.get_pending_passengers("1")
        unclaimed = [p for p in pending if p["passenger_id"] == "p_e2e"]
        assert len(unclaimed) == 0

class TestCargoEndToEnd:

    async def test_cargo_load_and_delivery(
        self, message_bus, redis_client, station_client, network_context,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "1",
            "request_id": "c_e2e",
            "destination": "3",
            "weight": 100.0,
        })
        assert station._cargo_count == 1

        pod = make_cargo_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        claim_ok = await station_client.claim_cargo("1", "c_e2e", pod.pod_id)
        assert claim_ok is True

        await pod._execute_pickup("1")
        assert len(pod.cargo) == 1
        assert pod.current_weight == 100.0

        await pod._execute_delivery("3")
        assert len(pod.cargo) == 0
        assert pod.current_weight == 0.0

class TestMultiPodClaimRace:

    async def test_three_pods_one_passenger(
        self, message_bus, redis_client, station_client, network_context,
    ):
        await async_seed_passenger(redis_client, "1", "p_race", "2")

        async def attempt_claim(pod_id):
            return await station_client.claim_passenger("1", "p_race", pod_id)

        results = await asyncio.gather(
            attempt_claim("pod_a"),
            attempt_claim("pod_b"),
            attempt_claim("pod_c"),
        )
        winners = sum(1 for r in results if r is True)
        assert winners == 1

class TestProximityClaimAdvantage:

    async def test_docked_wins_over_remote(
        self, message_bus, redis_client, station_client, network_context,
    ):
        await async_seed_passenger(redis_client, "1", "p_prox", "2")

        docked_pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="docked", station_id="1",
        )
        remote_pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="remote", station_id="3",
        )

        docked_result = await station_client.claim_passenger(
            "1", "p_prox", docked_pod.pod_id
        )

        await asyncio.sleep(0.2)
        remote_result = await station_client.claim_passenger(
            "1", "p_prox", remote_pod.pod_id
        )

        assert docked_result is True
        assert remote_result is False

class TestPodReDecisionOnArrival:

    async def test_arrival_triggers_new_decision(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        pod.passengers = [
            {"passenger_id": "p_deliver", "destination": "2",
             "pickup_time": datetime.now(UTC)},
        ]

        await async_seed_passenger(redis_client, "2", "p_new", "4")

        decision_called = False
        original_make = pod.make_decision

        async def tracking_decision():
            nonlocal decision_called
            decision_called = True

        pod.make_decision = tracking_decision

        await pod._handle_station_arrival("2")
        assert decision_called

class TestCongestionAlertPropagation:

    async def test_pod_receives_congestion_alert(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        pod.status = PodStatus.EN_ROUTE
        pod.current_route = Route(
            route_id="r1", stations=["1", "2"], estimated_duration=5
        )

        await pod._handle_congestion_alert({
            "message": {
                "event_type": "CongestionAlert",
                "station_id": "2",
                "congestion_level": 0.95,
                "severity": "critical",
            }
        })

        assert pod.status == PodStatus.EN_ROUTE

    async def test_pod_ignores_alert_for_unrelated_station(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        pod.status = PodStatus.EN_ROUTE
        pod.current_route = Route(
            route_id="r1", stations=["1", "2"], estimated_duration=5
        )
        await pod._handle_congestion_alert({
            "message": {
                "event_type": "CongestionAlert",
                "station_id": "99",
                "congestion_level": 0.95,
            }
        })
        assert pod.status == PodStatus.EN_ROUTE

    async def test_pod_without_route_ignores_alert(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        pod.current_route = None
        await pod._handle_congestion_alert({
            "message": {
                "event_type": "CongestionAlert",
                "station_id": "2",
                "congestion_level": 0.95,
            }
        })
        assert pod.status == PodStatus.IDLE

class TestRouteAssignmentCommand:

    async def test_assign_route_sets_en_route(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        assert pod.status == PodStatus.IDLE

        await pod._handle_command({
            "message": {
                "command_type": "AssignRoute",
                "target": pod.pod_id,
                "route": ["1", "2", "4"],
            }
        })
        assert pod.status == PodStatus.EN_ROUTE
        assert pod.current_route is not None
        assert pod.current_route.stations == ["1", "2", "4"]

    async def test_assign_route_for_other_pod_ignored(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, pod_id="p1", station_id="1"
        )
        await pod._handle_command({
            "message": {
                "command_type": "AssignRoute",
                "target": "p999",
                "route": ["1", "2"],
            }
        })
        assert pod.status == PodStatus.IDLE
        assert pod.current_route is None

class TestStationClientInit:

    def test_none_redis_raises(self):
        with pytest.raises((ValueError, TypeError)):
            StationClient(None)

class TestOrphanClaims:

    async def test_orphan_passenger_claim(
        self, redis_client, station_client,
    ):
        await async_seed_passenger(redis_client, "1", "p_orphan", "2")
        claim_ok = await station_client.claim_passenger(
            "1", "p_orphan", "pod_x"
        )
        assert claim_ok is True

        await redis_client.hdel("aexis:station:1:passengers", "p_orphan")

        claimed = await station_client.get_claimed_passengers("1", "pod_x")

        assert isinstance(claimed, list)

    async def test_orphan_cargo_claim(
        self, redis_client, station_client,
    ):
        await async_seed_cargo(redis_client, "1", "c_orphan", "2", 50.0)
        claim_ok = await station_client.claim_cargo(
            "1", "c_orphan", "pod_x"
        )
        assert claim_ok is True

        await redis_client.hdel("aexis:station:1:cargo", "c_orphan")

        claimed = await station_client.get_claimed_cargo("1", "pod_x")
        assert isinstance(claimed, list)

class TestCorruptStateData:

    async def test_corrupt_state_returns_none(
        self, redis_client, station_client,
    ):
        await redis_client.set(
            "aexis:station:corrupt:state", "{{{invalid json}}"
        )
        result = await station_client.get_station_state("corrupt")
        assert result is None
