
import asyncio
from datetime import UTC, datetime

import pytest

from aexis.core.model import (
    LocationDescriptor,
    Coordinate,
    PodStatus,
)
from aexis.tests.conftest import (
    async_seed_passenger,
    make_cargo_pod,
    make_passenger_pod,
)

def _make_status_tracker(pod):
    status_log = []
    original_publish = pod._publish_status_update

    async def tracking_publish():
        status_log.append(pod.status)
        await original_publish()

    pod._publish_status_update = tracking_publish
    return status_log

def _make_event_tracker(pod):
    events = []
    original_publish = pod._publish_event

    async def tracking_publish(event):
        events.append({
            "type": getattr(event, "event_type", str(type(event).__name__)),
            "station_id": getattr(event, "station_id", None),
            "passenger_id": getattr(event, "passenger_id", None),
            "request_id": getattr(event, "request_id", None),
        })
        await original_publish(event)

    pod._publish_event = tracking_publish
    return events

def _make_snapshot_tracker(pod):
    snapshots = []
    original_snapshot = pod._publish_state_snapshot

    async def tracking_snapshot():
        snapshots.append({
            "status": pod.status.value,
            "passenger_count": len(getattr(pod, "passengers", [])),
            "cargo_count": len(getattr(pod, "cargo", [])),
            "weight": getattr(pod, "current_weight", 0.0),
        })
        await original_snapshot()

    pod._publish_state_snapshot = tracking_snapshot
    return snapshots

class TestPassengerMultiStopMicroTransitions:

    def _make_loaded_pod(self, message_bus, redis_client, station_client):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        now = datetime.now(UTC)
        pod.passengers = [
            {"passenger_id": "p_a", "destination": "2", "pickup_time": now},
            {"passenger_id": "p_b", "destination": "3", "pickup_time": now},
            {"passenger_id": "p_c", "destination": "4", "pickup_time": now},
        ]
        return pod

    async def test_status_sequence_across_3_stops(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)
        status_log = _make_status_tracker(pod)

        await pod._execute_delivery("2")

        await pod._execute_delivery("3")

        await pod._execute_delivery("4")

        unloading_entries = [s for s in status_log if s == PodStatus.UNLOADING]
        assert len(unloading_entries) == 3

    async def test_passenger_count_at_each_micro_step(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)

        assert len(pod.passengers) == 3

        await pod._execute_delivery("2")
        assert len(pod.passengers) == 2
        remaining_ids = {p["passenger_id"] for p in pod.passengers}
        assert remaining_ids == {"p_b", "p_c"}

        await pod._execute_delivery("3")
        assert len(pod.passengers) == 1
        remaining_ids = {p["passenger_id"] for p in pod.passengers}
        assert remaining_ids == {"p_c"}

        await pod._execute_delivery("4")
        assert len(pod.passengers) == 0

    async def test_events_published_per_stop(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)
        events = _make_event_tracker(pod)

        await pod._execute_delivery("2")
        delivered_at_2 = [
            e for e in events
            if e["type"] == "PassengerDelivered" and e["station_id"] == "2"
        ]
        assert len(delivered_at_2) == 1
        assert delivered_at_2[0]["passenger_id"] == "p_a"

        await pod._execute_delivery("3")
        delivered_at_3 = [
            e for e in events
            if e["type"] == "PassengerDelivered" and e["station_id"] == "3"
        ]
        assert len(delivered_at_3) == 1
        assert delivered_at_3[0]["passenger_id"] == "p_b"

        await pod._execute_delivery("4")
        delivered_at_4 = [
            e for e in events
            if e["type"] == "PassengerDelivered" and e["station_id"] == "4"
        ]
        assert len(delivered_at_4) == 1
        assert delivered_at_4[0]["passenger_id"] == "p_c"

        total_delivered = [e for e in events if e["type"] == "PassengerDelivered"]
        assert len(total_delivered) == 3

    async def test_snapshot_written_at_each_unloading(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)
        snapshots = _make_snapshot_tracker(pod)

        await pod._execute_delivery("2")
        await pod._execute_delivery("3")
        await pod._execute_delivery("4")

        assert len(snapshots) == 3

        assert snapshots[0]["status"] == "unloading"
        assert snapshots[0]["passenger_count"] == 3

        assert snapshots[1]["status"] == "unloading"
        assert snapshots[1]["passenger_count"] == 2

        assert snapshots[2]["status"] == "unloading"
        assert snapshots[2]["passenger_count"] == 1

    async def test_post_final_delivery_status(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)

        await pod._execute_delivery("2")
        assert pod.status == PodStatus.EN_ROUTE

        await pod._execute_delivery("3")
        assert pod.status == PodStatus.EN_ROUTE

        await pod._execute_delivery("4")
        assert pod.status == PodStatus.EN_ROUTE
        assert len(pod.passengers) == 0

class TestCargoMultiStopWeight:

    def _make_loaded_pod(self, message_bus, redis_client, station_client):
        pod = make_cargo_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        now = datetime.now(UTC)
        pod.cargo = [
            {"request_id": "c_a", "destination": "2", "weight": 100.0, "pickup_time": now},
            {"request_id": "c_b", "destination": "3", "weight": 150.0, "pickup_time": now},
            {"request_id": "c_c", "destination": "4", "weight": 200.0, "pickup_time": now},
        ]
        pod.current_weight = 450.0
        return pod

    async def test_weight_decrement_at_each_stop(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)
        assert pod.current_weight == 450.0

        await pod._execute_delivery("2")
        assert pod.current_weight == 350.0

        await pod._execute_delivery("3")
        assert pod.current_weight == 200.0

        await pod._execute_delivery("4")
        assert pod.current_weight == 0.0

    async def test_cargo_count_at_each_step(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)

        await pod._execute_delivery("2")
        assert len(pod.cargo) == 2
        remaining = {c["request_id"] for c in pod.cargo}
        assert remaining == {"c_b", "c_c"}

        await pod._execute_delivery("3")
        assert len(pod.cargo) == 1
        assert pod.cargo[0]["request_id"] == "c_c"

        await pod._execute_delivery("4")
        assert len(pod.cargo) == 0

    async def test_weight_never_goes_negative(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)

        await pod._execute_delivery("2")
        assert pod.current_weight >= 0.0
        await pod._execute_delivery("3")
        assert pod.current_weight >= 0.0
        await pod._execute_delivery("4")
        assert pod.current_weight >= 0.0
        assert pod.current_weight == 0.0

    async def test_cargo_events_per_stop(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)
        events = _make_event_tracker(pod)

        await pod._execute_delivery("2")
        cargo_delivered = [e for e in events if e["type"] == "CargoDelivered"]
        assert len(cargo_delivered) == 1
        assert cargo_delivered[0]["request_id"] == "c_a"

        await pod._execute_delivery("3")
        cargo_delivered = [e for e in events if e["type"] == "CargoDelivered"]
        assert len(cargo_delivered) == 2

        await pod._execute_delivery("4")
        cargo_delivered = [e for e in events if e["type"] == "CargoDelivered"]
        assert len(cargo_delivered) == 3

    async def test_status_sequence_cargo(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = self._make_loaded_pod(message_bus, redis_client, station_client)
        status_log = _make_status_tracker(pod)

        await pod._execute_delivery("2")
        assert pod.status == PodStatus.EN_ROUTE
        await pod._execute_delivery("3")
        assert pod.status == PodStatus.EN_ROUTE
        await pod._execute_delivery("4")
        assert pod.status == PodStatus.EN_ROUTE

        unloading_entries = [s for s in status_log if s == PodStatus.UNLOADING]
        assert len(unloading_entries) == 3

class TestDeliveryThenPickupAtSameStation:

    async def test_deliver_then_pickup_status_sequence(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="2"
        )
        now = datetime.now(UTC)
        pod.passengers = [
            {"passenger_id": "p_a", "destination": "2", "pickup_time": now},
            {"passenger_id": "p_b", "destination": "4", "pickup_time": now},
        ]

        await async_seed_passenger(redis_client, "2", "p_new", "3")

        status_log = _make_status_tracker(pod)

        await pod._execute_delivery("2")

        assert pod.status == PodStatus.EN_ROUTE
        assert len(pod.passengers) == 1
        assert pod.passengers[0]["passenger_id"] == "p_b"

        delivery_statuses = [s for s in status_log if s == PodStatus.UNLOADING]
        assert len(delivery_statuses) == 1

        await pod._execute_pickup("2")

        pickup_statuses = [s for s in status_log if s == PodStatus.LOADING]
        assert len(pickup_statuses) == 1

    async def test_passenger_list_between_delivery_and_pickup(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="2"
        )
        now = datetime.now(UTC)
        pod.passengers = [
            {"passenger_id": "p_a", "destination": "2", "pickup_time": now},
            {"passenger_id": "p_b", "destination": "4", "pickup_time": now},
        ]
        await async_seed_passenger(redis_client, "2", "p_new", "3")

        await pod._execute_delivery("2")

        ids_after_delivery = [p["passenger_id"] for p in pod.passengers]
        assert ids_after_delivery == ["p_b"]
        assert len(pod.passengers) == 1

    async def test_passenger_list_after_pickup(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="2"
        )
        now = datetime.now(UTC)
        pod.passengers = [
            {"passenger_id": "p_a", "destination": "2", "pickup_time": now},
            {"passenger_id": "p_b", "destination": "4", "pickup_time": now},
        ]
        await async_seed_passenger(redis_client, "2", "p_new", "3")

        await pod._execute_delivery("2")
        await pod._execute_pickup("2")

        ids = {p["passenger_id"] for p in pod.passengers}
        assert "p_b" in ids
        assert "p_new" in ids
        assert "p_a" not in ids
        assert len(pod.passengers) == 2

class TestPassThroughNoDelivery:

    async def test_no_unloading_status_when_no_match(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        pod.passengers = [
            {"passenger_id": "p_x", "destination": "3",
             "pickup_time": datetime.now(UTC)},
        ]
        status_log = _make_status_tracker(pod)

        await pod._execute_delivery("2")

        assert PodStatus.UNLOADING not in status_log
        assert len(pod.passengers) == 1
        assert pod.passengers[0]["passenger_id"] == "p_x"

    async def test_cargo_pass_through_no_weight_change(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        pod.cargo = [
            {"request_id": "c_x", "destination": "4", "weight": 100.0,
             "pickup_time": datetime.now(UTC)},
        ]
        pod.current_weight = 100.0

        await pod._execute_delivery("2")

        assert pod.current_weight == 100.0
        assert len(pod.cargo) == 1

class TestDuplicateDestination:

    async def test_both_delivered_at_same_stop(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        now = datetime.now(UTC)
        pod.passengers = [
            {"passenger_id": "p_1", "destination": "2", "pickup_time": now},
            {"passenger_id": "p_2", "destination": "2", "pickup_time": now},
            {"passenger_id": "p_3", "destination": "3", "pickup_time": now},
        ]
        events = _make_event_tracker(pod)

        await pod._execute_delivery("2")

        delivered_ids = [
            e["passenger_id"]
            for e in events if e["type"] == "PassengerDelivered"
        ]
        assert set(delivered_ids) == {"p_1", "p_2"}

        assert len(pod.passengers) == 1
        assert pod.passengers[0]["passenger_id"] == "p_3"

    async def test_duplicate_cargo_destination_weight(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_cargo_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        now = datetime.now(UTC)
        pod.cargo = [
            {"request_id": "c_1", "destination": "2", "weight": 80.0, "pickup_time": now},
            {"request_id": "c_2", "destination": "2", "weight": 120.0, "pickup_time": now},
            {"request_id": "c_3", "destination": "3", "weight": 50.0, "pickup_time": now},
        ]
        pod.current_weight = 250.0

        await pod._execute_delivery("2")

        assert pod.current_weight == 50.0
        assert len(pod.cargo) == 1
        assert pod.cargo[0]["request_id"] == "c_3"

class TestCargoWeightTrackingAcrossStops:

    async def test_weight_through_delivery_and_pickup(
        self, message_bus, redis_client, station_client, network_context,
    ):
        from aexis.tests.conftest import async_seed_cargo

        pod = make_cargo_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        now = datetime.now(UTC)
        pod.cargo = [
            {"request_id": "c_a", "destination": "2", "weight": 100.0, "pickup_time": now},
            {"request_id": "c_b", "destination": "3", "weight": 200.0, "pickup_time": now},
        ]
        pod.current_weight = 300.0

        await pod._execute_delivery("2")
        assert pod.current_weight == 200.0
        assert len(pod.cargo) == 1

        await pod._execute_delivery("3")
        assert pod.current_weight == 0.0
        assert len(pod.cargo) == 0

        await async_seed_cargo(redis_client, "3", "c_new", "4", 50.0)

        pod.location_descriptor = LocationDescriptor(
            location_type="station", node_id="3",
            coordinate=Coordinate(0, 100),
        )
        await pod._execute_pickup("3")

        assert pod.current_weight == 50.0
        assert len(pod.cargo) == 1
        assert pod.cargo[0]["request_id"] == "c_new"
