
import json
from datetime import UTC, datetime

import pytest

from aexis.core.model import (
    Coordinate,
    LocationDescriptor,
    PodStatus,
    StationStatus,
)
from aexis.station import Station
from aexis.tests.conftest import (
    async_seed_passenger,
    async_seed_cargo,
    make_cargo_pod,
    make_passenger_pod,
)

async def _read_pod_state(redis_client, pod_id: str) -> dict | None:
    raw = await redis_client.get(f"aexis:pod:{pod_id}:state")
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

async def _read_all_pod_states(redis_client) -> dict[str, dict]:
    result = {}
    async for key in redis_client.scan_iter(match="aexis:pod:*:state", count=100):
        parts = key.split(":")
        if len(parts) == 4:
            pod_id = parts[2]
            raw = await redis_client.get(key)
            if raw:
                try:
                    result[pod_id] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    pass
    return result

async def _read_station_state(redis_client, station_id: str) -> dict | None:
    raw = await redis_client.get(f"aexis:station:{station_id}:state")
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

async def _read_all_station_states(redis_client) -> dict[str, dict]:
    result = {}
    async for key in redis_client.scan_iter(match="aexis:station:*:state", count=100):
        parts = key.split(":")
        if len(parts) == 4:
            station_id = parts[2]
            raw = await redis_client.get(key)
            if raw:
                try:
                    result[station_id] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    pass
    return result

class TestPodStateReflection:

    async def test_pod_state_before_and_after_pickup(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_test", station_id="1",
        )

        state = await _read_pod_state(redis_client, "pod_test")
        assert state is None

        await pod._publish_state_snapshot()

        state = await _read_pod_state(redis_client, "pod_test")
        assert state is not None
        assert state["status"] == "idle"
        assert len(state.get("passengers", [])) == 0

        await async_seed_passenger(redis_client, "1", "p_snap", "2")
        await pod._execute_pickup("1")

        await pod._publish_state_snapshot()
        state = await _read_pod_state(redis_client, "pod_test")
        assert state["status"] == "en_route"
        assert len(state["passengers"]) == 1
        assert state["passengers"][0]["passenger_id"] == "p_snap"

    async def test_pod_state_during_unloading(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_unload", station_id="1",
        )
        pod.passengers = [
            {"passenger_id": "p_del", "destination": "2",
             "pickup_time": datetime.now(UTC)},
        ]

        snapshots_captured = []
        original_snapshot = pod._publish_state_snapshot

        async def capturing_snapshot():
            snapshots_captured.append({
                "status": pod.status.value,
                "passenger_count": len(pod.passengers),
            })
            await original_snapshot()

        pod._publish_state_snapshot = capturing_snapshot

        await pod._execute_delivery("2")

        assert len(snapshots_captured) == 1
        assert snapshots_captured[0]["status"] == "unloading"
        assert snapshots_captured[0]["passenger_count"] == 1

        pod._publish_state_snapshot = original_snapshot
        await pod._publish_state_snapshot()
        state = await _read_pod_state(redis_client, "pod_unload")
        assert state["status"] == "en_route"
        assert len(state.get("passengers", [])) == 0

    async def test_pod_location_updates_on_arrival(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_loc", station_id="1",
        )

        pod.location_descriptor = LocationDescriptor(
            location_type="edge",
            edge_id="1->2",
            coordinate=Coordinate(50, 50),
        )
        await pod._publish_state_snapshot()

        state = await _read_pod_state(redis_client, "pod_loc")
        assert "on edge" in state["location"]

        pod.location_descriptor = LocationDescriptor(
            location_type="station",
            node_id="2",
            coordinate=Coordinate(100, 0),
        )
        pod.status = PodStatus.IDLE
        await pod._publish_state_snapshot()

        state = await _read_pod_state(redis_client, "pod_loc")
        assert state["location"] == "2"
        assert state["status"] == "idle"

class TestStationStateReflection:

    async def test_station_queue_after_single_arrival(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["passengers"]["waiting"] == 0

        await station._handle_passenger_arrival({
            "station_id": "1", "passenger_id": "p_q1", "destination": "2",
        })
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["passengers"]["waiting"] == 1

    async def test_station_queue_after_pickup(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")

        for i in range(3):
            await station._handle_passenger_arrival({
                "station_id": "1", "passenger_id": f"p_{i}", "destination": "2",
            })

        await station._publish_state_snapshot()
        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["passengers"]["waiting"] == 3
        assert state["metrics"]["total_passengers_processed"] == 0

        await station._handle_passenger_pickup({
            "station_id": "1", "passenger_id": "p_0",
        })
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["passengers"]["waiting"] == 2
        assert state["metrics"]["total_passengers_processed"] == 1

    async def test_congestion_transitions_visible(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["status"] == StationStatus.OPERATIONAL.value

        station._passenger_count = 30
        station._cargo_count = 15
        station.available_bays = 0
        station._update_congestion_level()
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["status"] == StationStatus.CONGESTED.value
        assert state["congestion_level"] > 0.7

class TestCrossComponentPropagation:

    async def test_pod_delivery_updates_both_snapshots(
        self, message_bus, redis_client, station_client, network_context,
    ):

        station = Station(message_bus, redis_client, "2")
        await station._handle_passenger_arrival({
            "station_id": "2", "passenger_id": "p_cross", "destination": "3",
        })
        await station._publish_state_snapshot()

        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_cross", station_id="2",
        )
        pod.passengers = [
            {"passenger_id": "p_del", "destination": "2",
             "pickup_time": datetime.now(UTC)},
        ]
        await pod._execute_delivery("2")
        await pod._publish_state_snapshot()

        pod_state = await _read_pod_state(redis_client, "pod_cross")
        assert pod_state["status"] == "en_route"
        assert len(pod_state.get("passengers", [])) == 0

        st_state = await _read_station_state(redis_client, "2")
        assert st_state["queues"]["passengers"]["waiting"] == 1

    async def test_inject_passenger_reflects_in_station_queue(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["passengers"]["waiting"] == 0

        await station._handle_passenger_arrival({
            "station_id": "1", "passenger_id": "p_injected", "destination": "3",
        })
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["passengers"]["waiting"] == 1

    async def test_inject_cargo_reflects_in_station_queue(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["cargo"]["waiting"] == 0

        await station._handle_cargo_request({
            "origin": "1", "request_id": "c_inj", "destination": "2", "weight": 80.0,
        })
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["queues"]["cargo"]["waiting"] == 1

    async def test_system_aggregation_across_pods_and_stations(
        self, message_bus, redis_client, station_client, network_context,
    ):

        pod1 = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_agg_1", station_id="1",
        )
        pod1.passengers = [
            {"passenger_id": "pa_1", "destination": "2", "pickup_time": datetime.now(UTC)},
            {"passenger_id": "pa_2", "destination": "3", "pickup_time": datetime.now(UTC)},
        ]
        pod1.status = PodStatus.EN_ROUTE
        await pod1._publish_state_snapshot()

        pod2 = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_agg_2", station_id="2",
        )
        pod2.passengers = [
            {"passenger_id": "pb_1", "destination": "4", "pickup_time": datetime.now(UTC)},
        ]
        pod2.status = PodStatus.EN_ROUTE
        await pod2._publish_state_snapshot()

        station = Station(message_bus, redis_client, "1")
        station._passenger_count = 3
        await station._publish_state_snapshot()

        pods = await _read_all_pod_states(redis_client)
        total_onboard = sum(len(p.get("passengers", [])) for p in pods.values())
        assert total_onboard == 3

        en_route_count = sum(1 for p in pods.values() if p.get("status") == "en_route")
        assert en_route_count == 2

        stations = await _read_all_station_states(redis_client)
        total_waiting = sum(
            s.get("queues", {}).get("passengers", {}).get("waiting", 0)
            for s in stations.values()
        )
        assert total_waiting == 3

    async def test_metrics_accumulate(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")

        for i in range(5):
            await station._handle_passenger_arrival({
                "station_id": "1", "passenger_id": f"p_met_{i}", "destination": "2",
            })
        for i in range(5):
            await station._handle_passenger_pickup({
                "station_id": "1", "passenger_id": f"p_met_{i}",
            })
        await station._publish_state_snapshot()

        state = await _read_station_state(redis_client, "1")
        assert state["metrics"]["total_passengers_processed"] == 5

class TestScanResilience:

    async def test_corrupt_pod_json_skipped(self, redis_client):
        await redis_client.set("aexis:pod:bad:state", "{{{invalid")
        pods = await _read_all_pod_states(redis_client)
        assert "bad" not in pods

    async def test_corrupt_station_json_skipped(self, redis_client):
        await redis_client.set("aexis:station:bad:state", "not-json")
        stations = await _read_all_station_states(redis_client)
        assert "bad" not in stations

    async def test_valid_and_corrupt_mixed(self, redis_client):
        await redis_client.set(
            "aexis:pod:good:state",
            json.dumps({"pod_id": "good", "status": "idle"}),
        )
        await redis_client.set("aexis:pod:bad:state", "{{corrupt}}")

        pods = await _read_all_pod_states(redis_client)
        assert "good" in pods
        assert "bad" not in pods

    async def test_missing_pod_returns_none(self, redis_client):
        state = await _read_pod_state(redis_client, "nonexistent")
        assert state is None

    async def test_missing_station_returns_none(self, redis_client):
        state = await _read_station_state(redis_client, "nonexistent")
        assert state is None

class TestSnapshotRoundTrip:

    async def test_pod_roundtrip_integrity(
        self, message_bus, redis_client, station_client, network_context,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_rt", station_id="1",
        )
        pod.passengers = [
            {"passenger_id": "p_rt", "destination": "3",
             "pickup_time": datetime.now(UTC)},
        ]
        pod.status = PodStatus.EN_ROUTE

        local_state = pod.get_state()
        await pod._publish_state_snapshot()
        redis_state = await _read_pod_state(redis_client, "pod_rt")

        assert redis_state["pod_id"] == local_state["pod_id"]
        assert redis_state["status"] == local_state["status"]
        assert redis_state["location"] == local_state["location"]
        assert len(redis_state["passengers"]) == len(local_state["passengers"])
        assert redis_state["passengers"][0]["passenger_id"] == "p_rt"

    async def test_station_roundtrip_integrity(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        station._passenger_count = 7
        station._cargo_count = 3
        station.total_passengers_processed = 12

        local_state = station.get_state()
        await station._publish_state_snapshot()
        redis_state = await _read_station_state(redis_client, "1")

        assert redis_state["station_id"] == local_state["station_id"]
        assert redis_state["status"] == local_state["status"]
        assert redis_state["queues"]["passengers"]["waiting"] == 7
        assert redis_state["queues"]["cargo"]["waiting"] == 3
        assert redis_state["metrics"]["total_passengers_processed"] == 12

class TestIncrementalStatePropagation:

    async def test_passenger_lifecycle_state_propagation(
        self, message_bus, redis_client, station_client, network_context,
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_passenger_arrival({
            "station_id": "1", "passenger_id": "p_lc", "destination": "2",
        })
        await station._publish_state_snapshot()

        st_state = await _read_station_state(redis_client, "1")
        assert st_state["queues"]["passengers"]["waiting"] == 1
        assert st_state["metrics"]["total_passengers_processed"] == 0

        pod = make_passenger_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_lc", station_id="1",
        )
        await pod._execute_pickup("1")
        await pod._publish_state_snapshot()

        pod_state = await _read_pod_state(redis_client, "pod_lc")
        assert pod_state["status"] == "en_route"
        assert len(pod_state["passengers"]) == 1

        await station._handle_passenger_pickup({
            "station_id": "1", "passenger_id": "p_lc",
        })
        await station._publish_state_snapshot()

        st_state = await _read_station_state(redis_client, "1")
        assert st_state["queues"]["passengers"]["waiting"] == 0
        assert st_state["metrics"]["total_passengers_processed"] == 1

        await pod._execute_delivery("2")
        await pod._publish_state_snapshot()

        pod_state = await _read_pod_state(redis_client, "pod_lc")
        assert pod_state["status"] == "en_route"
        assert len(pod_state.get("passengers", [])) == 0

    async def test_cargo_lifecycle_state_propagation(
        self, message_bus, redis_client, station_client, network_context,
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_cargo_request({
            "origin": "1", "request_id": "c_lc", "destination": "3", "weight": 100.0,
        })
        await station._publish_state_snapshot()

        st_state = await _read_station_state(redis_client, "1")
        assert st_state["queues"]["cargo"]["waiting"] == 1

        pod = make_cargo_pod(
            message_bus, redis_client, station_client,
            pod_id="pod_clc", station_id="1",
        )
        await async_seed_cargo(redis_client, "1", "c_lc", "3", 100.0)
        await pod._execute_pickup("1")
        await pod._publish_state_snapshot()

        pod_state = await _read_pod_state(redis_client, "pod_clc")
        assert len(pod_state.get("cargo", [])) == 1
        assert pod_state["cargo"][0]["request_id"] == "c_lc"

        await pod._execute_delivery("3")
        await pod._publish_state_snapshot()

        pod_state = await _read_pod_state(redis_client, "pod_clc")
        assert len(pod_state.get("cargo", [])) == 0
