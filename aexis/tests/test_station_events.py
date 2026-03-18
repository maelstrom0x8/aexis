
import json

import pytest

from aexis.core.message_bus import MessageBus
from aexis.core.model import Priority, StationStatus
from aexis.station import Station

class TestStationEventDispatch:

    async def test_passenger_arrival_dispatches(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_event({
            "message": {
                "event_type": "PassengerArrival",
                "station_id": "1",
                "passenger_id": "p_001",
                "destination": "2",
            }
        })
        assert station._passenger_count == 1

    async def test_passenger_picked_up_dispatches(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_arrival({
            "station_id": "1", "passenger_id": "p_001", "destination": "2",
        })
        await station._handle_passenger_event({
            "message": {
                "event_type": "PassengerPickedUp",
                "station_id": "1",
                "passenger_id": "p_001",
            }
        })
        assert station._passenger_count == 0

    async def test_unknown_passenger_event_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_event({
            "message": {
                "event_type": "UnknownEvent",
                "station_id": "1",
            }
        })
        assert station._passenger_count == 0

    async def test_cargo_request_dispatches(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_event({
            "message": {
                "event_type": "CargoRequest",
                "origin": "1",
                "request_id": "c_001",
                "destination": "2",
                "weight": 50.0,
            }
        })
        assert station._cargo_count == 1

    async def test_cargo_loaded_dispatches(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "1", "request_id": "c_001",
            "destination": "2", "weight": 50.0,
        })
        await station._handle_cargo_event({
            "message": {
                "event_type": "CargoLoaded",
                "station_id": "1",
                "request_id": "c_001",
            }
        })
        assert station._cargo_count == 0

    async def test_unknown_cargo_event_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_event({
            "message": {"event_type": "UnknownCargo", "origin": "1"}
        })
        assert station._cargo_count == 0

class TestStationPassengerFlow:

    async def test_01_arrival_writes_to_redis(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_arrival({
            "station_id": "1",
            "passenger_id": "p_flow",
            "destination": "2",
            "priority": Priority.HIGH.value,
        })
        raw = await redis_client.hget("aexis:station:1:passengers", "p_flow")
        assert raw is not None
        data = json.loads(raw)
        assert data["destination"] == "2"
        assert data["priority"] == Priority.HIGH.value
        assert station._passenger_count == 1

    async def test_02_pickup_removes_from_redis(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_arrival({
            "station_id": "1", "passenger_id": "p_flow", "destination": "2",
        })
        await station._handle_passenger_pickup({
            "station_id": "1", "passenger_id": "p_flow",
        })
        remaining = await redis_client.hlen("aexis:station:1:passengers")
        assert remaining == 0
        assert station.total_passengers_processed == 1

    async def test_03_wrong_station_arrival_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_arrival({
            "station_id": "2",
            "passenger_id": "p_wrong",
            "destination": "3",
        })
        assert station._passenger_count == 0

    async def test_04_missing_passenger_id_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_arrival({
            "station_id": "1",
            "destination": "2",

        })
        assert station._passenger_count == 0

class TestStationCargoFlow:

    async def test_01_request_writes_to_redis(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "1",
            "request_id": "c_flow",
            "destination": "3",
            "weight": 75.0,
        })
        raw = await redis_client.hget("aexis:station:1:cargo", "c_flow")
        assert raw is not None
        assert station._cargo_count == 1

    async def test_02_loading_removes_from_redis(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "1", "request_id": "c_flow",
            "destination": "3", "weight": 75.0,
        })
        await station._handle_cargo_loading({
            "station_id": "1", "request_id": "c_flow",
        })
        remaining = await redis_client.hlen("aexis:station:1:cargo")
        assert remaining == 0
        assert station.total_cargo_processed == 1

    async def test_03_wrong_origin_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "2",
            "request_id": "c_wrong",
            "destination": "3",
            "weight": 50.0,
        })
        assert station._cargo_count == 0

    async def test_04_missing_request_id_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "1",
            "destination": "3",
            "weight": 50.0,

        })
        assert station._cargo_count == 0

class TestDeliveryIsInformational:

    async def test_passenger_delivery_no_mutation(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_passenger_arrival({
            "station_id": "1", "passenger_id": "p_stay", "destination": "2",
        })
        await station._handle_passenger_delivery({
            "station_id": "1", "passenger_id": "p_delivered",
        })
        assert station._passenger_count == 1

    async def test_cargo_delivery_no_mutation(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_request({
            "origin": "1", "request_id": "c_stay",
            "destination": "3", "weight": 50.0,
        })
        await station._handle_cargo_delivery({
            "station_id": "1", "request_id": "c_delivered",
        })
        assert station._cargo_count == 1

    async def test_delivery_wrong_station_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")

        await station._handle_passenger_delivery({
            "station_id": "999", "passenger_id": "p_x",
        })
        await station._handle_cargo_delivery({
            "station_id": "999", "request_id": "c_x",
        })

class TestStationBayManagement:

    async def test_pod_arrival_decrements_bay(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        assert station.available_bays == 4
        await station._handle_pod_event({
            "message": {
                "event_type": "PodArrival",
                "station_id": "1",
            }
        })
        assert station.available_bays == 3

    async def test_pod_arrival_wrong_station_no_change(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_pod_event({
            "message": {
                "event_type": "PodArrival",
                "station_id": "other",
            }
        })
        assert station.available_bays == 4

    async def test_pod_departure_increments_bay(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        station.available_bays = 2
        await station._handle_pod_event({
            "message": {
                "event_type": "PodStatusUpdate",
                "location": "1",
                "status": "en_route",
            }
        })
        assert station.available_bays == 3

    async def test_bays_floor_at_zero(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        station.available_bays = 0
        await station._handle_pod_event({
            "message": {
                "event_type": "PodArrival",
                "station_id": "1",
            }
        })
        assert station.available_bays == 0

    async def test_bays_ceiling_at_loading_bays(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        station.available_bays = 4
        station.loading_bays = 4
        await station._handle_pod_event({
            "message": {
                "event_type": "PodStatusUpdate",
                "location": "1",
                "status": "en_route",
            }
        })
        assert station.available_bays == 4

class TestCongestionBoundaries:

    @pytest.mark.parametrize("passengers,cargo,bays_used,expected_status", [

        (5, 0, 0, StationStatus.OPERATIONAL),

        (15, 5, 2, StationStatus.OPERATIONAL),

        (20, 10, 4, StationStatus.CONGESTED),

        (20, 8, 2, StationStatus.OPERATIONAL),

        (20, 10, 2, StationStatus.CONGESTED),
    ])
    async def test_congestion_threshold(
        self, message_bus, redis_client,
        passengers, cargo, bays_used, expected_status,
    ):
        station = Station(message_bus, redis_client, "1")
        station._passenger_count = passengers
        station._cargo_count = cargo
        station.loading_bays = 4
        station.available_bays = 4 - bays_used
        station._update_congestion_level()
        assert station.status == expected_status

    async def test_recovery_from_congested_to_operational(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")

        station._passenger_count = 25
        station._cargo_count = 15
        station.available_bays = 0
        station._update_congestion_level()
        assert station.status == StationStatus.CONGESTED

        station._passenger_count = 2
        station._cargo_count = 0
        station.available_bays = 4
        station._update_congestion_level()
        assert station.status == StationStatus.OPERATIONAL
        assert station.congestion_level < 0.3

class TestCongestionAlertSeverity:

    async def test_alert_suppressed_at_low_congestion(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        station.congestion_level = 0.5

        events = []
        original_publish = message_bus.publish_event

        async def intercept(channel, event):
            events.append(event.event_type)
            return await original_publish(channel, event)

        message_bus.publish_event = intercept
        await station._publish_congestion_alert()
        assert "CongestionAlert" not in [e for e in events]

    @pytest.mark.parametrize("level,expected_severity", [
        (0.72, "medium"),
        (0.85, "high"),
        (0.95, "critical"),
    ])
    async def test_severity_gradation(
        self, message_bus, redis_client,
        level, expected_severity,
    ):
        station = Station(message_bus, redis_client, "1")
        station.congestion_level = level
        station._passenger_count = 10
        station._cargo_count = 5

        published_events = []
        original_publish = message_bus.publish_event

        async def intercept(channel, event):
            published_events.append(event)
            return await original_publish(channel, event)

        message_bus.publish_event = intercept
        await station._publish_congestion_alert()

        congestion_alerts = [
            e for e in published_events
            if hasattr(e, "event_type") and e.event_type == "CongestionAlert"
        ]
        assert len(congestion_alerts) == 1
        assert congestion_alerts[0].severity == expected_severity

class TestCapacityUpdateCommand:

    async def test_valid_update(self, message_bus, redis_client):
        station = Station(message_bus, redis_client, "1")
        await station._handle_capacity_update({
            "message": {
                "parameters": {
                    "max_pods": 8,
                    "processing_rate": 5.0,
                }
            }
        })
        assert station.loading_bays == 8
        assert station.processing_rate == 5.0

    async def test_command_for_wrong_station_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_system_command({
            "message": {
                "command_type": "UpdateCapacity",
                "target": "other_station",
                "parameters": {"max_pods": 10},
            }
        })
        assert station.loading_bays == 4

    async def test_unknown_command_type_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_system_command({
            "message": {
                "command_type": "UnknownCommand",
                "target": "1",
            }
        })

        assert station.loading_bays == 4

    async def test_missing_params_uses_defaults(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        original_bays = station.loading_bays
        original_rate = station.processing_rate
        await station._handle_capacity_update({
            "message": {"parameters": {}}
        })
        assert station.loading_bays == original_bays
        assert station.processing_rate == original_rate

class TestPassengerPickupEdgeCases:

    async def test_pickup_nonexistent_no_crash(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_pickup({
            "station_id": "1",
            "passenger_id": "p_ghost",
        })
        assert station._passenger_count == 0

    async def test_missing_pickup_passenger_id_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_passenger_pickup({
            "station_id": "1",

        })
        assert station._passenger_count == 0

class TestCargoLoadingEdgeCases:

    async def test_loading_nonexistent_no_crash(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_loading({
            "station_id": "1",
            "request_id": "c_ghost",
        })
        assert station._cargo_count == 0

    async def test_missing_loading_request_id_ignored(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")
        await station._handle_cargo_loading({
            "station_id": "1",

        })
        assert station._cargo_count == 0

class TestStateSnapshotResilience:

    async def test_snapshot_survives_redis_failure(
        self, message_bus, redis_client,
    ):
        station = Station(message_bus, redis_client, "1")

        async def failing_set(*args, **kwargs):
            raise ConnectionError("Redis unavailable")

        station._redis.set = failing_set
        await station._publish_state_snapshot()

        assert station.status == StationStatus.OPERATIONAL

class TestClaimCorruptData:

    async def test_corrupt_passenger_json_skipped(
        self, station_client, redis_client,
    ):

        await redis_client.hset(
            "aexis:station:1:passengers", "p_corrupt", "{{not json}}"
        )
        pending = await station_client.get_pending_passengers("1")

        assert len(pending) == 0

    async def test_corrupt_cargo_json_skipped(
        self, station_client, redis_client,
    ):
        await redis_client.hset(
            "aexis:station:1:cargo", "c_corrupt", "not-json-at-all"
        )
        pending = await station_client.get_pending_cargo("1")
        assert len(pending) == 0
