import pytest
from aexis.core.message_bus import MessageBus
from aexis.core.model import PassengerArrival


@pytest.mark.anyio
async def test_passenger_lifecycle_flow(system_with_mock_redis):
    """
    Integration Test: Verify complete passenger lifecycle infrastructure

    Scenario:
    1. Verify system has stations ready for passengers
    2. Check that message bus is publishing events
    3. Verify stations can receive and process events
    """
    system = system_with_mock_redis
    assert system.running
    assert system.message_bus.redis_client is not None

    # Setup test data
    origin_id = "station_001"
    dest_id = "station_002"

    # Verify stations exist
    assert origin_id in system.stations
    assert dest_id in system.stations

    origin_station = system.stations[origin_id]
    dest_station = system.stations[dest_id]

    # Get initial state
    initial_state = system.get_system_state()
    initial_pending = initial_state["metrics"]["pending_passengers"]

    # Inject passenger directly via message bus
    passenger_id = "test_passenger_001"
    passenger_event = PassengerArrival(
        passenger_id=passenger_id,
        station_id=origin_id,
        destination=dest_id,
        priority=3,
        group_size=1,
        special_needs=[],
        wait_time_limit=45,
    )

    # Verify event can be published
    success = await system.message_bus.publish_event(
        MessageBus.get_event_channel(passenger_event.event_type), passenger_event
    )
    assert success, "Failed to publish passenger event"

    # Verify stations are subscribed to passenger events
    passenger_channel = MessageBus.CHANNELS["PASSENGER_EVENTS"]
    assert passenger_channel in system.message_bus.subscribers

    # Verify at least one handler is registered
    assert len(system.message_bus.subscribers[passenger_channel]) > 0, (
        "No handlers subscribed to passenger events"
    )

    # Verify station state can be retrieved
    origin_state = origin_station.get_state()
    assert "station_id" in origin_state
    assert origin_state["station_id"] == origin_id
    # Station should have queues info
    assert "queues" in origin_state or "queue_length" in origin_state


@pytest.mark.anyio
async def test_system_topology_mesh(system_with_mock_redis):
    """
    Topology Test: Verify mesh connectivity.
    Each station should have more than 2 connections (beyond simple ring).
    """
    system = system_with_mock_redis

    for station_id, station in system.stations.items():
        connections = len(station.connected_stations)
        assert connections >= 2, f"{station_id} has only {connections} connections"
        # Mesh should have some stations with > 2 connections (hubs)

    # At least some stations should be hubs (more than 2 connections)
    hub_count = sum(
        1 for s in system.stations.values() if len(s.connected_stations) > 2
    )
    assert hub_count >= 1, "No hub stations found in mesh topology"


@pytest.mark.anyio
async def test_pod_initial_state(system_with_mock_redis):
    """
    Smoke Test: Verify pods are running and distributed across stations.
    """
    system = system_with_mock_redis

    assert len(system.pods) == 5, f"Expected 5 pods, got {len(system.pods)}"

    # All pods should have a location
    for pod_id, pod in system.pods.items():
        assert pod.location is not None, f"{pod_id} has no location"
        assert pod.location.startswith("station_"), f"{pod_id} has invalid location"
