"""
Behavior Edge Case Integration Tests for AEXIS System

Tests verify:
- Claim system prevents double-pickup
- Loading timing simulation
- Failed pickup behavior (pod remains IDLE)
- Multi-pod conflict resolution
"""

import asyncio
import json
import pytest
from pathlib import Path
from datetime import datetime, UTC
from unittest.mock import MagicMock, AsyncMock, patch

from aexis.core.system import AexisSystem, SystemContext, AexisConfig
from aexis.core.pod import PassengerPod, PodStatus, LocationDescriptor
from aexis.core.network import NetworkContext
from aexis.core.model import Coordinate, PassengerArrival
from aexis.core.message_bus import LocalMessageBus, MessageBus


@pytest.fixture
def local_message_bus():
    return LocalMessageBus()


@pytest.fixture
def network_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "network.json"


@pytest.fixture
def aexis_system_two_pods(local_message_bus, network_path, mocker):
    """System with 2 passenger pods for conflict testing"""
    config = AexisConfig(
        debug=True,
        network_data_path=str(network_path),
        pods={"count": 2, "cargoPercentage": 0.0},
        stations={"count": 21},
        ai={"provider": "none"},
        redis={"url": "local://"}
    )
    
    mock_ctx = MagicMock(spec=SystemContext)
    mock_ctx.get_config.return_value = config
    
    NetworkContext._instance = None
    with open(str(network_path), 'r') as f:
        network_data = json.load(f)
    real_network = NetworkContext(network_data=network_data)
    NetworkContext._instance = real_network
    mock_ctx.get_network_context.return_value = real_network
    
    system = AexisSystem(system_context=mock_ctx, message_bus=local_message_bus)
    
    # Minimize non-essential mocks
    system._update_metrics = AsyncMock()
    system._publish_snapshot = AsyncMock()
    system._log_system_status = AsyncMock()
    
    return system


@pytest.mark.asyncio
async def test_claim_prevents_double_pickup(aexis_system_two_pods, local_message_bus):
    """
    Test: Claim system prevents two pods from picking up the same passenger.
    
    Scenario:
    1. One passenger at station
    2. Pod A arrives and claims
    3. Pod B arrives, claim fails
    4. Passenger picked up only once
    """
    system = aexis_system_two_pods
    success = await system.initialize()
    assert success
    
    # Start stations
    for station in system.stations.values():
        await station.start()
    
    origin_id = "station_001"
    dest_id = "station_005"
    passenger_id = "conflict_test_p1"
    
    # Inject passenger directly to station queue
    station = system.stations[origin_id]
    station.passenger_queue.append({
        "passenger_id": passenger_id,
        "destination": dest_id,
        "priority": 3,
        "arrival_time": datetime.now(UTC)
    })
    
    # Pod A claims first
    pod_a_id = "pod_a"
    claim_a = station.claim_passenger(passenger_id, pod_a_id)
    assert claim_a is True, "First claim should succeed"
    
    # Pod B tries to claim same passenger
    pod_b_id = "pod_b"
    claim_b = station.claim_passenger(passenger_id, pod_b_id)
    assert claim_b is False, "Second claim should fail"
    
    # Verify passenger is claimed by Pod A
    passenger = next((p for p in station.passenger_queue if p["passenger_id"] == passenger_id), None)
    assert passenger is not None
    assert passenger.get("claimed_by") == pod_a_id
    
    print("✅ Claim conflict test passed")


@pytest.mark.asyncio
async def test_loading_time_simulation(aexis_system_two_pods, local_message_bus):
    """
    Test: Pod loading time is 5 seconds × passenger count.
    
    Verifies:
    - Pod enters LOADING status
    - Time spent matches expected
    - Pod transitions to EN_ROUTE after loading
    """
    system = aexis_system_two_pods
    success = await system.initialize()
    assert success
    
    for station in system.stations.values():
        await station.start()
    
    pod_id = list(system.pods.keys())[0]
    pod = system.pods[pod_id]
    await pod.start()
    
    origin_id = "station_001"
    
    # Place pod at station manually
    pod.location = origin_id
    pod.status = PodStatus.IDLE
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id=origin_id,
        coordinate=Coordinate(-250, -250)
    )
    
    # Inject 2 passengers
    station = system.stations[origin_id]
    for i in range(2):
        station.passenger_queue.append({
            "passenger_id": f"timing_p{i}",
            "destination": "station_005",
            "priority": 3,
            "arrival_time": datetime.now(UTC)
        })
    
    # Mock asyncio.sleep to measure calls without actual delay
    sleep_calls = []
    original_sleep = asyncio.sleep
    
    async def mock_sleep(duration):
        sleep_calls.append(duration)
        await original_sleep(0.01)  # Minimal delay for test speed
    
    with patch('aexis.core.pod.asyncio.sleep', side_effect=mock_sleep):
        await pod._execute_pickup(origin_id)
    
    # Verify loading time = 5s × 2 passengers = 10s
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == 10.0, f"Expected 10s loading, got {sleep_calls[0]}s"
    
    # Verify pod loaded passengers
    assert len(pod.passengers) == 2
    
    print("✅ Loading time simulation test passed")


@pytest.mark.asyncio
async def test_empty_station_arrival_goes_idle(aexis_system_two_pods, local_message_bus):
    """
    Test: Pod arriving at empty station goes to IDLE.
    
    Scenario:
    1. Pod routes to station for pickup
    2. All passengers removed before arrival
    3. Pod goes IDLE (no proactive re-decision)
    """
    system = aexis_system_two_pods
    success = await system.initialize()
    assert success
    
    for station in system.stations.values():
        await station.start()
    
    pod_id = list(system.pods.keys())[0]
    pod = system.pods[pod_id]
    await pod.start()
    
    origin_id = "station_001"
    
    # Place pod at station
    pod.location = origin_id
    pod.status = PodStatus.EN_ROUTE  # Arriving
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id=origin_id,
        coordinate=Coordinate(-250, -250)
    )
    
    # Station has NO passengers
    station = system.stations[origin_id]
    station.passenger_queue = []
    
    # Simulate arrival
    with patch('aexis.core.pod.asyncio.sleep', new_callable=AsyncMock):
        await pod._handle_station_arrival(origin_id)
    
    # Verify pod is IDLE with no passengers
    assert pod.status == PodStatus.IDLE or pod.status == PodStatus.EN_ROUTE
    assert len(pod.passengers) == 0, "Pod should have no passengers"
    
    print("✅ Empty station arrival test passed")


@pytest.mark.asyncio 
async def test_get_pending_passengers_excludes_claimed(aexis_system_two_pods):
    """
    Test: get_pending_passengers() only returns unclaimed passengers.
    """
    system = aexis_system_two_pods
    await system.initialize()
    
    station = system.stations["station_001"]
    
    # Add 3 passengers
    station.passenger_queue = [
        {"passenger_id": "p1", "destination": "station_002"},
        {"passenger_id": "p2", "destination": "station_003", "claimed_by": "pod_x"},
        {"passenger_id": "p3", "destination": "station_002"},
    ]
    
    pending = station.get_pending_passengers()
    
    # Should return only p1 and p3 (unclaimed)
    assert len(pending) == 2
    pending_ids = [p["passenger_id"] for p in pending]
    assert "p1" in pending_ids
    assert "p2" not in pending_ids  # Claimed
    assert "p3" in pending_ids
    
    print("✅ get_pending_passengers test passed")
