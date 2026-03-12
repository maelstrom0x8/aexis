"""
Adversarial Edge Case Integration Tests for AEXIS System

"Treat the spec as a claim to be falsified." - Hostile Test Engineer

Tests verify system robustness against:
- High concurrency race conditions (Claim Spam)
- Invalid state transitions (Ghost Stations)
- Resource exhaustion/boundary violations (Capacity Overflow)
- Data corruption/inconsistency (Zombie Passengers)
- Temporal anomalies (Time Travel)
"""

import asyncio
import json
import pytest
import logging
from pathlib import Path
from datetime import datetime, UTC, timedelta
from unittest.mock import MagicMock, AsyncMock, patch
import random

from aexis.core.system import AexisSystem, SystemContext, AexisConfig
from aexis.core.pod import PassengerPod, PodStatus, LocationDescriptor
from aexis.core.network import NetworkContext
from aexis.core.model import Coordinate, PassengerArrival
from aexis.core.message_bus import LocalMessageBus, MessageBus


# Configure logging to capture failures clearly
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def local_message_bus():
    return LocalMessageBus()


@pytest.fixture
def network_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "network.json"


@pytest.fixture
def aexis_system_adversarial(local_message_bus, network_path):
    """System with configurable pod count for stress testing"""
    config = AexisConfig(
        debug=True,
        network_data_path=str(network_path),
        pods={"count": 5, "cargoPercentage": 0.0}, # Start with 5 pods
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
async def test_claim_race_condition_spam(aexis_system_adversarial):
    """
    Adversarial Test: Claim Spam
    
    Scenario:
    1. Single passenger at station.
    2. 50 concurrent claim requests from simulated pods.
    3. Verify EXACTLY ONE claim succeeds.
    4. Verify internal state is consistent (no negative queues, no double booking).
    """
    system = aexis_system_adversarial
    await system.initialize()
    
    station = system.stations["station_001"]
    passenger_id = "victim_p1"
    
    # Inject one passenger
    station.passenger_queue.append({
        "passenger_id": passenger_id,
        "destination": "station_005",
        "arrival_time": datetime.now(UTC)
    })
    
    # Create 50 simulated pods trying to claim
    claimers = [f"bot_pod_{i}" for i in range(50)]
    
    async def try_claim(pod_id):
        # Add random jitter to simulate network reality, but keeps it tight to stress race
        await asyncio.sleep(random.uniform(0.001, 0.005))
        return station.claim_passenger(passenger_id, pod_id)
        
    # Execute all claims concurrently
    results = await asyncio.gather(*(try_claim(pid) for pid in claimers))
    
    success_count = sum(1 for r in results if r)
    
    assert success_count == 1, f"Expected exactly 1 successful claim, got {success_count}. Race condition detected!"
    assert station.passenger_queue[0]["claimed_by"] is not None
    
    # Double check: ensure the winner is properly recorded
    winner_idx = results.index(True)
    winner_pod = claimers[winner_idx]
    assert station.passenger_queue[0]["claimed_by"] == winner_pod
    
    print(f"✅ Claim Race Condition passed: {success_count} winner out of {len(claimers)} attempts")


@pytest.mark.asyncio
async def test_ghost_station_arrival(aexis_system_adversarial):
    """
    Adversarial Test: Ghost Station Arrival
    
    Scenario:
    1. Pod forced to arrive at non-existent station ID "station_666".
    2. System should handle gracefully (log error, not crash).
    3. Pod state should remain valid (or transition to SAFE state).
    """
    system = aexis_system_adversarial
    await system.initialize()
    
    pod = list(system.pods.values())[0]
    await pod.start()
    
    ghost_station_id = "station_666"
    
    # Trap exceptions
    try:
        await pod._handle_station_arrival(ghost_station_id)
    except Exception as e:
        pytest.fail(f"System crashed on ghost station arrival: {e}")
        
    # Verify pod didn't explode
    assert pod.status in [PodStatus.IDLE, PodStatus.EN_ROUTE], "Pod status should be valid"
    # Ideally should handle it by logging and staying IDLE
    
    print("✅ Ghost Station Arrival passed (no crash)")


@pytest.mark.asyncio
async def test_capacity_overflow_attempt(aexis_system_adversarial):
    """
    Adversarial Test: Capacity Overflow
    
    Scenario:
    1. Pod has capacity 4.
    2. Station has 10 passengers.
    3. Pod attempts to load.
    4. Verify pod DOES NOT exceed capacity.
    """
    system = aexis_system_adversarial
    await system.initialize()
    
    pod_id = list(system.pods.keys())[0]
    pod = system.pods[pod_id]
    pod.capacity = 4 # Force low capacity
    
    station = system.stations["station_001"]
    
    # Inject 10 passengers
    for i in range(10):
        station.passenger_queue.append({
            "passenger_id": f"sardine_{i}",
            "destination": "station_002"
        })
        
    await pod._execute_pickup("station_001")
    
    assert len(pod.passengers) <= 4, f"Pod overflowed! Capacity 4, loaded {len(pod.passengers)}"
    assert len(pod.passengers) == 4, "Pod should have filled to capacity"
    
    print("✅ Capacity Overflow passed")


@pytest.mark.asyncio
async def test_zombie_passenger_consistency(aexis_system_adversarial):
    """
    Adversarial Test: Zombie Passenger
    
    Scenario:
    1. Passenger is in Pod (status: Picked Up).
    2. Same Passenger ID appears in Station Queue (status: Waiting).
    3. System should identify duplication or handle it without crashing.
    4. Pod should NOT pick up the 'ghost' duplicate.
    """
    system = aexis_system_adversarial
    await system.initialize()
    
    pod = list(system.pods.values())[0]
    station = system.stations["station_001"]
    
    pid = "schrodinger_p1"
    
    # 1. Put passenger in Pod
    pod.passengers.append({"passenger_id": pid, "destination": "station_002"})
    
    # 2. Put 'duplicate' in Station
    station.passenger_queue.append({"passenger_id": pid, "destination": "station_002"})
    
    # 3. Pod arrives at station
    await pod._execute_pickup("station_001")
    
    # 4. Verify Pod didn't pick up the duplicate (it shouldn't pick up ANY if it has the ID already?)
    # Actually, simplistic logic might just append. We want to see if it doubles up.
    
    passengers_with_id = [p for p in pod.passengers if p["passenger_id"] == pid]
    
    # If logic is robust, it prevents duplicate IDs in manifest.
    # If not, this test REVEALS the flaw.
    if len(passengers_with_id) > 1:
         pytest.warns(UserWarning, match="Duplicate passenger loaded! Zombie apocalypse initiated.")
    
    # We verify the count. 
    # Strict expectation: System should NOT have duplicates.
    assert len(passengers_with_id) == 1, f"Pod loaded duplicate/zombie passenger! {passengers_with_id}"

    print("✅ Zombie Passenger test passed")


@pytest.mark.asyncio
async def test_time_travel_events(aexis_system_adversarial):
    """
    Adversarial Test: Time Travel
    
    Scenario:
    1. Passenger arrives with timestamp 100 years in future.
    2. Passenger arrives with timestamp 100 years in past.
    3. System processing should not hang or crash on wait_time calculations.
    """
    system = aexis_system_adversarial
    await system.initialize()
    station = system.stations["station_001"]
    
    future_time = datetime.now(UTC) + timedelta(days=36500) # +100 years
    past_time = datetime.now(UTC) - timedelta(days=36500)   # -100 years
    
    # Inject problematic passengers
    station.passenger_queue.append({
        "passenger_id": "future_boy",
        "arrival_time": future_time
    })
    station.passenger_queue.append({
        "passenger_id": "ancient_one",
        "arrival_time": past_time 
    })
    
    # Trigger metric updates which usually use timestamps
    try:
        station._update_wait_time_metrics()
    except Exception as e:
        pytest.fail(f"Metric calculation crashed on time travel: {e}")
        
    # Check metrics are somewhat sane (not Infinity or NaN ideally, but definitely not crashed)
    # This is a loose assertion, mainly checking for crashes.
    
    print("✅ Time Travel test passed")

