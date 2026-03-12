import asyncio
import logging
import pytest
import pytest_asyncio
import json
import os
from pathlib import Path
from datetime import datetime, UTC
from aexis.core.system import AexisSystem, SystemContext, AexisConfig
from aexis.core.pod import PodStatus, PodType, LocationDescriptor
from aexis.core.network import NetworkContext
from aexis.core.model import (
    PassengerArrival, 
    CargoRequest, 
    PodArrival, 
    Coordinate, 
    AssignRoute,
    Priority
)
from aexis.core.message_bus import MessageBus
from unittest.mock import patch

logger = logging.getLogger(__name__)

# --- Integration Test Configuration ---
@pytest_asyncio.fixture(scope="function")
async def system_instance():
    """Initialize full Aexis system with real config and no mocks."""
    
    # Mock sleep in pod module for faster tests
    with patch("aexis.core.pod.asyncio.sleep", return_value=None):
        # Reset Singletons
        NetworkContext._instance = None
        SystemContext._instance = None
        
        # Path to real network data
        base_dir = Path(__file__).resolve().parent.parent
        network_path = base_dir / "network.json"
        
        # Create temp config for isolation
        temp_config = "/tmp/aexis_lifecycle_test_config.json"
        config_data = {
            "config": {
                "debug": False,
                "networkDataPath": str(network_path),
                "redis": {
                    "url": "local://"
                },
                "ai": {
                    "provider": "mock"
                },
                "pods": {
                    "count": 4,
                    "cargoRatio": 0.5,
                    "cargoPercentage": 50
                },
                "stations": {
                    "count": 21
                },
                "system": {
                    "snapshotInterval": 300,
                    "decisionInterval": 30,
                    "monitoringInterval": 60
                }
            }
        }
        
        with open(temp_config, "w") as f:
            json.dump(config_data, f)
            
        system_context = SystemContext.initialize_sync(config_path=temp_config)
        aexis_system = AexisSystem(system_context=system_context)
        
        # Initialize system components
        await aexis_system.initialize()
        
        # Start message bus listening in background
        aexis_system.running = True
        asyncio.create_task(aexis_system.message_bus.start_listening())
        
        # Start all stations and pods
        for station in aexis_system.stations.values():
            await station.start()
        for pod in aexis_system.pods.values():
            await pod.start()
            
        # Setup subscriptions for reactive behavior (normally done in system.start())
        await aexis_system._setup_subscriptions()
            
        # Ensure NetworkContext singleton is set correctly for all components
        NetworkContext.set_instance(aexis_system.network_context)
        
        yield aexis_system
        
        # Cleanup
        await aexis_system.shutdown()
        if os.path.exists(temp_config):
            os.remove(temp_config)

@pytest.mark.asyncio
async def test_passenger_pickup_lifecycle(system_instance):
    """
    Verify full lifecycle of passenger pickup and delivery.
    """
    origin = "station_001"
    destination = "station_017"
    passenger_id = "p_lifecycle_001"
    
    # 1. Find a passenger pod
    pod = next(p for p in system_instance.pods.values() if p.pod_type == PodType.PASSENGER)
    
    # 2. Place pod at origin
    station_obj = system_instance.stations[origin]
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id=origin,
        coordinate=Coordinate(station_obj.coordinate.get('x'), station_obj.coordinate.get('y'))
    )
    pod.status = PodStatus.IDLE
    pod.current_segment = None
    pod.route_queue.clear()
    pod.passengers.clear()
    
    # 3. Inject Passenger Arrival
    event = PassengerArrival(
        passenger_id=passenger_id,
        station_id=origin,
        destination=destination,
        priority=Priority.NORMAL.value
    )
    await system_instance.message_bus.publish_event(MessageBus.CHANNELS["PASSENGER_EVENTS"], event)
    
    # 4. Wait for System to process and Pod to pickup
    # Since sleep is mocked, this happens very fast.
    max_retries = 100
    success = False
    for _ in range(max_retries):
        await system_instance._simulate_pod_movement_once(1.0)
        if any(p["passenger_id"] == passenger_id for p in pod.passengers):
            success = True
            break
        await asyncio.sleep(0.01)
        
    assert success, f"Pod {pod.pod_id} failed to pick up passenger {passenger_id}. Status: {pod.status}"
    
    # Wait for Station to process PickedUp event
    station = system_instance.stations[origin]
    for _ in range(100):
        if len(station.passenger_queue) == 0:
            break
        await asyncio.sleep(0.01)

    assert len(station.passenger_queue) == 0, f"Station queue should be empty after pickup. Current: {[p['passenger_id'] for p in station.passenger_queue]}"

    # 6. Delivery
    # Assign route to destination
    route = ["station_001", "station_017"]
    command = AssignRoute(target=pod.pod_id, route=route)
    await system_instance.message_bus.publish_command(MessageBus.CHANNELS["POD_COMMANDS"], command)
    await asyncio.sleep(0.2) # Process command
    
    # Simulate movement
    arrived = False
    for _ in range(500):
        await system_instance._simulate_pod_movement_once(1.0)
        if pod.location_descriptor.node_id == destination and pod.status == PodStatus.IDLE:
            arrived = True
            break
        await asyncio.sleep(0.005)
        
    assert arrived, "Pod failed to reach delivery destination"
    assert not any(p["passenger_id"] == passenger_id for p in pod.passengers), "Passenger should be delivered (unloaded)"

@pytest.mark.asyncio
async def test_cargo_weight_limit_lifecycle(system_instance):
    """
    Verify cargo weight limits and rejection.
    """
    origin = "station_001"
    destination = "station_002"
    
    # 1. Find a cargo pod
    pod = next(p for p in system_instance.pods.values() if p.pod_type == PodType.CARGO)
    pod.max_weight = 500.0 # Standard
    
    # 2. Place pod at origin
    station_obj = system_instance.stations[origin]
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id=origin,
        coordinate=Coordinate(station_obj.coordinate.get('x'), station_obj.coordinate.get('y'))
    )
    pod.status = PodStatus.IDLE
    pod.current_segment = None
    pod.route_queue.clear()
    pod.cargo.clear()
    pod.current_weight = 0.0
    
    # 3. Inject HUGE Cargo Request (Exceeds capacity)
    cargo_id = "c_heavy_001"
    request = CargoRequest(
        request_id=cargo_id,
        origin=origin,
        destination=destination,
        weight=600.0 # OVER LIMIT
    )
    await system_instance.message_bus.publish_event(MessageBus.CHANNELS["CARGO_EVENTS"], request)
    await asyncio.sleep(0.5)
    
    # 4. Simulate arrival processing
    # Pod should NOT pick it up
    for _ in range(10):
        await system_instance._simulate_pod_movement_once(1.0)
        
    assert not any(c["request_id"] == cargo_id for c in pod.cargo), "Pod should NOT have loaded overweight cargo"
    assert len(system_instance.stations[origin].cargo_queue) == 1, "Cargo should remain in station queue"

@pytest.mark.asyncio
async def test_multi_stop_payload_persistence(system_instance):
    """
    Verify that payloads persist through intermediate stops.
    """
    origin = "station_001"
    intermediate = "station_002"
    final = "station_003"
    passenger_id = "p_long_001"
    
    pod = next(p for p in system_instance.pods.values() if p.pod_type == PodType.PASSENGER)
    
    # Setup: Pod at origin with passenger already loaded
    pod.location_descriptor.node_id = origin
    pod.passengers.clear()
    pod.passengers.append({"passenger_id": passenger_id, "destination": final})
    pod.status = PodStatus.IDLE
    pod.current_segment = None
    pod.route_queue.clear()
    
    # Route: 001 -> 002 -> 003
    route = ["station_001", "station_002", "station_003"]
    command = AssignRoute(target=pod.pod_id, route=route)
    await system_instance.message_bus.publish_command(MessageBus.CHANNELS["POD_COMMANDS"], command)
    await asyncio.sleep(0.1)
    
    # Move to intermediate 002
    arrived_intermediate = False
    for _ in range(200):
        await system_instance._simulate_pod_movement_once(1.0)
        if pod.location_descriptor.node_id == intermediate:
            arrived_intermediate = True
            # Check persistence
            assert any(p["passenger_id"] == passenger_id for p in pod.passengers), "Passenger should still be on board at intermediate stop"
            # Pod should remain EN_ROUTE because it hasn't reached final destination
            assert pod.status == PodStatus.EN_ROUTE or pod.segment_progress > 0 or pod.route_queue
            break
            
    assert arrived_intermediate, "Failed to reach intermediate stop"
    
    # Move to final 003
    arrived_final = False
    for _ in range(400):
        await system_instance._simulate_pod_movement_once(1.0)
        if pod.location_descriptor.node_id == final and pod.status == PodStatus.IDLE:
            arrived_final = True
            break
            
    assert arrived_final, "Failed to reach final destination"
    assert not any(p["passenger_id"] == passenger_id for p in pod.passengers), "Passenger should be delivered at final destination"
