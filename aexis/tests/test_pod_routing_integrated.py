import sys
import os
import asyncio
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
def aexis_system(local_message_bus, network_path, mocker):
    # Setup real configuration but override specific values for the test
    # AexisConfig.get expects nested structure for dots
    config = AexisConfig(
        debug=True,
        network_data_path=str(network_path),
        pods={"count": 1, "cargoPercentage": 0.0},
        stations={"count": 21},
        ai={"provider": "none"},
        redis={"url": "local://"}
    )
    
    # Mock SystemContext to return our test-specific config and real network context
    mock_ctx = MagicMock(spec=SystemContext)
    mock_ctx.get_config.return_value = config
    
    # Real network context
    NetworkContext._instance = None # Reset singleton
    import json
    with open(str(network_path), 'r') as f:
        network_data = json.load(f)
    real_network = NetworkContext(network_data=network_data)
    NetworkContext._instance = real_network # Essential for components to find it
    mock_ctx.get_network_context.return_value = real_network
    
    # Create system with local message bus
    system = AexisSystem(system_context=mock_ctx, message_bus=local_message_bus)
    
    # Minimize internal system mocks which are not relevant to routing
    system._update_metrics = AsyncMock()
    system._publish_snapshot = AsyncMock()
    system._log_system_status = AsyncMock()
    
    return system

@pytest.mark.asyncio
async def test_pod_routing_and_delivery_lifecycle(aexis_system, local_message_bus):
    """
    Integrated Test: Verify full lifecycle of passenger pod routing using real components.
    - Uses LocalMessageBus for real event propagation.
    - Uses real Pod and Station logic.
    - Minimal mocking (only non-core system side-effects).
    """
    # 1. Initialize
    success = await aexis_system.initialize()
    assert success
    
    # Start event processors for all stations and the pod
    # This is normally done in aexis_system.start()
    for station in aexis_system.stations.values():
        await station.start()
        
    pod_id = list(aexis_system.pods.keys())[0]
    pod = aexis_system.pods[pod_id]
    await pod.start()
    
    # Now that stations have subscribed, subscribe the system
    # This ensures stations process arrival events BEFORE the system triggers decisions
    await aexis_system._setup_subscriptions()
    
    # Manually place pod at Station "station_001" for predictability
    pod.location = "station_001"
    pod.status = PodStatus.IDLE
    pod.current_segment = None
    pod.location_descriptor = LocationDescriptor(
        location_type="station",
        node_id="station_001",
        coordinate=Coordinate(-250, -250)
    )
    
    # Distant stations: Pickup at 017, Deliver to 011
    origin_station_id = "station_017"
    dest_station_id = "station_011"
    passenger_id = "test_p_001"
    
    # 2. Inject Passenger at Station 17 via Message Bus
    # This triggers both Station queue update and System reactive decision
    station_17 = aexis_system.stations[origin_station_id]
    event = PassengerArrival(
        passenger_id=passenger_id,
        station_id=origin_station_id,
        destination=dest_station_id,
        priority=3
    )
    
    # Use LocalMessageBus to publish - this should resolve the user's specific requirement
    # that PassengerArrival triggers navigation.
    await local_message_bus.publish_event(MessageBus.CHANNELS["PASSENGER_EVENTS"], event)
    
    assert len(station_17.passenger_queue) == 1, "Station should have received event and updated queue"
    
    # 3. Decision Making happens reactively in AexisSystem._handle_event
    # Patch sleep to speed up pods
    with patch('aexis.core.pod.asyncio.sleep', new_callable=AsyncMock):
        # We may need a tiny sleep to allow the async event dispatch to complete if needed,
        # but LocalMessageBus is immediate.
        
        # Verify Route Calculation (triggered by the event!)
        assert pod.current_route is not None, "Pod should have reactively calculated a route"
        assert origin_station_id in pod.current_route.stations, "Route should include pickup station"
        assert pod.status == PodStatus.EN_ROUTE
        
        # 4. Simulate Movement to Pickup
        max_ticks = 1000
        ticks = 0
        while ticks < max_ticks:
            desc = pod.location_descriptor
            if desc.location_type == "station" and desc.node_id == origin_station_id:
                break
            # Check proximity to station (robust physics-based check)
            # Station 17: (400, 250)
            dist_sq = (desc.coordinate.x - 400)**2 + (desc.coordinate.y - 250)**2
            if dist_sq < 2500:  # Within 50 units
                break
            
            await pod.update(2.0)
            ticks += 1
            
        # Verify we found it
        reached = (pod.location_descriptor.coordinate.x - 400)**2 + \
                  (pod.location_descriptor.coordinate.y - 250)**2 < 2500
            
        assert reached, f"Pod failed to reach pickup after {ticks} ticks"
        
        # Verify pickup actually happened (wait for loading if needed)
        # Pickup involves sleep, so pod might be LOADING
        timeout = 0
        while len(pod.passengers) == 0 and timeout < 20:
             await pod.update(1.0)
             timeout += 1
             
        assert len(pod.passengers) == 1, f"Pod failed to pick up passenger from {origin_station_id}. Status: {pod.status}, Passengers: {pod.passengers}"
        
        # 5. Verify Pickup (Event-driven)
        # In a real system, the pod arriving triggers _handle_route_completion -> _handle_station_arrival -> pickup
        # LocalMessageBus should have propagated the PassengerPickedUp event from Pod to Station
        # Wait, the Pod publishes PassengerPickedUp, Station listens to it.
        # Let's ensure Station is listening. AexisSystem.initialize() calls _setup_generators() 
        # which might not be enough. Actually, Stations subscribe during their creation?
        
        # Check if station updated its queue via the event
        assert len(station_17.passenger_queue) == 0, "Station queue should be empty after pickup event"
        assert len(pod.passengers) == 1, "Pod should have 1 passenger"
        
        # 6. Simulate Movement to Destination
        # The pod automatically triggers new decision after arrival if idle
        # Reset ticks for second leg
        ticks = 0
        while ticks < max_ticks:
            desc = pod.location_descriptor
            
            # Check proximity to station 
            # Station 11: (-400, 0)
            dist_sq = (desc.coordinate.x + 400)**2 + (desc.coordinate.y)**2
            if dist_sq < 2500:
                break
            
            await pod.update(2.0)
            ticks += 1
            
        # Get final descriptor for check
        desc = pod.location_descriptor
        reached_dest = (desc.coordinate.x + 400)**2 + \
                       (desc.coordinate.y)**2 < 2500
            
        assert reached_dest, f"Pod failed to reach destination after {ticks} ticks"
        
        # Verify unloading happens
        timeout = 0
        while len(pod.passengers) > 0 and timeout < 20:
             await pod.update(1.0)
             timeout += 1

        assert len(pod.passengers) == 0, f"Pod should be empty after delivery. Passengers: {pod.passengers}"
        
    print(f"\n✅ Integrated Pod Routing Test Passed in {ticks} ticks!")
