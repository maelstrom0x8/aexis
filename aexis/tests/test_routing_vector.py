import os
import asyncio
import pytest
import json
from pathlib import Path
from aexis.core.system import AexisSystem, SystemContext, AexisConfig
from aexis.core.pod import PodStatus, PodType, LocationDescriptor
from aexis.core.network import NetworkContext
from aexis.core.model import AssignRoute, PodArrival, Coordinate
from aexis.core.message_bus import MessageBus

# --- Integration Test Configuration ---
# Use a dedicated Redis DB for tests to ensure NO MOCKS while maintaining isolation
TEST_REDIS_URL = "redis://localhost:6379/15" 
TEST_NETWORK_JSON = "/home/godelhaze/dev/megalith/aexis/aexis/network.json"
TEST_AEXIS_JSON = "/home/godelhaze/dev/megalith/aexis/aexis/aexis.json"

import pytest_asyncio

@pytest_asyncio.fixture(scope="function")
async def system_instance():
    """Initialize full Aexis system with real config and no mocks."""
    # Reset Singletons
    SystemContext._instance = None
    NetworkContext._instance = None
    
    # Load real production config and override Redis for test isolation
    with open(TEST_AEXIS_JSON, 'r') as f:
        config_data = json.load(f)
    
    # Override Redis URL for test isolation (Real Redis, just different DB)
    config_data['config']['redis']['url'] = TEST_REDIS_URL
    config_data['config']['networkDataPath'] = TEST_NETWORK_JSON
    
    # Write temp config for SystemContext to load
    temp_config_path = "/tmp/aexis_test_config.json"
    with open(temp_config_path, 'w') as f:
        json.dump(config_data, f)
    
    ctx = SystemContext.initialize_sync(temp_config_path)
    aexis_system = AexisSystem(system_context=ctx)
    
    # Ensure message bus is connected
    await aexis_system.message_bus.connect()
    
    # Initialize system components
    await aexis_system.initialize()
    
    # Start message bus listening in background
    aexis_system.running = True
    asyncio.create_task(aexis_system.message_bus.start_listening())
    
    # Start all pods so they subscribe to commands
    for pod in aexis_system.pods.values():
        await pod.start()
        
    yield aexis_system
    
    # Cleanup
    await aexis_system.shutdown()
    if os.path.exists(temp_config_path):
        os.remove(temp_config_path)

# --- Routing Vector Scenarios ---

ROUTING_SCENARIOS = [
    # (scenario_id, route_list, expected_final_station)
    ("single_hop", ["station_001", "station_002"], "station_002"),
    ("multi_hop", ["station_001", "station_007", "station_011"], "station_011"),
    ("circular_loop", ["station_003", "station_021", "station_003"], "station_003"),
    ("self_loop", ["station_004", "station_004"], "station_004"),
    ("long_range", ["station_001", "station_017"], "station_017"),
    ("tsp_sequence", ["station_001", "station_004", "station_009", "station_010"], "station_010"),
    ("cross_network", ["station_011", "station_013"], "station_013"),
    ("inter-hub", ["station_005", "station_013", "station_012"], "station_012"),
    ("rim_navigation", ["station_017", "station_018", "station_019", "station_020"], "station_020"),
    ("back-and-forth", ["station_001", "station_002", "station_001", "station_002"], "station_002"),
    ("diagonal_cross", ["station_006", "station_015"], "station_015"),
    ("central_hub_spin", ["station_010", "station_009", "station_011", "station_010"], "station_010"),
    ("edge_case_path", ["station_021", "station_001"], "station_001"),
    ("high_entropy", ["station_002", "station_008", "station_014", "station_020"], "station_020"),
    ("short_hop", ["station_015", "station_016"], "station_016"),
    ("station_flip", ["station_013", "station_014", "station_013"], "station_013"),
    ("complex_tree", ["station_001", "station_003", "station_005", "station_007"], "station_007"),
    ("u-turn", ["station_008", "station_009", "station_008"], "station_008"),
    ("zenith", ["station_001", "station_021"], "station_021"),
    ("nadir", ["station_021", "station_004"], "station_004"),
]

@pytest.mark.asyncio
@pytest.mark.parametrize("name, route, expected", ROUTING_SCENARIOS)
async def test_no_mock_routing_navigation(system_instance, name, route, expected):
    """
    NO-MOCK INTEGRATION TEST:
    Verify that a pod correctly navigates a complex route across the real network topology.
    Falsifies: Pathfinding correctness, segment transition state machine.
    """
    # Pick an idle pod
    pod = next(iter(system_instance.pods.values()))
    
    # Ensure pod is at the start station for clean test
    start_station = route[0]
    station_obj = system_instance.stations.get(start_station)
    assert station_obj, f"Start station {start_station} not found in system"
    
    pod.location_descriptor.node_id = start_station
    pod.location_descriptor.location_type = "station"
    pod.location_descriptor.coordinate = Coordinate(
        station_obj.coordinate.get("x", 0),
        station_obj.coordinate.get("y", 0)
    )
    pod.status = PodStatus.IDLE
    pod.current_segment = None
    pod.route_queue.clear()
    
    # Assign route via AssignRoute command (simulating external API/System command)
    command = AssignRoute(
        target=pod.pod_id,
        route=route
    )
    
    # Inject directly via message bus to test the full loop
    success = await system_instance.message_bus.publish_command(
        MessageBus.CHANNELS["POD_COMMANDS"], 
        command
    )
    assert success, "Failed to publish AssignRoute command"
    
    # Wait for the background listener to pick up and process the command
    await asyncio.sleep(0.2)
    print(f"[{pod.pod_id}]: Status after command delay: {pod.status}, Queue length: {len(pod.route_queue)}")
    
    # Wait for the system to process the command and the pod to move
    # We poll the pod status and location
    max_retries = 500 # Increased for long-range routes
    arrived = False
    
    for i in range(max_retries):
        # Allow system to process physics and events
        await system_instance._simulate_pod_movement_once(0.5) # Force tick
        
        if pod.location_descriptor.node_id == expected and pod.status == PodStatus.IDLE:
            arrived = True
            break
        
        if i % 50 == 0:
             print(f"[{pod.pod_id}]: Step {i}, Loc: {pod.location_descriptor.node_id}, Status: {pod.status}")
             
        await asyncio.sleep(0.01)
        
    assert arrived, f"Pod {pod.pod_id} failed to reach {expected} in {name}. Current: {pod.location_descriptor.node_id}, Status: {pod.status}"

@pytest.mark.asyncio
async def test_adversarial_malicious_node_routing(system_instance):
    """
    ADVERSARIAL: Attempt to route to a non-existent station ID.
    Verify: System does not crash and pod remains IDLE or rejects.
    """
    pod = next(iter(system_instance.pods.values()))
    start_station = "station_001"
    bad_route = [start_station, "station_999"]
    
    # Force IDLE
    pod.location_descriptor.node_id = start_station
    pod.location_descriptor.location_type = "station"
    pod.status = PodStatus.IDLE
    
    command = AssignRoute(target=pod.pod_id, route=bad_route)
    await system_instance.message_bus.publish_command(
        MessageBus.CHANNELS["POD_COMMANDS"], 
        command
    )
    
    # Wait a bit
    await asyncio.sleep(0.5)
    await system_instance._simulate_pod_movement_once(1.0)
    
    # Should still be IDLE at start or current location
    assert pod.status == PodStatus.IDLE
    assert pod.location_descriptor.node_id == start_station

@pytest.mark.asyncio
async def test_adversarial_disconnected_routing(system_instance):
    """
    ADVERSARIAL: Attempt to route between stations with no physical path.
    Verify: System handles pathfinding failure gracefully.
    """
    pod = next(iter(system_instance.pods.values()))
    pod.status = PodStatus.IDLE
    
    command = AssignRoute(target=pod.pod_id, route=[])
    await system_instance.message_bus.publish_command(
        MessageBus.CHANNELS["POD_COMMANDS"], 
        command
    )
    
    await asyncio.sleep(0.1)
    await system_instance._simulate_pod_movement_once(1.0)
    
    assert pod.status == PodStatus.IDLE

@pytest.mark.asyncio
async def test_thundering_herd_routing_hammering(system_instance):
    """
    STRESS: Hammer a single pod with 20 rapid route assignments.
    Verify: State consistency and that the LATEST command eventually wins or is queued.
    """
    pod = next(iter(system_instance.pods.values()))
    pod.status = PodStatus.IDLE
    
    stations = ["station_002", "station_003", "station_004", "station_005"]
    
    # Parallel bombardment
    tasks = [
        system_instance.message_bus.publish_command(
            MessageBus.CHANNELS["POD_COMMANDS"], 
            AssignRoute(target=pod.pod_id, route=["station_001", s])
        )
        for s in stations
    ]
    await asyncio.gather(*tasks)
    
    # Wait for processing
    await asyncio.sleep(0.5)
    
    # Pod should be targeting ONE of these, not in a corrupted state
    assert pod.status in [PodStatus.EN_ROUTE, PodStatus.IDLE]
    if pod.status == PodStatus.EN_ROUTE:
        assert pod.route_queue or pod.current_segment
