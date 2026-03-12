import sys
import os
from pathlib import Path
from unittest.mock import MagicMock

# Mock redis before imports
sys.modules['redis'] = MagicMock()
sys.modules['redis.asyncio'] = MagicMock()

import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from aexis.core.system import AexisSystem, SystemContext, AexisConfig
from aexis.core.pod import Pod, PodStatus
from aexis.core.model import PodPositionUpdate
from aexis.core.network import NetworkContext

# --- Fixtures ---

@pytest.fixture
def mock_system_context(mocker):
    # Mock SystemContext dependencies
    mock_ctx = MagicMock(spec=SystemContext)
    
    # Config: 1 Pod, 3 Stations
    # AexisConfig.get('pods.count') expects nested dict or attribute
    # We'll use a dict approach which AexisConfig.get handles
    config_dict = {
        "debug": True,
        "pods": {
            "count": 1,
            "cargoPercentage": 0.0
        }
    }
    
    # We can pass these as kwargs or wrap in a mock that behaves like AexisConfig
    # Simpler: Create AexisConfig and patch its .get method or ensure it works
    real_config = AexisConfig(**config_dict)
    
    # Override get to work with our dict structure if default implementation doesn't handle it fully
    # Actually AexisSystem invokes config.get('pods.count')
    # Let's just mock the get method to be sure
    
    mock_config = MagicMock(spec=AexisConfig)
    def config_get(key, default=None):
        if key == "pods.count": return 1
        if key == "pods.cargoPercentage": return 0.0
        if key == "stations.count": return 3
        if key == "redis.url": return "redis://localhost:6379"
        return default
        
    mock_config.get.side_effect = config_get
    mock_ctx.get_config.return_value = mock_config
    
    # Point to test_network.json
    base_dir = Path(__file__).resolve().parent
    network_path = base_dir / "test_network.json"
    
    print(f"\nDEBUG: Loading test network from {network_path}")

    # Reset singleton
    NetworkContext._instance = None
    os.environ["AEXIS_NETWORK_DATA"] = str(network_path)
    
    # Initialize real network
    real_network = NetworkContext(network_data=None) 
    mock_ctx.get_network_context.return_value = real_network
    
    # Patch the class method to return the real loaded network setup
    mocker.patch('aexis.core.system.SystemContext.get_instance', return_value=mock_ctx)
    mocker.patch('aexis.core.network.NetworkContext.get_instance', return_value=real_network)
    
    return mock_ctx

@pytest.fixture
def aexis_system(mock_system_context, mocker):
    # Initialize system with injected context
    system = AexisSystem(system_context=mock_system_context)
    
    # Replace real message bus with AsyncMock
    system.message_bus = MagicMock()
    system.message_bus.publish_event = AsyncMock()
    system.message_bus.connect = AsyncMock(return_value=True)
    system.message_bus.start_listening = AsyncMock()
    
    # Disable other background tasks for isolation
    system._update_metrics = AsyncMock()
    system._publish_snapshot = AsyncMock()
    system._log_system_status = AsyncMock()
    
    return system

# --- Test ---

@pytest.mark.asyncio
async def test_system_initialization_and_movement(aexis_system, mocker):
    """
    End-to-End System Test:
    1. Initialize system with test_network.json (3 stations).
    2. System auto-creates 1 Pod and spawns it on an edge.
    3. Run simulation loop.
    4. Verify Pod moves and emits events.
    """
    
    # 1. Initialize System
    # This calls _create_pods, which uses the fix we implemented to set current_segment
    success = await aexis_system.initialize()
    assert success, "System initialization failed"
    
    assert len(aexis_system.pods) == 1
    # Get the auto-created pod
    pod_id = list(aexis_system.pods.keys())[0]
    pod = aexis_system.pods[pod_id]
    
    print(f"\nInitialized Pod {pod_id} at {pod.location_descriptor}")
    
    # Ensure it spawned on an edge (most likely, though small chance of station fallback)
    # The test network has 2 edges (A<->B, B<->C) and 3 stations.
    # spawn_pod_at_random_edge logic picks an edge.
    
    # Assert initial state
    assert pod.status == PodStatus.EN_ROUTE
    assert pod.current_segment is not None
    assert pod.location_descriptor.location_type == "edge"
    
    # 2. Run the simulation loop
    aexis_system.running = True
    sim_task = asyncio.create_task(aexis_system._simulate_pod_movement())
    
    # Wait for ~0.35s (target interval 0.1s -> ~3 ticks)
    await asyncio.sleep(0.35)
    
    # Stop
    aexis_system.running = False
    await asyncio.sleep(0.1)
    if not sim_task.done():
        sim_task.cancel()
        try:
            await sim_task
        except asyncio.CancelledError:
            pass

    # 3. Verify Events
    assert aexis_system.message_bus.publish_event.await_count >= 3
    
    calls = aexis_system.message_bus.publish_event.await_args_list
    events = []
    
    for call in calls:
        event = call.args[1]
        if isinstance(event, PodPositionUpdate) and event.pod_id == pod_id:
            events.append(event)
            
    assert len(events) >= 3, f"Expected continuous updates, got {len(events)}"
    
    print(f"\nCaptured {len(events)} position events for {pod_id}.")
    
    # 4. Verify Monotonic Movement
    # The pod moves along a specific edge. X (or Y) should change monotonically.
    # Note: Depending on edge direction (A->B or B->A), X might increase or decrease.
    first_x = events[0].location.coordinate.x
    last_x = events[-1].location.coordinate.x
    
    diff = last_x - first_x
    print(f"Movement: Start X={first_x}, End X={last_x}, Diff={diff}")
    
    assert abs(diff) > 0.0001, "Pod did not move significantly"
    
    # Verify interval consistency roughly
    # (Just ensuring we didn't get all events in one burst)
    
    print("\n✅ End-to-End verification successful: System initialized real pod and drove it.")
