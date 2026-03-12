import sys
from unittest.mock import MagicMock

# Mock redis before importing modules that need it
sys.modules['redis'] = MagicMock()
sys.modules['redis.asyncio'] = MagicMock()

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from aexis.core.pod import Pod, PodStatus
from aexis.core.model import Coordinate, EdgeSegment, Route, LocationDescriptor, PodPositionUpdate

# Test Helpers
class MockNetworkContext:
    def __init__(self):
        self.edges = {}
        self.station_positions = {
            "s1": (0, 0),
            "s2": (10, 0),
            "s3": (20, 0)
        }
    
    @classmethod
    def get_instance(cls):
        return cls._instance

class MockPod(Pod):
    def _get_pod_type(self):
        from aexis.core.pod import PodType
        return PodType.PASSENGER
        
    async def _build_decision_context(self):
        return {}

@pytest.fixture
def mock_network(mocker):
    network = MockNetworkContext()
    MockNetworkContext._instance = network
    
    # Create edges: s1->s2 (10m), s2->s3 (10m)
    e1 = EdgeSegment(
        segment_id="s1->s2", start_node="s1", end_node="s2",
        start_coord=Coordinate(0,0), end_coord=Coordinate(10,0)
    )
    e2 = EdgeSegment(
        segment_id="s2->s3", start_node="s2", end_node="s3",
        start_coord=Coordinate(10,0), end_coord=Coordinate(20,0)
    )
    network.edges = {
        "s1->s2": e1,
        "s2->s3": e2
    }
    
    # Patch the NetworkContext.get_instance where it is defined
    mocker.patch('aexis.core.network.NetworkContext.get_instance', return_value=network)
    return network

@pytest.fixture
def mock_bus(mocker):
    bus = MagicMock()
    # Mock publish_event as AsyncMock since it's awaited
    bus.publish_event = AsyncMock()
    return bus

@pytest.fixture
def pod(mock_bus):
    pod = MockPod(mock_bus, "pod_test")
    pod._get_capacity_status = MagicMock(return_value=(0,0,0,0))
    pod.speed = 20.0
    return pod

@pytest.mark.asyncio
async def test_movement_overflow_event_publication(pod, mock_network, mock_bus):
    """
    Test that:
    1. Pod transitions edges seamlessly (Overflow logic).
    2. PodPositionUpdate event is correctly published exactly once.
    """
    
    # Hydrate route s1 -> s2 -> s3
    await pod._hydrate_route(["s1", "s2", "s3"])
    pod.status = PodStatus.EN_ROUTE

    # Execute update(0.75) -> 15m distance
    # Should travel full 10m of s1->s2, and 5m of s2->s3
    await pod.update(0.75)
    
    # Checks internal state (Physics verification)
    assert pod.current_segment.segment_id == "s2->s3"
    assert pod.segment_progress == 5.0
    assert pod.location_descriptor.edge_id == "s2->s3"
    assert pod.location_descriptor.coordinate.x == 15.0
    
    # Checks Event Publication (Behavior verification with pytest-mock)
    # We assert that the bus.publish_event was awaited exactly once
    assert mock_bus.publish_event.await_count == 1
    
    # Get the arguments of the call
    # call_args[0] are positional args -> (channel_name, event_object)
    call_args = mock_bus.publish_event.await_args
    event_obj = call_args[0][1]
    
    # Assert Event Payload
    assert isinstance(event_obj, PodPositionUpdate)
    assert event_obj.pod_id == "pod_test"
    assert event_obj.location.edge_id == "s2->s3"
    assert event_obj.location.coordinate.x == 15.0
    
    print("\n✅ Verification Successful: Event published with correct coordinates.")
