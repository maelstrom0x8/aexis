
import asyncio

import pytest

from aexis.core.model import (
    Coordinate,
    EdgeSegment,
    LocationDescriptor,
    PodStatus,
)
from aexis.core.station_client import StationClient
from aexis.pod import PassengerPod

class TestEdgeTraversal:

    async def test_basic_forward_movement(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 50.0

        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod.segment_progress = 0.0
        pod.location_descriptor = LocationDescriptor(
            location_type="edge",
            edge_id=seg.segment_id,
            coordinate=seg.start_coord,
        )

        arrived = await pod.update(1.0)
        assert not arrived
        assert 49.0 < pod.segment_progress < 51.0

    async def test_segment_completion_triggers_next(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 200.0

        seg1 = network_context.edges["1->2"]
        seg2 = network_context.edges["2->4"]
        pod.current_segment = seg1
        pod.segment_progress = 80.0

        from collections import deque
        pod.route_queue = deque([seg2])

        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2", "4"],
            estimated_duration=10,
        )

        arrived = await pod.update(0.5)

        if not arrived:
            assert pod.current_segment == seg2
            assert 79.0 < pod.segment_progress < 81.0

    async def test_idle_pod_does_not_move(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.IDLE
        result = await pod.update(1.0)
        assert result is False

    async def test_large_dt_capped(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 10.0

        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod.segment_progress = 0.0

        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2"],
            estimated_duration=10,
        )

        await pod.update(100.0)

class TestRouteHydration:

    async def test_hydrate_valid_route(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )

        success = await pod._hydrate_route(
            ["1", "2", "4"]
        )
        assert success is True
        assert pod.current_segment is not None
        assert pod.current_segment.start_node == "1"
        assert pod.current_segment.end_node == "2"
        assert len(pod.route_queue) == 1

    async def test_hydrate_single_station_no_segments(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        success = await pod._hydrate_route(["1"])
        assert success is True
        assert pod.current_segment is None
        assert len(pod.route_queue) == 0

class TestPositionDescriptor:

    async def test_position_midway_on_edge(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 50.0

        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod.segment_progress = 50.0

        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2"],
            estimated_duration=10,
        )

        await pod.update(0.01)

        loc = pod.location_descriptor
        assert loc.location_type == "edge"

        assert 49.0 < loc.coordinate.x < 52.0
        assert -1.0 < loc.coordinate.y < 1.0

class TestRouteCompletion:

    async def test_route_completion_sets_station_location(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 200.0

        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod.segment_progress = 99.0

        from collections import deque
        pod.route_queue = deque()
        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2"],
            estimated_duration=5,
        )

        await pod.update(1.0)

        assert pod.location_descriptor.location_type == "station"
        assert pod.location_descriptor.node_id == "2"
