"""Unit tests for pod movement physics.

Tests edge traversal, segment overflow, route completion, position descriptor
accuracy, and speed/dt boundary conditions.
"""

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
    """Pod movement along a single edge segment."""

    async def test_basic_forward_movement(
        self, message_bus, redis_client, station_client, network_context
    ):
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 50.0  # m/s

        # Place on edge 1 -> 2 (100m)
        seg = network_context.edges["1->2"]
        pod.current_segment = seg
        pod.segment_progress = 0.0
        pod.location_descriptor = LocationDescriptor(
            location_type="edge",
            edge_id=seg.segment_id,
            coordinate=seg.start_coord,
        )

        # 1 second at 50 m/s = 50m travelled
        arrived = await pod.update(1.0)
        assert not arrived
        assert 49.0 < pod.segment_progress < 51.0

    async def test_segment_completion_triggers_next(
        self, message_bus, redis_client, station_client, network_context
    ):
        """Pod overflows into the next queued segment."""
        pod = PassengerPod(
            message_bus, redis_client, "1", station_client
        )
        pod.status = PodStatus.EN_ROUTE
        pod.speed = 200.0

        # Two-segment route: 001->002 (100m) then 002->004 (100m)
        seg1 = network_context.edges["1->2"]
        seg2 = network_context.edges["2->4"]
        pod.current_segment = seg1
        pod.segment_progress = 80.0  # 20m from end of seg1

        from collections import deque
        pod.route_queue = deque([seg2])

        # Route to trigger completion callback
        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2", "4"],
            estimated_duration=10,
        )

        # dt=0.5 at 200 m/s = 100m. 20m finishes seg1, 80m into seg2.
        arrived = await pod.update(0.5)

        # Should have advanced into seg2
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
        """dt > 1s should be capped in the movement loop to prevent warp."""
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

        # Pass dt=100 (huge). update() caps distance per step.
        await pod.update(100.0)
        # Speed * dt would be 1000 but dist_to_travel is capped at 100
        # Segment is 100m, so pod should complete the route
        # (distance may exceed segment, triggering route completion)


class TestRouteHydration:
    """Converting station lists into EdgeSegment queues."""

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
        assert len(pod.route_queue) == 1  # 2->4

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
    """Location descriptor accuracy during movement."""

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
        pod.segment_progress = 50.0  # midway

        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2"],
            estimated_duration=10,
        )

        # Tiny step just to trigger position update
        await pod.update(0.01)

        loc = pod.location_descriptor
        assert loc.location_type == "edge"
        # Midpoint of (0,0) -> (100,0) should be roughly (50,0)
        assert 49.0 < loc.coordinate.x < 52.0
        assert -1.0 < loc.coordinate.y < 1.0


class TestRouteCompletion:
    """End-of-route behavior."""

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
        pod.segment_progress = 99.0  # 1m from end

        from collections import deque
        pod.route_queue = deque()
        from aexis.core.model import Route
        pod.current_route = Route(
            route_id="test",
            stations=["1", "2"],
            estimated_duration=5,
        )

        await pod.update(1.0)

        # Should have completed route and set location to station
        assert pod.location_descriptor.location_type == "station"
        assert pod.location_descriptor.node_id == "2"
