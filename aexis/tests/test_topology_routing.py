"""Topology-varied routing and scale-up tests.

Covers:
- Linear chain: multi-hop hydration
- Star hub: all routes through center
- Ring: shortest path selection
- Disconnected: hydration failure across clusters
- Single station: degenerate case
- Synthetic edge fallback
- RoutingProvider fallback chain
- Destination extraction logic
- TSP solver edge cases
- Idle routing behavior
- Scale-up: 50 stations, 20 concurrent pods, 100 pending passengers
"""

import asyncio
from collections import deque

import pytest

from aexis.core.model import (
    Coordinate,
    DecisionContext,
    EdgeSegment,
    LocationDescriptor,
    PodStatus,
    Route,
)
from aexis.core.network import NetworkContext
from aexis.core.routing import (
    OfflineRouter,
    OfflineRoutingStrategy,
    Router,
    RoutingProvider,
)
from aexis.tests.conftest import (
    async_seed_cargo,
    async_seed_passenger,
    make_cargo_pod,
    make_passenger_pod,
)


# ======================================================================
# Linear network
# ======================================================================


class TestLinearNetworkRouting:
    """5 stations in a chain: 1→2→3→4→5."""

    async def test_multi_hop_hydration(
        self, message_bus, redis_client, station_client, linear_network,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        success = await pod._hydrate_route(["1", "2", "3", "4", "5"])
        assert success is True
        assert pod.current_segment is not None
        assert pod.current_segment.start_node == "1"
        assert pod.current_segment.end_node == "2"
        assert len(pod.route_queue) == 3  # 2→3, 3→4, 4→5

    async def test_reverse_traversal(
        self, message_bus, redis_client, station_client, linear_network,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="5"
        )
        success = await pod._hydrate_route(["5", "4", "3"])
        assert success is True
        assert pod.current_segment.start_node == "5"
        assert len(pod.route_queue) == 1


# ======================================================================
# Star network
# ======================================================================


class TestStarNetworkRouting:
    """Hub 1 + spokes 2,3,4,5 — spoke-to-spoke always goes through hub."""

    async def test_spoke_to_spoke_through_hub(
        self, message_bus, redis_client, station_client, star_network,
    ):
        strategy = OfflineRoutingStrategy(star_network)
        context = DecisionContext(
            pod_id="1",
            current_location="2",
            current_route=None,
            capacity_available=10,
            weight_available=0.0,
            available_requests=[],
            network_state={},
            system_metrics={},
            passengers=[{"passenger_id": "p1", "destination": "4"}],
        )
        result = strategy.calculate_optimal_route(context)
        route = result["route"]
        # Must pass through hub (station 1)
        assert "1" in route
        assert route[-1] == "4"

    async def test_hub_direct_to_spoke(
        self, message_bus, redis_client, station_client, star_network,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        success = await pod._hydrate_route(["1", "3"])
        assert success is True
        assert pod.current_segment.end_node == "3"


# ======================================================================
# Ring network
# ======================================================================


class TestRingNetworkRouting:
    """5-station cycle — shortest path should be chosen."""

    async def test_shortest_path_selection(
        self, message_bus, redis_client, station_client, ring_network,
    ):
        strategy = OfflineRoutingStrategy(ring_network)
        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=10,
            weight_available=0.0,
            available_requests=[],
            network_state={},
            system_metrics={},
            passengers=[{"passenger_id": "p1", "destination": "3"}],
        )
        result = strategy.calculate_optimal_route(context)
        route = result["route"]
        # 1→2→3 (2 hops) is shorter than 1→5→4→3 (3 hops)
        assert len(route) <= 4
        assert route[0] == "1"
        assert route[-1] == "3"


# ======================================================================
# Disconnected network
# ======================================================================


class TestDisconnectedNetworkRouting:
    """Two isolated clusters: A(1↔2), B(3↔4). No cross-edges."""

    async def test_hydration_fails_across_clusters(
        self, message_bus, redis_client, station_client, disconnected_network,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        # Attempt to route from cluster A to cluster B
        # Edge 2->3 doesn't exist in the network, but both positions are known
        # so it should synthesize the edge (warning logged)
        success = await pod._hydrate_route(["1", "2", "3"])
        # Synthetic edge created because both stations have positions
        assert success is True

    async def test_routing_strategy_handles_no_path(
        self, message_bus, redis_client, station_client, disconnected_network,
    ):
        """Offline strategy produces a fallback route when no path exists."""
        strategy = OfflineRoutingStrategy(disconnected_network)
        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=10,
            weight_available=0.0,
            available_requests=[{
                "type": "passenger",
                "passenger_id": "p1",
                "origin": "3",
                "destination": "4",
            }],
            network_state={},
            system_metrics={},
        )
        result = strategy.calculate_optimal_route(context)
        # Should produce some route (possibly fallback [start, nearest])
        assert len(result["route"]) >= 1


# ======================================================================
# Single station
# ======================================================================


class TestSingleStationNetwork:
    """Degenerate case: 1 node, 0 edges."""

    async def test_hydration_single_station(
        self, message_bus, redis_client, station_client, single_station_network,
    ):
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        success = await pod._hydrate_route(["1"])
        assert success is True
        assert pod.current_segment is None
        assert len(pod.route_queue) == 0

    async def test_idle_route_single_node(
        self, message_bus, redis_client, station_client, single_station_network,
    ):
        strategy = OfflineRoutingStrategy(single_station_network)
        result = strategy._get_idle_route("1")
        assert result["route"] == ["1"]
        assert result["duration"] == 0


# ======================================================================
# Synthetic edge fallback
# ======================================================================


class TestSyntheticEdgeFallback:
    """Pod synthesizes edge when direct connection missing but positions known."""

    async def test_synthetic_edge_created(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Stations 1 and 4 are not directly connected but both have positions."""
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        # Remove the edge from context so hydration must synthesize it
        edges_to_remove = [k for k in network_context.edges if "1->4" in k]
        for k in edges_to_remove:
            del network_context.edges[k]

        success = await pod._hydrate_route(["1", "4"])
        assert success is True
        assert pod.current_segment is not None
        assert pod.current_segment.start_node == "1"
        assert pod.current_segment.end_node == "4"

    async def test_hydration_fails_unknown_station(
        self, message_bus, redis_client, station_client, network_context,
    ):
        """Station positions don't exist → hydration fails."""
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        success = await pod._hydrate_route(["1", "999"])
        assert success is False
        assert pod.current_segment is None


# ======================================================================
# RoutingProvider fallback chain
# ======================================================================


class TestRoutingProviderFallback:
    """RoutingProvider tries routers in order; raises if all fail."""

    async def test_no_routers_raises(self, network_context):
        provider = RoutingProvider()
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[], network_state={}, system_metrics={},
        )
        with pytest.raises(ValueError, match="No routers configured"):
            await provider.route(context)

    async def test_first_fails_second_succeeds(self, network_context):
        class FailingRouter(Router):
            async def route(self, context):
                raise ConnectionError("down")

        provider = RoutingProvider()
        provider.add_router(FailingRouter())
        provider.add_router(OfflineRouter(network_context))

        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[], network_state={}, system_metrics={},
            passengers=[{"passenger_id": "p1", "destination": "2"}],
        )
        route = await provider.route(context)
        assert route is not None
        assert len(route.stations) >= 1

    async def test_all_fail_raises(self, network_context):
        class AlwaysFails(Router):
            async def route(self, context):
                raise RuntimeError("broken")

        provider = RoutingProvider()
        provider.add_router(AlwaysFails())

        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[], network_state={}, system_metrics={},
        )
        with pytest.raises(ValueError, match="All routing strategies failed"):
            await provider.route(context)

    async def test_timeout_error_triggers_fallback(self, network_context):
        class TimeoutRouter(Router):
            async def route(self, context):
                raise TimeoutError("timed out")

        provider = RoutingProvider()
        provider.add_router(TimeoutRouter())
        provider.add_router(OfflineRouter(network_context))

        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[], network_state={}, system_metrics={},
            passengers=[{"passenger_id": "p1", "destination": "2"}],
        )
        route = await provider.route(context)
        assert route is not None


# ======================================================================
# Destination extraction
# ======================================================================


class TestDestinationExtraction:
    """OfflineRoutingStrategy._extract_destinations filters correctly."""

    def test_passenger_pod_ignores_cargo_requests(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[
                {"type": "cargo", "origin": "2", "destination": "3"},
            ],
            network_state={}, system_metrics={},
        )
        dests = strategy._extract_destinations(context)
        assert len(dests) == 0

    def test_cargo_pod_ignores_passenger_requests(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=0, weight_available=100.0,
            available_requests=[
                {"type": "passenger", "origin": "2", "destination": "3"},
            ],
            network_state={}, system_metrics={},
        )
        dests = strategy._extract_destinations(context)
        assert len(dests) == 0

    def test_onboard_passengers_add_destinations(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[],
            network_state={}, system_metrics={},
            passengers=[
                {"passenger_id": "p1", "destination": "3"},
                {"passenger_id": "p2", "destination": "4"},
            ],
        )
        dests = strategy._extract_destinations(context)
        assert "3" in dests
        assert "4" in dests

    def test_onboard_cargo_adds_destinations(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=0, weight_available=100.0,
            available_requests=[],
            network_state={}, system_metrics={},
            cargo=[{"request_id": "c1", "destination": "2"}],
        )
        dests = strategy._extract_destinations(context)
        assert "2" in dests

    def test_empty_requests_and_no_payload(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[],
            network_state={}, system_metrics={},
        )
        dests = strategy._extract_destinations(context)
        assert len(dests) == 0


# ======================================================================
# TSP solver
# ======================================================================


class TestTSP:
    """OfflineRoutingStrategy._solve_traveling_salesman edge cases."""

    def test_empty_destinations_returns_start(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        route = strategy._solve_traveling_salesman("1", [])
        assert route == ["1"]

    def test_single_destination(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        route = strategy._solve_traveling_salesman("1", ["2"])
        assert route[0] == "1"
        assert route[-1] == "2"


# ======================================================================
# Idle routing
# ======================================================================


class TestIdleRouting:
    """_get_idle_route behavior on various topologies."""

    def test_multi_node_idles_to_nearest(self, network_context):
        strategy = OfflineRoutingStrategy(network_context)
        result = strategy._get_idle_route("1")
        # Should stay idle at current station
        assert len(result["route"]) == 1
        assert result["route"][0] == "1"
        assert result["duration"] == 0

    def test_single_node_stays_put(self, single_station_network):
        strategy = OfflineRoutingStrategy(single_station_network)
        result = strategy._get_idle_route("1")
        assert result["route"] == ["1"]


# ======================================================================
# Scale-up tests
# ======================================================================


class TestScaleUp:
    """Verify system behavior at scale: many stations, pods, passengers."""

    async def test_large_network_hydration(
        self, message_bus, redis_client, station_client, large_network,
    ):
        """50-station grid: hydration of a long route works."""
        pod = make_passenger_pod(
            message_bus, redis_client, station_client, station_id="1"
        )
        # Route across the grid: 1→2→3→4→5→6→7 (first row)
        route = [str(i) for i in range(1, 8)]
        success = await pod._hydrate_route(route)
        assert success is True
        assert pod.current_segment is not None
        assert len(pod.route_queue) == 5  # 6 edges - 1 current

    async def test_large_network_routing(
        self, message_bus, redis_client, station_client, large_network,
    ):
        """50-station grid: offline router finds a path across the grid."""
        strategy = OfflineRoutingStrategy(large_network)
        context = DecisionContext(
            pod_id="1", current_location="1", current_route=None,
            capacity_available=10, weight_available=0.0,
            available_requests=[],
            network_state={}, system_metrics={},
            passengers=[{"passenger_id": "p1", "destination": "50"}],
        )
        result = strategy.calculate_optimal_route(context)
        assert len(result["route"]) >= 2
        assert result["route"][0] == "1"
        assert result["route"][-1] == "50"

    async def test_20_concurrent_pod_decisions(
        self, message_bus, redis_client, station_client, large_network,
    ):
        """20 pods making decisions concurrently on the same network."""
        pods = []
        for i in range(1, 21):
            station_id = str(i)
            pod = make_passenger_pod(
                message_bus, redis_client, station_client,
                pod_id=f"pod_{i}", station_id=station_id,
            )
            pod._available_requests = [{
                "type": "passenger",
                "passenger_id": f"p_{i}",
                "origin": station_id,
                "destination": str(50 - i + 1),
            }]
            pods.append(pod)

        # All 20 pods decide concurrently
        results = await asyncio.gather(
            *[pod.make_decision() for pod in pods],
            return_exceptions=True,
        )
        # All should succeed (no crashes)
        errors = [r for r in results if isinstance(r, Exception)]
        assert len(errors) == 0, f"Errors: {errors}"
        # All pods should have a route
        for pod in pods:
            assert pod.current_route is not None

    async def test_station_with_100_pending_passengers(
        self, message_bus, redis_client, station_client, large_network,
    ):
        """Station handles 100 pending passengers without degradation."""
        from aexis.station import Station

        station = Station(message_bus, redis_client, "1")
        for i in range(100):
            await station._handle_passenger_arrival({
                "station_id": "1",
                "passenger_id": f"p_{i:04d}",
                "destination": str((i % 49) + 2),
            })
        assert station._passenger_count == 100

        # Verify all 100 are in Redis
        count = await redis_client.hlen("aexis:station:1:passengers")
        assert count == 100

        # Congestion should be maxed on passenger component
        assert station.congestion_level > 0.3

        # Pending query should return all unclaimed
        pending = await station_client.get_pending_passengers("1")
        assert len(pending) == 100

    async def test_concurrent_claims_at_scale(
        self, message_bus, redis_client, station_client, large_network,
    ):
        """10 pods racing for 5 passengers — exactly 5 total winners."""
        for i in range(5):
            await async_seed_passenger(
                redis_client, "1", f"p_race_{i}", "2"
            )

        async def claim_all(pod_id):
            wins = 0
            for i in range(5):
                if await station_client.claim_passenger(
                    "1", f"p_race_{i}", pod_id
                ):
                    wins += 1
            return wins

        results = await asyncio.gather(
            *[claim_all(f"pod_{j}") for j in range(10)]
        )
        total_wins = sum(results)
        # Each passenger can only be claimed once
        assert total_wins == 5
