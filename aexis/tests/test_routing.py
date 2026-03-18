
import pytest

from aexis.core.model import DecisionContext, Route
from aexis.core.routing import OfflineRouter, RoutingProvider

@pytest.fixture
def network_context_for_routing(network_context):
    return network_context

class TestOfflineRouter:

    async def test_routes_to_nearest_destination(
        self, network_context_for_routing
    ):
        router = OfflineRouter()
        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=10,
            weight_available=0.0,
            available_requests=[
                {
                    "type": "passenger",
                    "origin": "2",
                    "destination": "4",
                    "passenger_id": "p_001",
                },
            ],
            network_state={},
            system_metrics={},
            pod_type="passenger",
            pod_constraints={},
            specialization="passenger_transport",
            passengers=[],
            cargo=[],
        )

        route = await router.route(context)
        assert isinstance(route, Route)
        assert "1" in route.stations
        assert "2" in route.stations

    async def test_idle_route_when_no_requests(
        self, network_context_for_routing
    ):
        router = OfflineRouter()
        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=10,
            weight_available=0.0,
            available_requests=[],
            network_state={},
            system_metrics={},
            pod_type="passenger",
            pod_constraints={},
            specialization="passenger_transport",
            passengers=[],
            cargo=[],
        )

        route = await router.route(context)

        assert len(route.stations) == 1
        assert route.stations[0] == "1"

    async def test_route_with_passengers_onboard(
        self, network_context_for_routing
    ):
        router = OfflineRouter()
        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=8,
            weight_available=0.0,
            available_requests=[],
            network_state={},
            system_metrics={},
            pod_type="passenger",
            pod_constraints={},
            specialization="passenger_transport",
            passengers=[
                {"passenger_id": "p_001", "destination": "4"},
                {"passenger_id": "p_002", "destination": "3"},
            ],
            cargo=[],
        )

        route = await router.route(context)
        assert isinstance(route, Route)

        assert "3" in route.stations or "4" in route.stations

    async def test_cargo_route_with_mixed_destinations(
        self, network_context_for_routing
    ):
        router = OfflineRouter()
        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=0,
            weight_available=400.0,
            available_requests=[
                {
                    "type": "cargo",
                    "origin": "3",
                    "destination": "4",
                    "request_id": "c_001",
                    "weight": 50.0,
                },
            ],
            network_state={},
            system_metrics={},
            pod_type="cargo",
            pod_constraints={},
            specialization="cargo_transport",
            passengers=[],
            cargo=[
                {"request_id": "c_existing", "destination": "2"},
            ],
        )

        route = await router.route(context)
        assert isinstance(route, Route)

        assert len(route.stations) >= 2

class TestRoutingProvider:

    async def test_provider_delegates_to_router(
        self, network_context_for_routing
    ):
        provider = RoutingProvider()
        provider.add_router(OfflineRouter())

        context = DecisionContext(
            pod_id="1",
            current_location="1",
            current_route=None,
            capacity_available=10,
            weight_available=0.0,
            available_requests=[],
            network_state={},
            system_metrics={},
            pod_type="passenger",
            pod_constraints={},
            specialization="passenger_transport",
            passengers=[],
            cargo=[],
        )

        route = await provider.route(context)
        assert isinstance(route, Route)
        assert len(route.stations) == 1
        assert route.stations[0] == "1"
