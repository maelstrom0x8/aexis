import abc
import logging
import sys
from datetime import UTC, datetime, timedelta

import networkx as nx

from .network import NetworkContext
from .ai_provider import AIProvider, MockAIProvider
from .model import Decision, DecisionContext, Route

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aexis_core.pod.log"),
    ],
)
logger = logging.getLogger(__name__)

class Router(abc.ABC):

    @abc.abstractmethod
    async def route(self, context: DecisionContext) -> Route:
        pass

class RoutingStrategy(abc.ABC):

    @abc.abstractmethod
    def calculate_optimal_route(self, context: DecisionContext) -> dict:
        pass

class RoutingProvider:

    def __init__(self) -> None:
        self._routers: list[Router] = []

    def add_router(self, router: Router) -> None:
        if not isinstance(router, Router):
            raise TypeError(f"Expected Router, got {type(router)}")
        self._routers.append(router)

    async def route(self, context: DecisionContext) -> Route:
        if not self._routers:
            raise ValueError("No routers configured")

        for router in self._routers:
            try:
                route = await router.route(context)
                if route:
                    return route
            except ConnectionError as e:

                logger.warning(
                    f"Router {router.__class__.__name__} connection failed (will try next): {e}",
                    extra={
                        "pod_id": context.pod_id,
                        "router": router.__class__.__name__,
                    },
                )
                continue
            except TimeoutError as e:

                logger.warning(
                    f"Router {router.__class__.__name__} timeout (will try next): {e}",
                    extra={
                        "pod_id": context.pod_id,
                        "router": router.__class__.__name__,
                    },
                )
                continue
            except Exception as e:

                logger.warning(
                    f"Router {router.__class__.__name__} failed: {e}",
                    extra={
                        "pod_id": context.pod_id,
                        "router": router.__class__.__name__,
                    },
                    exc_info=True,
                )
                continue

        raise ValueError("All routing strategies failed")

class OfflineRoutingStrategy(RoutingStrategy):

    def __init__(self, network_context: NetworkContext | None = None):

        self.network_context = network_context or NetworkContext.get_instance()

    def calculate_optimal_route(self, context: DecisionContext) -> dict:

        has_payload = bool(context.passengers) or bool(context.cargo)
        if not context.available_requests and not has_payload:

            return self._get_idle_route(context.current_location)

        destinations = self._extract_destinations(context)

        if not destinations:

            return self._get_idle_route(context.current_location)

        optimal_route = self._solve_traveling_salesman(
            context.current_location, list(destinations)
        )

        total_distance = self.network_context.get_route_distance(optimal_route)
        estimated_duration = self._estimate_travel_time(
            total_distance, context.network_state
        )

        return {
            "route": optimal_route,
            "duration": estimated_duration,
            "distance": total_distance,
            "confidence": 0.75,
        }

    def _extract_destinations(self, context: DecisionContext) -> set:
        destinations = set()

        is_passenger_pod = (
            context.capacity_available >= 0 and context.weight_available == 0
        )
        is_cargo_pod = context.weight_available > 0 and context.capacity_available == 0

        for req in context.available_requests:
            req_type = req.get("type")

            if is_passenger_pod and req_type != "passenger":
                continue
            if is_cargo_pod and req_type != "cargo":
                continue

            origin = req.get("origin")
            destination = req.get("destination")

            if origin and origin != context.current_location:

                destinations.add(origin)
            elif origin == context.current_location and destination:

                destinations.add(destination)

        if context.passengers:
            for p in context.passengers:
                dest = p.get("destination")
                if dest and dest != context.current_location:
                    destinations.add(dest)

        if context.cargo:
            for c in context.cargo:
                dest = c.get("destination")
                if dest and dest != context.current_location:
                    destinations.add(dest)

        return destinations

    def _solve_traveling_salesman(
        self, start: str, destinations: list[str]
    ) -> list[str]:
        if not destinations:
            return [start]

        current = start
        unvisited = destinations.copy()

        nearest = self._find_nearest_station(current, unvisited)

        try:

            path = nx.shortest_path(
                self.network_context.network_graph,
                current,
                nearest,
                weight="weight",
            )
            return path
        except (nx.NetworkXNoPath, nx.NodeNotFound):

            return [start, nearest]

    def _find_nearest_station(self, current: str, candidates: list[str]) -> str:
        if not candidates:
            return current

        nearest = candidates[0]
        min_distance = self.network_context.calculate_distance(current, nearest)

        for station in candidates[1:]:
            distance = self.network_context.calculate_distance(current, station)
            if distance < min_distance:
                min_distance = distance
                nearest = station

        return nearest

    def _get_idle_route(self, current_location: str) -> dict:
        return {
            "route": [current_location],
            "duration": 0,
            "distance": 0,
            "confidence": 1.0,
        }

    def _estimate_travel_time(self, distance: float, network_state: dict) -> int:
        base_speed = 50.0
        congestion_factor = network_state.get("avg_congestion", 0.0)
        adjusted_speed = base_speed * (1.0 - congestion_factor * 0.5)
        travel_time = distance / adjusted_speed if adjusted_speed > 0 else 0
        return int(round(travel_time))

class OfflineRouter(Router):

    def __init__(self, network_context: NetworkContext = None):

        self.network_context = network_context or NetworkContext.get_instance()

    async def route(self, context: DecisionContext) -> Route:
        strategy = OfflineRoutingStrategy(self.network_context)
        result = strategy.calculate_optimal_route(context)
        return Route(
            route_id=f"offline_{datetime.now().timestamp()}",
            stations=result["route"],
            estimated_duration=result["duration"],
            distance=result["distance"],
        )

class AIDecisionEngine:

    def __init__(self, ai_provider: AIProvider, pod_id: str):
        self.ai_provider = ai_provider
        self.pod_id = pod_id
        self.failure_count = 0
        self.last_failure = None
        self.decision_history: list[Decision] = []

    async def make_decision(self, context: DecisionContext) -> Decision:
        if not self._should_use_ai():
            raise ValueError("AI unavailable - use fallback")

        try:
            decision = await self.ai_provider.make_decision(context)
            self._record_success(decision)
            return decision
        except Exception as e:
            self._record_failure(e)
            raise

    def _should_use_ai(self) -> bool:
        if not self.ai_provider or not self.ai_provider.is_available():
            return False

        if self.last_failure:
            retry_window = timedelta(minutes=30)
            if datetime.now(UTC) - self.last_failure < retry_window:
                return False

        return True

    def _record_success(self, decision: Decision) -> None:
        decision.timestamp = datetime.now(UTC)
        self.decision_history.append(decision)

        if len(self.decision_history) > 100:
            self.decision_history = self.decision_history[-50:]

        self.failure_count = 0
        self.last_failure = None

    def _record_failure(self, error: Exception) -> None:
        self.failure_count += 1
        self.last_failure = datetime.now(UTC)
        logger.warning(f"AI decision failed for pod {self.pod_id}: {error}")

class AIRouter(Router):

    def __init__(
        self,
        pod_id: str,
        ai_provider: AIProvider | None = None,
        fallback_strategy: RoutingStrategy | None = None,
    ):
        self.pod_id = pod_id
        self.ai_provider = ai_provider or MockAIProvider()
        self.fallback_strategy = fallback_strategy or OfflineRoutingStrategy()
        self.decision_engine = AIDecisionEngine(self.ai_provider, pod_id)

    async def route(self, context: DecisionContext) -> Route:
        try:
            decision = await self.decision_engine.make_decision(context)
            route_stations = decision.route

            mandatory_destinations = set()
            if context.passengers:
                for p in context.passengers:
                    dest = p.get("destination")
                    if dest and dest != context.current_location:
                        mandatory_destinations.add(dest)
            if context.cargo:
                for c in context.cargo:
                    dest = c.get("destination")
                    if dest and dest != context.current_location:
                        mandatory_destinations.add(dest)

            for dest in mandatory_destinations:
                if dest not in route_stations:
                    route_stations.append(dest)

            route_data = {
                "route": route_stations,
                "duration": decision.estimated_duration,
                "confidence": decision.confidence,
            }
        except Exception as e:
            logger.debug(
                f"AI routing failed for pod {self.pod_id}, using fallback: {e}"
            )

            route_data = self.fallback_strategy.calculate_optimal_route(context)

        return Route(
            route_id=f"ai_{datetime.now().timestamp()}",
            stations=route_data["route"],
            estimated_duration=route_data["duration"],
            distance=route_data.get("distance", 0.0),
        )

