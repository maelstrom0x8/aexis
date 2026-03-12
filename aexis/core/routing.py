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
    """Abstract base for all routing strategies - consistent async interface (LSP compliant)"""

    @abc.abstractmethod
    async def route(self, context: DecisionContext) -> Route:
        """Route request. All implementations must be async for consistent interface."""
        pass


class RoutingStrategy(abc.ABC):
    """Strategy for route optimization (SRP: route calculation only)"""

    @abc.abstractmethod
    def calculate_optimal_route(self, context: DecisionContext) -> dict:
        """Calculate optimal route. Returns dict with route, duration, distance, confidence."""
        pass


class RoutingProvider:
    """Manages fallback chain of routers (SRP: delegation only)"""

    def __init__(self) -> None:
        self._routers: list[Router] = []

    def add_router(self, router: Router) -> None:
        """Add router to fallback chain"""
        if not isinstance(router, Router):
            raise TypeError(f"Expected Router, got {type(router)}")
        self._routers.append(router)

    async def route(self, context: DecisionContext) -> Route:
        """Get route by trying routers in order (DRY: async/sync consistent)"""
        if not self._routers:
            raise ValueError("No routers configured")

        for router in self._routers:
            try:
                route = await router.route(context)
                if route:
                    return route
            except ConnectionError as e:
                # Recoverable: connection issue, try next router
                logger.warning(
                    f"Router {router.__class__.__name__} connection failed (will try next): {e}",
                    extra={
                        "pod_id": context.pod_id,
                        "router": router.__class__.__name__,
                    },
                )
                continue
            except TimeoutError as e:
                # Recoverable: timeout, try next router
                logger.warning(
                    f"Router {router.__class__.__name__} timeout (will try next): {e}",
                    extra={
                        "pod_id": context.pod_id,
                        "router": router.__class__.__name__,
                    },
                )
                continue
            except Exception as e:
                # Other errors: log but continue fallback chain
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
    """Route calculation using offline algorithm (SRP: calculation only, no AI)"""

    def __init__(self, network_context: NetworkContext | None = None):
        # Dependency injection - can be tested with mock NetworkContext
        self.network_context = network_context or NetworkContext.get_instance()

    def calculate_optimal_route(self, context: DecisionContext) -> dict:
        """Calculate optimal route using TSP approximation"""
        # Fix: Consider passengers/cargo as active tasks
        # If no requests AND no onboard payload, then patrol
        has_payload = bool(context.passengers) or bool(context.cargo)
        if not context.available_requests and not has_payload:
            # No requests and no payload: remain idle at current station
            return self._get_idle_route(context.current_location)

        # Get valid destinations based on pod type
        destinations = self._extract_destinations(context)

        if not destinations:
            # Still no destinations: remain idle
            return self._get_idle_route(context.current_location)

        # Solve TSP
        optimal_route = self._solve_traveling_salesman(
            context.current_location, list(destinations)
        )

        # Calculate metrics
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
        """Extract valid destinations from available requests (SRP: filtering logic)"""
        destinations = set()

        # Determine pod type from capacity constraints
        is_passenger_pod = (
            context.capacity_available >= 0 and context.weight_available == 0
        )
        is_cargo_pod = context.weight_available > 0 and context.capacity_available == 0

        for req in context.available_requests:
            req_type = req.get("type")

            # Validate request matches pod type
            if is_passenger_pod and req_type != "passenger":
                continue
            if is_cargo_pod and req_type != "cargo":
                continue

            # If we are NOT at the origin, the origin is a destination for pickup
            origin = req.get("origin")
            if origin and origin != context.current_location:
                destinations.add(origin)

            destination = req.get("destination")
            if destination and destination != context.current_location:
                destinations.add(destination)

        # Add destinations of current passengers
        if context.passengers:
            for p in context.passengers:
                dest = p.get("destination")
                if dest and dest != context.current_location:
                    destinations.add(dest)

        # Add destinations of current cargo
        if context.cargo:
            for c in context.cargo:
                dest = c.get("destination")
                if dest and dest != context.current_location:
                    destinations.add(dest)
    
        return destinations

    def _solve_traveling_salesman(
        self, start: str, destinations: list[str]
    ) -> list[str]:
        """Nearest-neighbor TSP approximation"""
        if not destinations:
            return [start]

        full_route = [start]
        current = start
        unvisited = destinations.copy()

        while unvisited:
            nearest = self._find_nearest_station(current, unvisited)
            try:
                path = nx.shortest_path(
                    self.network_context.network_graph,
                    current,
                    nearest,
                    weight="weight",
                )
                # Skip current to avoid duplication
                if len(path) > 1:
                    full_route.extend(path[1:])
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                # Fallback: just add nearest directly
                full_route.append(nearest)

            unvisited.remove(nearest)
            current = nearest

        return full_route

    def _find_nearest_station(self, current: str, candidates: list[str]) -> str:
        """Find nearest station from candidates"""
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
        """Get idle route (stay at current location)"""
        return {
            "route": [current_location],
            "duration": 0,
            "distance": 0,
            "confidence": 1.0,
        }

    def _estimate_travel_time(self, distance: float, network_state: dict) -> int:
        """Estimate travel time in minutes"""
        base_speed = 50.0  # units per minute
        congestion_factor = network_state.get("avg_congestion", 0.0)
        adjusted_speed = base_speed * (1.0 - congestion_factor * 0.5)
        travel_time = distance / adjusted_speed if adjusted_speed > 0 else 0
        return int(round(travel_time))


class OfflineRouter(Router):
    """Offline routing algorithm for fallback decision making"""

    def __init__(self, network_context: NetworkContext = None):
        # Use provided NetworkContext or fall back to singleton for backward compatibility
        self.network_context = network_context or NetworkContext.get_instance()

    async def route(self, context: DecisionContext) -> Route:
        """Get route using offline strategy (LSP: async interface)"""
        strategy = OfflineRoutingStrategy(self.network_context)
        result = strategy.calculate_optimal_route(context)
        return Route(
            route_id=f"offline_{datetime.now().timestamp()}",
            stations=result["route"],
            estimated_duration=result["duration"],
            distance=result["distance"],
        )


class AIDecisionEngine:
    """Encapsulates AI decision making logic (SRP: AI decisions only)"""

    def __init__(self, ai_provider: AIProvider, pod_id: str):
        self.ai_provider = ai_provider
        self.pod_id = pod_id
        self.failure_count = 0
        self.last_failure = None
        self.decision_history: list[Decision] = []

    async def make_decision(self, context: DecisionContext) -> Decision:
        """Make routing decision using AI"""
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
        """Determine if AI should be used"""
        if not self.ai_provider or not self.ai_provider.is_available():
            return False

        # Check if we should retry after previous failure
        if self.last_failure:
            retry_window = timedelta(minutes=30)
            if datetime.now(UTC) - self.last_failure < retry_window:
                return False

        return True

    def _record_success(self, decision: Decision) -> None:
        """Record successful decision"""
        decision.timestamp = datetime.now(UTC)
        self.decision_history.append(decision)
        # Keep only last 100 decisions
        if len(self.decision_history) > 100:
            self.decision_history = self.decision_history[-50:]
        # Reset failure tracking
        self.failure_count = 0
        self.last_failure = None

    def _record_failure(self, error: Exception) -> None:
        """Record AI failure"""
        self.failure_count += 1
        self.last_failure = datetime.now(UTC)
        logger.warning(f"AI decision failed for pod {self.pod_id}: {error}")


class AIRouter(Router):
    """Router using AI with offline fallback (LSP: consistent async interface)"""

    def __init__(
        self,
        pod_id: str,
        ai_provider: AIProvider | None = None,
        fallback_strategy: RoutingStrategy | None = None,
    ):
        """Initialize with dependency injection (DIP compliant)

        Args:
            pod_id: Pod identifier
            ai_provider: AI provider for decisions (optional, will use MockAIProvider if None)
            fallback_strategy: Fallback routing strategy if AI fails
        """
        self.pod_id = pod_id
        self.ai_provider = ai_provider or MockAIProvider()
        self.fallback_strategy = fallback_strategy or OfflineRoutingStrategy()
        self.decision_engine = AIDecisionEngine(self.ai_provider, pod_id)

    async def route(self, context: DecisionContext) -> Route:
        """Get route using AI with offline fallback (LSP: consistent async interface)"""
        try:
            decision = await self.decision_engine.make_decision(context)
            route_data = {
                "route": decision.route,
                "duration": decision.estimated_duration,
                "confidence": decision.confidence,
            }
        except Exception as e:
            logger.debug(
                f"AI routing failed for pod {self.pod_id}, using fallback: {e}"
            )
            # Fallback to offline routing
            route_data = self.fallback_strategy.calculate_optimal_route(context)

        return Route(
            route_id=f"ai_{datetime.now().timestamp()}",
            stations=route_data["route"],
            estimated_duration=route_data["duration"],
            distance=route_data.get("distance", 0.0),
        )

    # Note: _build_ai_prompt moved to AIProvider where it belongs
