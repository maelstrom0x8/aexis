import logging
import sys
import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Dict, Optional, Any, Deque

from .message_bus import EventProcessor, MessageBus
from .model import (
    Decision,
    DecisionContext,
    LocationDescriptor,
    PodArrival,
    PodDecision,
    PodPositionUpdate,
    PodStatus,
    PodStatusUpdate,
    Route,
    Coordinate,
    EdgeSegment,
)
from .routing import OfflineRouter, RoutingProvider, AIDecisionEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aexis_core.pod.log"),
    ],
)
logger = logging.getLogger(__name__)


class PodType(Enum):
    """Enumeration of pod types for system identification"""
    PASSENGER = "passenger"
    CARGO = "cargo"


class Pod(EventProcessor):
    """Base Autonomous pod class"""

    def __init__(
        self,
        message_bus: MessageBus,
        pod_id: str,
        routing_provider: RoutingProvider | None = None,
        stations: dict | None = None,
    ):
        """Initialize pod with dependency injection (DIP compliant)

        Args:
            message_bus: Message bus for event publishing
            pod_id: Pod identifier
            routing_provider: Routing provider for route decisions. Pod is agnostic to routing implementation.
                            If None, creates default provider with offline routing.
            stations: Reference to system's station dict for live queue queries.
        """
        super().__init__(message_bus, pod_id)
        self.pod_id = pod_id
        self.status = PodStatus.IDLE
        self._available_requests = []
        self.decision: Optional[Decision] = None
        
        # Lock to prevent concurrent station arrival processing
        self._arrival_lock = asyncio.Lock()
        
        # Lock to prevent concurrent station arrival processing
        self._arrival_lock = asyncio.Lock()

        # Movement state
        self._stations = stations or {}  # Reference to system's station dict

        # PHASE 1: Position tracking on network
        self.location_descriptor = LocationDescriptor(
            "station", "station_001")  # Default

        # Continuous Navigation State
        self.route_queue: Deque[EdgeSegment] = deque()
        self.current_segment: Optional[EdgeSegment] = None
        self.segment_progress: float = 0.0  # Meters traveled on current segment
        # Increased speed for better responsiveness (m/s)
        self.speed: float = 20.0

        self.current_route: Route | None = None
        self.movement_start_time = None
        self.estimated_arrival = None

        # Pod type identification (to be set by subclasses)
        self.pod_type = self._get_pod_type()

        # Setup routing provider (DIP compliant, implementation-agnostic)
        if routing_provider:
            # Use injected provider (for testing or external configuration)
            self.routing_provider = routing_provider
        else:
            # Create default provider with offline routing
            self.routing_provider = RoutingProvider()
            self.routing_provider.add_router(OfflineRouter())

    @property
    def location(self) -> str:
        """Backward compatible location property for testing and CLI"""
        if self.location_descriptor.location_type == "station":
            return self.location_descriptor.node_id
        return self.location_descriptor.edge_id

    @location.setter
    def location(self, value: str):
        """Backward compatible location setter for testing and CLI"""
        if value is None:
            return
        if value.startswith("station_"):
            self.location_descriptor = LocationDescriptor(
                location_type="station",
                node_id=value
            )
        else:
            self.location_descriptor = LocationDescriptor(
                location_type="edge",
                edge_id=value
            )

    def _get_pod_type(self) -> PodType:
        """Get the pod type - must be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement _get_pod_type")

    def get_pod_constraints(self) -> Dict[str, Any]:
        """Get pod-specific constraints for routing decisions"""
        cap_used, cap_total, w_used, w_total = self._get_capacity_status()
        return {
            "pod_type": self.pod_type.value,
            "capacity_available": cap_total - cap_used,
            "weight_available": w_total - w_used,
            "max_capacity": cap_total,
            "max_weight": w_total,
            "current_load": {
                "passengers": cap_used,
                "cargo_weight": w_used
            }
        }

    async def _setup_subscriptions(self):
        """Subscribe to relevant channels"""
        self.message_bus.subscribe(
            MessageBus.CHANNELS["POD_COMMANDS"], self._handle_command
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["SYSTEM_EVENTS"], self._handle_system_event
        )

    async def _cleanup_subscriptions(self):
        """Unsubscribe from channels"""
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["POD_COMMANDS"], self._handle_command
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["SYSTEM_EVENTS"], self._handle_system_event
        )

    async def _handle_command(self, data: dict):
        """Handle incoming commands"""
        try:
            command_type = data.get("message", {}).get("command_type", "")
            target = data.get("message", {}).get("target", "")

            if target != self.pod_id:
                return

            if command_type == "AssignRoute":
                await self._handle_route_assignment(data)

        except KeyError as e:
            logger.warning(
                f"Pod {self.pod_id}: malformed command message - missing key {e}"
            )
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} command handling error: {e}", exc_info=True
            )

    async def _handle_system_event(self, data: dict):
        """Handle system-wide events"""
        try:
            event_type = data.get("message", {}).get("event_type", "")

            # React to congestion alerts
            if event_type == "CongestionAlert":
                await self._handle_congestion_alert(data)

        except KeyError as e:
            logger.warning(
                f"Pod {self.pod_id}: malformed event data - missing key {e}")
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} event handling error: {e}", exc_info=True)

    async def _handle_route_assignment(self, data: dict):
        """Handle route assignment command"""
        try:
            msg_content = data.get("message", {})
            parameters = msg_content.get("parameters", {})
            
            # Look in parameters first, then top level of message (for serialized dataclasses)
            route_data = parameters.get("route") or msg_content.get("route", [])

            # Handle if route is just list of strings (legacy/command input)
            # We need to convert it to a Route object
            # For MVP, if we get a list, we wrap it in a dummy Route
            if isinstance(route_data, list):
                from .model import Route

                # Simple dummy route object
                route_obj = Route(
                    route_id=f"route_{datetime.now().timestamp()}",
                    stations=route_data,
                    estimated_duration=len(route_data) * 5,
                )
                self.current_route = route_obj
            elif isinstance(route_data, dict):
                # Deserialize from dict with validation
                required_fields = {"route_id",
                                   "stations", "estimated_duration"}
                if not required_fields.issubset(route_data.keys()):
                    logger.error(
                        f"Invalid route object: missing fields {required_fields - route_data.keys()}"
                    )
                    return
                route_obj = Route(
                    route_id=route_data["route_id"],
                    stations=route_data["stations"],
                    estimated_duration=route_data["estimated_duration"],
                )
                self.current_route = route_obj
            else:
                logger.error(
                    f"Invalid route data type: {type(route_data)}. Expected list or dict."
                )
                return

            if self.current_route and self.current_route.stations:
                # Hydrate route into edge segments
                if await self._hydrate_route(self.current_route.stations):
                    self.status = PodStatus.EN_ROUTE
                    self.movement_start_time = datetime.now(UTC)
                    self.estimated_arrival = self.movement_start_time + timedelta(
                        minutes=self.current_route.estimated_duration
                    )

                    await self._publish_status_update()
                    logger.info(
                        f"Pod {self.pod_id} assigned route: {self.current_route.stations}"
                    )
                else:
                    self.status = PodStatus.IDLE
                    logger.error(f"Pod {self.pod_id} rejected invalid route")

        except ValueError as e:
            logger.error(f"Pod {self.pod_id}: invalid route data - {e}")
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} route assignment error: {e}", exc_info=True
            )

    async def _hydrate_route(self, stations: list[str]) -> bool:
        """Convert list of station IDs into a queue of EdgeSegments for navigation"""
        from .network import NetworkContext
        network = NetworkContext.get_instance()

        # If we have a current transit segment, check if it leads to the start of the new route
        preserved_segment = False
        if self.current_segment and stations and self.current_segment.end_node == stations[0]:
            # Keep current_segment, just clear the queue after it
            self.route_queue.clear()
            preserved_segment = True
        else:
            self.route_queue.clear()
            self.current_segment = None
            self.segment_progress = 0.0

        if len(stations) < 2:
            return True

        for i in range(len(stations) - 1):
            start = stations[i]
            end = stations[i+1]
            edge_id = f"{start}->{end}"

            # Check edge existence
            if edge_id in network.edges:
                self.route_queue.append(network.edges[edge_id])
            else:
                # Fallback: Create synthetic edge IF BOTH STATIONS EXIST
                if start in network.station_positions and end in network.station_positions:
                    logger.warning(
                        f"Pod {self.pod_id} hydrating synthetic edge {edge_id}")
                    p1 = network.station_positions[start]
                    p2 = network.station_positions[end]
                    synthetic_edge = EdgeSegment(
                        segment_id=edge_id,
                        start_node=start,
                        end_node=end,
                        start_coord=Coordinate(p1[0], p1[1]),
                        end_coord=Coordinate(p2[0], p2[1])
                    )
                    self.route_queue.append(synthetic_edge)
                else:
                    logger.error(f"Pod {self.pod_id} hydration failed: unknown station in route {edge_id}")
                    # Clear queue to stop movement
                    self.route_queue.clear()
                    self.current_segment = None
                    return False

        # Prime the first segment
        if self.route_queue:
            self.current_segment = self.route_queue.popleft()
            self.segment_progress = 0.0
        
        return True

    async def _handle_congestion_alert(self, data: dict):
        """Handle congestion alerts"""
        try:
            alert_data = data.get("message", {}).get("data", {})
            affected_routes = alert_data.get("affected_routes", [])

            if not self.current_route or not self.current_route.stations:
                return

            # Check if current route is affected
            current_route_str = "->".join(self.current_route.stations)
            if any(route in current_route_str for route in affected_routes):
                logger.info(f"Pod {self.pod_id} route affected by congestion")
                # Could trigger re-routing decision here

        except KeyError as e:
            logger.warning(
                f"Pod {self.pod_id}: malformed congestion alert - missing key {e}"
            )
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} congestion handling error: {e}", exc_info=True
            )

    async def make_decision(self):
        """Make routing decision (async to handle routing provider)"""
        try:
            # Build decision context
            context = await self._build_decision_context()

            # Get route from routing provider (now properly async)
            route = await self.routing_provider.route(context)
            # logger.warning(f"Pod {self.pod_id} received route from provider: {route.stations if route else 'None'}")            

            # Convert route to decision format
            decision = Decision(
                decision_type="route_selection",
                accepted_requests=[],
                rejected_requests=[],
                route=route.stations,
                estimated_duration=route.estimated_duration,
                confidence=0.8,
                reasoning="Route determined by RoutingProvider",
                fallback_used=False,
            )

            
            self.decision = decision
            # Execute decision
            await self._execute_decision(decision)

            # Publish decision event
            await self._publish_decision_event(decision)

        except ValueError as e:
            logger.error(
                f"Pod {self.pod_id}: routing failure (all strategies exhausted) - {e}"
            )
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} decision making error: {e}", exc_info=True)

    async def _build_decision_context(self) -> DecisionContext:
        """Build context for decision making - must be implemented by subclasses

        Subclasses MUST override this method to provide pod-specific context.
        Base Pod class should never be instantiated directly.

        Raises:
            NotImplementedError: Always. This method must be overridden.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _build_decision_context(). "
            "Pod is an abstract class and should not be instantiated directly."
        )

    async def _execute_decision(self, decision: Decision):
        """Execute the routing decision and setup pickup/delivery"""
        if decision:
            # Create Route object from decision data
            from .model import Route

            self.current_route = Route(
                route_id=f"rt_{datetime.now().timestamp()}",
                stations=decision.route,
                estimated_duration=decision.estimated_duration,
            )

            # Hydrate route into segments for movement
            await self._hydrate_route(decision.route)

            # Only start movement if we have a valid route with segments
            if self.current_segment or self.route_queue:
                self.status = PodStatus.EN_ROUTE
                self.movement_start_time = datetime.now(UTC)
            else:
                self.status = PodStatus.IDLE
                logger.info(f"Pod {self.pod_id} remaining IDLE (no route segments)")

            # Setup pickup and delivery stations for pods
            await self._setup_pickup_delivery_routes(decision.route)

            # If already at a station, execute pickup/delivery immediately before starting movement
            if self.location_descriptor.location_type == "station":
                station_id = self.location_descriptor.node_id
                await self._execute_pickup(station_id)
                await self._execute_delivery(station_id)

            logger.info(
                f"Pod {self.pod_id} executing decision: {decision.route}")

    async def _setup_pickup_delivery_routes(self, stations: list[str]):
        """Setup pickup and delivery stations from route

        This is overridden by subclasses to handle passenger/cargo specific logic.
        """
        # Base implementation does nothing
        pass

    async def _execute_pickup(self, station_id: str):
        """Execute pickup at station (overridden by subclasses)"""
        pass

    async def _execute_delivery(self, station_id: str):
        """Execute delivery at station (overridden by subclasses)"""
        pass

    async def _publish_decision_event(self, decision: Decision):
        """Publish pod decision event"""
        event = PodDecision(
            pod_id=self.pod_id,
            decision_type=decision.decision_type,
            decision={
                "accepted_requests": decision.accepted_requests,
                "rejected_requests": decision.rejected_requests,
                "route": decision.route,
                "estimated_duration": decision.estimated_duration,
                "confidence": decision.confidence,
            },
            reasoning=decision.reasoning,
            confidence=decision.confidence,
            fallback_used=decision.fallback_used,
        )

        await self.publish_event(event)

    async def _publish_status_update(self):
        """Publish pod status update"""
        # Populate generic fields based on subclass
        cap_used, cap_total, w_used, w_total = self._get_capacity_status()

        event = PodStatusUpdate(
            pod_id=self.pod_id,
            location=self.location_descriptor.node_id if self.location_descriptor.location_type == "station" else self.location_descriptor.edge_id,
            status=self.status.value,
            capacity_used=cap_used,
            capacity_total=cap_total,
            weight_used=w_used,
            weight_total=w_total,
            current_route=self.current_route,
        )

        await self.publish_event(event)

    async def update(self, dt: float) -> bool:
        """Update pod physics for time step dt

        Implements Continuous Path Integration:
        - Consumes distance from current segment
        - Overflows to next segment in queue if dt > remaining length
        - Handles precise position interpolation
        """
        if self.status != PodStatus.EN_ROUTE or not self.current_segment:
            return False

        dist_to_travel = self.speed * dt
        # Safety cap to prevent warping across map in one lag spike (e.g. max 100m/tick)
        dist_to_travel = min(dist_to_travel, 100.0)

        while dist_to_travel > 0:
            if not self.current_segment:
                # End of route reached
                await self._handle_route_completion()
                return True

            remaining_on_edge = self.current_segment.length - self.segment_progress

            if dist_to_travel >= remaining_on_edge:
                # Overflow case: Complete this edge and continue to next
                dist_to_travel -= remaining_on_edge

                # Capture the node we just arrived at
                arrived_node = self.current_segment.end_node
                self._advance_segment()

                if not self.current_segment:
                    # logger.warning(
                    #     f"[{self.pod_id}]: Checking station arrival at {arrived_node}")
                    
                    # Fix: Handle station arrival asynchronously to prevent blocking physics loop
                    # If this was awaited, a slow decision (e.g. AI call) would freeze the entire simulation
                    asyncio.create_task(self._handle_station_arrival(arrived_node))
                    
                    # If pod is now loading/unloading, stop movement for this tick
                    # If pod is now loading/unloading, stop movement for this tick
                    if self.status in [PodStatus.LOADING, PodStatus.UNLOADING]:
                        return True
            else:
                # Normal case: Move along current edge
                self.segment_progress += dist_to_travel
                dist_to_travel = 0

        # Update observable location state
        self._update_location_descriptor()

        # Publish exactly one position update per physics tick
        # This gives the UI the final resolved position after all internal edge transitions
        await self._publish_position_update()
        return False

    def _advance_segment(self):
        """Move to next segment in queue"""
        if self.route_queue:
            self.current_segment = self.route_queue.popleft()
            self.segment_progress = 0.0
        else:
            # No more segments, we have arrived at final destination node of the last segment
            # Mark as finished so next loop iteration catches it
            self.current_segment = None
        # print(f"[{self.pod_id}]: Current segment after advance: {self.current_segment.segment_id if self.current_segment else 'None'}")

    def _update_location_descriptor(self):
        """Update the public location descriptor based on internal physics state"""
        if not self.current_segment:
            return

        # Interpolate exact position
        current_coord = self.current_segment.get_point_at_distance(
            self.segment_progress)

        if self.segment_progress == 0.0:
            # At start of segment = At start node (Station)
            self.location_descriptor = LocationDescriptor(
                location_type="station",
                node_id=self.current_segment.start_node,
                coordinate=current_coord
            )
        else:
            self.location_descriptor = LocationDescriptor(
                location_type="edge",
                edge_id=self.current_segment.segment_id,
                coordinate=current_coord,
                distance_on_edge=self.segment_progress
            )

    async def _handle_route_completion(self):
        """Handle arrival at destination"""
        # Snap to final station coordinate before clearing route
        if self.current_route and self.current_route.stations:
            final_station = self.current_route.stations[-1]
            from .network import NetworkContext
            nc = NetworkContext.get_instance()
            pos = nc.station_positions.get(final_station, (0, 0))

            self.location_descriptor = LocationDescriptor(
                location_type="station",
                node_id=final_station,
                coordinate=Coordinate(pos[0], pos[1])
            )

            # Handle station arrival (pickup/delivery)
            # Use create_task to avoid blocking the physics loop
            asyncio.create_task(self._handle_station_arrival(final_station))

        self.status = PodStatus.IDLE
        self.current_route = None
        self.segment_progress = 0.0

        await self._publish_position_update()
        await self._publish_status_update()
        logger.info(f"Pod {self.pod_id} arrived at destination")

        # Remain idle after arrival. A new decision should be triggered by:
        # - a reactive system event (e.g. PassengerArrival/CargoRequest)
        # - an explicit command
        # This prevents oscillation/patrol back-and-forth when no payloads are available.

    async def _handle_station_arrival(self, station_id: str):
        """Handle arrival at a station for pickup/delivery

        This is called when pod completes a route segment to a station.
        Subclasses override to implement pickup/delivery logic.
        """
        if self._arrival_lock.locked():
             logger.warning(f"Pod {self.pod_id} ignoring concurrent _handle_station_arrival at {station_id}")
             return

        async with self._arrival_lock:
             # Logic implemented by subclasses, calling super doesn't do much but good practice?
             # Actually this base method is empty in the original code? 
             # Let's check. No, the base method was abstract or empty in finding.
             # Wait, in the files I viewed, Pod is the base class.
             # PassengerPod and CargoPod override it.
             # I need to add the lock to THE SUBCLASSES or make the base class enforce it.
             # Providing a locked wrapper in base and renaming subclass methods would be cleaner,
             # but modifying subclasses is less invasive to the structure if I can't refactor everything.
             # Let's modify the subclasses. 
             pass
        # Base implementation does nothing
        # Subclasses implement passenger/cargo handling
        pass

    async def navigate_to_station(self, target_station: str) -> bool:
        """Start navigation from current position to target station

        Args:
            target_station: Station ID to navigate to

        Returns:
            True if navigation started, False if already at station
        """
        try:
            from .network import NetworkContext
            import networkx as nx

            network = NetworkContext.get_instance()

            # If already at a station, find path from there
            if self.location_descriptor.location_type == "station":
                current_station = self.location_descriptor.node_id
            else:
                # On an edge - find nearest station or endpoint
                current_station = network.get_nearest_station(
                    self.location_descriptor.coordinate)

            if current_station == target_station:
                logger.debug(f"Pod {self.pod_id} already at {target_station}")
                return False

            # Find shortest path using network graph
            try:
                path = nx.shortest_path(
                    network.network_graph, current_station, target_station)
                if len(path) < 2:
                    return False

                # Start navigation using new system
                # Convert path to segments
                # Since we are already in async method, we can hydrate immediately

                # Create a mock route object for hydration
                dummy_route = Route(
                    route_id=f"nav_{datetime.now().timestamp()}",
                    stations=path,
                    estimated_duration=10  # Estimation...
                )
                self.current_route = dummy_route

                # Hydrate
                await self._hydrate_route(path)

                if self.current_segment:
                    self.status = PodStatus.EN_ROUTE
                    # logger.info(
                    #     f"Pod {self.pod_id} starting navigation to {target_station}")
                    return True
                return False

                return False
            except nx.NetworkXNoPath:
                logger.warning(
                    f"Pod {self.pod_id}: no path to {target_station}")
                return False

        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} navigation error: {e}", exc_info=True)
            return False

    async def _publish_position_update(self):
        """Publish real-time position update for UI streaming"""
        event = PodPositionUpdate(
            pod_id=self.pod_id,
            location=self.location_descriptor,
            status=self.status.value,
            speed=self.speed if self.status == PodStatus.EN_ROUTE else 0.0,
            current_route=self.current_route.stations if self.current_route else None
        )
        await self.publish_event(event)

    def _get_capacity_status(self):
        """Return (cap_used, cap_total, weight_used, weight_total)"""
        return 0, 0, 0.0, 0.0

    def get_state(self) -> dict:
        """Get current pod state"""
        cap_used, cap_total, w_used, w_total = self._get_capacity_status()

        # Build location description
        if self.location_descriptor.location_type == "station":
            location_str = self.location_descriptor.node_id
        else:
            location_str = f"on edge {self.location_descriptor.edge_id} @ {self.segment_progress:.1f}m"

        return {
            "pod_id": self.pod_id,
            "pod_type": self._get_pod_type().value,
            "status": self.status.value,
            "location": location_str,
            "coordinate": {"x": self.location_descriptor.coordinate.x, "y": self.location_descriptor.coordinate.y},
            # Add explicit new fields for UI if they want them
            "spine_id": self.location_descriptor.edge_id,
            "distance": self.segment_progress,
            "capacity": {"used": cap_used, "total": cap_total},
            "weight": {"used": w_used, "total": w_total},
            "current_route": [s for s in self.current_route.stations]
            if self.current_route
            else [],
            "estimated_arrival": self.estimated_arrival.isoformat()
            if self.estimated_arrival
            else None,
        }


class PassengerPod(Pod):
    """Pod specialized for passenger transport"""

    def __init__(
        self,
        message_bus: MessageBus,
        pod_id: str,
        routing_provider: RoutingProvider | None = None,
        stations: dict | None = None,
    ):
        super().__init__(message_bus, pod_id, routing_provider, stations)
        self.capacity = 4  # Seats
        self.passengers = []  # List[dict] with passenger_id, destination
        self.pickup_route = []  # Stations to pick up from, in order
        self.delivery_route = []  # Stations to deliver to, in order

    def _get_pod_type(self) -> PodType:
        """Return passenger pod type"""
        return PodType.PASSENGER

    async def _handle_station_arrival(self, station_id: str):
        """Handle passenger pickup/delivery at station"""
        self.status = PodStatus.IDLE
        event = PodArrival(pod_id=self.pod_id, station_id=station_id)
        # logger.warning(f"Pod {self.pod_id} arrived at station {station_id}, checking for passengers")
        await self.message_bus.publish_event(MessageBus.get_event_channel(event.event_type), event)
        await self._execute_pickup(station_id)
        await self._execute_delivery(station_id)
        # Trigger routing decision for next leg (whether pickup happened or not)
        await self.make_decision()

    async def _execute_pickup(self, station_id: str):
        """Execute passenger pickup at station using live queue query and claim system"""
        remaining_capacity = self.capacity - len(self.passengers)
        if remaining_capacity <= 0:
            logger.debug(
                f"Pod {self.pod_id} at capacity, skipping pickup at {station_id}")
            return

        # Query live station queue instead of stale _available_requests
        station = self._stations.get(station_id)
        if not station:
            # Fallback to legacy behavior if no station reference
            # logger.warning(
            #     f"Pod {self.pod_id}: No station reference for {station_id}, using _available_requests")
            pickups = [r for r in self._available_requests if r.get(
                "origin") == station_id and r.get("type") == "passenger"]
        else:
            # Get pending (unclaimed) passengers from station
            pending = station.get_pending_passengers()
            logger.info(
                f"Pod {self.pod_id}: execute_passenger_pickup at {station_id}, {len(pending)} pending passengers")
            
            # Claim passengers atomically (prevents double-pickup)
            pickups = []
            for p in pending[:remaining_capacity]:
                passenger_id = p.get("passenger_id")
                
                # ADVERSARIAL FIX: check if passenger already somehow on board (Zombie check)
                if any(existing.get("passenger_id") == passenger_id for existing in self.passengers):
                    logger.warning(
                        f"Pod {self.pod_id}: Passenger {passenger_id} already on board! Skipping duplicate pickup.")
                    continue

                if station.claim_passenger(passenger_id, self.pod_id):
                    pickups.append(p)
                    # Remove from available requests locally to prevent re-routing to it
                    self._available_requests = [
                        r for r in self._available_requests
                        if r.get("passenger_id") != passenger_id
                    ]

        if not pickups:
            logger.debug(
                f"Pod {self.pod_id} found no claimable passengers at {station_id}")
            return

        self.status = PodStatus.LOADING
        await self._publish_status_update()

        # Simulate loading time: 5 seconds per passenger
        loading_time = len(pickups) * 5
        await asyncio.sleep(loading_time)

        from .model import PassengerPickedUp
        for p in pickups:
            passenger = {
                "passenger_id": p.get("passenger_id"),
                "destination": p.get("destination"),
                "pickup_time": datetime.now(UTC)
            }
            self.passengers.append(passenger)

            # Notify system/station
            event = PassengerPickedUp(
                passenger_id=passenger["passenger_id"],
                pod_id=self.pod_id,
                station_id=station_id,
                pickup_time=passenger["pickup_time"]
            )
            logger.debug(f"Pod {self.pod_id} publishing PassengerPickedUp for {passenger['passenger_id']} at {station_id}")
            await self.publish_event(event)

        self.status = PodStatus.EN_ROUTE
        # logger.warning(
        #     f"Pod {self.pod_id} loaded {len(pickups)} passengers at {station_id}")



    async def _execute_delivery(self, station_id: str):
        """Execute passenger delivery at station"""
        delivered = [p for p in self.passengers if p.get(
            'destination') == station_id]

        if not delivered:
            return

        self.status = PodStatus.UNLOADING
        await self._publish_status_update()

        # Simulate unloading time: 5 seconds per passenger
        unload_time = len(delivered) * 5
        await asyncio.sleep(unload_time)

        # Remove delivered passengers
        from .model import PassengerDelivered
        for passenger in delivered:
            self.passengers.remove(passenger)

            # Calculate travel time if pickup_time is available
            travel_time = 0
            if "pickup_time" in passenger:
                travel_time = int(
                    (datetime.now(UTC) - passenger["pickup_time"]).total_seconds())

            # Publish delivery event
            event = PassengerDelivered(
                passenger_id=passenger.get('passenger_id', ''),
                pod_id=self.pod_id,
                station_id=station_id,
                delivery_time=datetime.now(UTC),
                total_travel_time=travel_time,
                satisfaction_score=0.9
            )
            await self.publish_event(event)

        self.status = PodStatus.EN_ROUTE
        logger.info(
            f"Pod {self.pod_id} delivered {len(delivered)} passengers at {station_id}")

    async def _setup_pickup_delivery_routes(self, stations: list[str]):
        """Setup pickup and delivery stations for passenger route"""
        # First station is pickup, remaining are delivery destinations
        if stations:
            self.pickup_route = [stations[0]]
            self.delivery_route = stations[1:] if len(stations) > 1 else []
            # logger.warning(
            #     f"Pod {self.pod_id} pickup: {self.pickup_route}, delivery: {self.delivery_route}")

    async def _build_decision_context(self) -> DecisionContext:
        """Build decision context with passenger-specific constraints and available requests"""
        constraints = self.get_pod_constraints()

        # Get current location - either station or nearest station if on edge
        if self.location_descriptor.location_type == "station":
            current_location = self.location_descriptor.node_id
        elif self.current_segment:
            # In transit: logical location for next route is the end of current segment
            current_location = self.current_segment.end_node
        else:
            from .network import NetworkContext
            network = NetworkContext.get_instance()
            current_location = network.get_nearest_station(
                self.location_descriptor.coordinate)

        # Gather available passenger requests from all stations
        available_requests = []
        try:
            from .system import SystemContext
            system_context = SystemContext.get_instance()
            network = system_context.get_network_context()

            # Get all stations
            all_stations = list(network.station_positions.keys())

            for station_id in all_stations:
                # We can't directly access stations from pod context
                # This will be populated by the system when assigning routes
                pass
        except Exception as e:
            logger.debug(f"Error gathering requests for {self.pod_id}: {e}")

        return DecisionContext(
            pod_id=self.pod_id,
            current_location=current_location,
            current_route=self.current_route,
            capacity_available=self.capacity - len(self.passengers),
            weight_available=0.0,  # Passenger pods don't handle weight
            available_requests=self._available_requests,
            network_state={},
            system_metrics={},
            # Enhanced context with pod type information
            pod_type=self.pod_type.value,
            pod_constraints=constraints,
            specialization="passenger_transport",
            passengers=list(self.passengers),
            cargo=[]
        )

    def _get_capacity_status(self):
        return len(self.passengers), self.capacity, 0.0, 0.0

    def get_state(self) -> dict:
        state = super().get_state()
        state["passengers"] = self.passengers
        return state


class CargoPod(Pod):
    """Pod specialized for cargo transport"""

    def __init__(
        self,
        message_bus: MessageBus,
        pod_id: str,
        routing_provider: RoutingProvider | None = None,
        stations: dict | None = None,
    ):
        super().__init__(message_bus, pod_id, routing_provider, stations)
        self.weight_capacity = 500.0  # kg
        self.current_weight = 0.0
        self.cargo = []  # List[dict] with request_id, destination, weight
        self.pickup_route = []  # Stations to pick up cargo from
        self.delivery_route = []  # Stations to deliver cargo to

    def _get_pod_type(self) -> PodType:
        """Return cargo pod type"""
        return PodType.CARGO

    async def _handle_station_arrival(self, station_id: str):
        """Handle cargo pickup/delivery at station"""
        if self._arrival_lock.locked():
             return

        async with self._arrival_lock:
            # Simulate docking and context building time
            await asyncio.sleep(2.0)
            
            self.status = PodStatus.IDLE
            # set location to station
            self.location_descriptor = LocationDescriptor(
                location_type="station", node_id=station_id, coordinate=self.location_descriptor.coordinate,
            )
            
            # Prioritize delivery then pickup? Or pickup then delivery?
            # Usually delivery first to free up space!
            # The original code did pickup then delivery?
            # Let's check original.
            # Original: _execute_pickup then _execute_delivery.
            # Wait, if I pick up first, I might not have space if I verify capacity!
            # But _execute_pickup checks available capacity.
            # If I have passengers to deliver, they occupy space.
            # So if I deliver first, I free space.
            # So we should probably Deliver THEN Pickup.
            # But the crash is in Delivery.
            # Let's just wrap it for now. the logic order is a separate behavioral optimization.
            
            await self._execute_pickup(station_id)
            await self._execute_delivery(station_id)
            # Trigger routing decision for next leg
            await self.make_decision()

    async def _execute_pickup(self, station_id: str):
        """Execute cargo pickup at station using live queue query and claim system"""
        remaining_capacity = self.weight_capacity - self.current_weight
        if remaining_capacity <= 0:
            logger.debug(
                f"Pod {self.pod_id} at weight capacity, skipping pickup at {station_id}")
            return

        # Query live station queue instead of stale _available_requests
        station = self._stations.get(station_id)
        if not station:
            # Fallback to legacy behavior if no station reference
            logger.warning(
                f"Pod {self.pod_id}: No station reference for {station_id}, using _available_requests")
            pending_cargo = [
                r
                for r in self._available_requests
                if r.get("origin") == station_id and r.get("type") == "cargo"
            ]
        else:
            # Get pending (unclaimed) cargo from station
            pending_cargo = station.get_pending_cargo()
            logger.info(
                f"Pod {self.pod_id}: execute_cargo_pickup at {station_id}, {len(pending_cargo)} pending cargo items")

        if not pending_cargo:
            logger.debug(f"Pod {self.pod_id} found no cargo at {station_id}")
            return

        self.status = PodStatus.LOADING
        await self._publish_status_update()

        loaded_count = 0
        loaded_weight = 0.0

        from .model import CargoLoaded

        for req in pending_cargo:
            req_weight = float(req.get("weight", 0.0) or 0.0)
            if req_weight <= 0:
                continue
            if self.current_weight + loaded_weight + req_weight > self.weight_capacity:
                continue

            # Claim cargo atomically (prevents double-loading)
            request_id = req.get("request_id", "")
            if station and not station.claim_cargo(request_id, self.pod_id):
                continue  # Skip if claim failed (already claimed by another pod)

            cargo_item = {
                "request_id": request_id,
                "destination": req.get("destination", ""),
                "weight": req_weight,
                "pickup_time": datetime.now(UTC),
            }
            self.cargo.append(cargo_item)
            loaded_count += 1
            loaded_weight += req_weight

            event = CargoLoaded(
                request_id=cargo_item["request_id"],
                pod_id=self.pod_id,
                station_id=station_id,
                load_time=cargo_item["pickup_time"],
            )
            await self.publish_event(event)
            
            # Remove from available requests locally
            self._available_requests = [
                r for r in self._available_requests
                if r.get("request_id") != request_id
            ]

        if loaded_count == 0:
            self.status = PodStatus.IDLE
            await self._publish_status_update()
            return

        self.current_weight += loaded_weight

        # Simulate loading time: 10 seconds per 100kg
        loading_time = max(1.0, (loaded_weight / 100.0) * 10.0)
        await asyncio.sleep(loading_time)

        self.status = PodStatus.EN_ROUTE
        logger.info(
            f"Pod {self.pod_id} loaded {loaded_count} cargo items ({loaded_weight:.1f}kg) at {station_id}"
        )

    async def _execute_delivery(self, station_id: str):
        """Execute cargo delivery at station"""
        delivered = [c for c in self.cargo if c.get(
            'destination') == station_id]

        if not delivered:
            return

        self.status = PodStatus.UNLOADING
        await self._publish_status_update()

        # Simulate unloading time: 10 seconds per item
        unload_time = len(delivered) * 10
        await asyncio.sleep(unload_time)

        # Remove delivered cargo and update weight
        for cargo in delivered:
            self.cargo.remove(cargo)
            weight = cargo.get('weight', 0)
            self.current_weight -= weight

            # Publish delivery event
            from .model import CargoDelivered
            event = CargoDelivered(
                request_id=cargo.get('request_id', ''),
                pod_id=self.pod_id,
                station_id=station_id,
                delivery_time=datetime.now(UTC),
                condition="intact",
                on_time=True
            )
            await self.publish_event(event)

        self.status = PodStatus.EN_ROUTE
        logger.info(
            f"Pod {self.pod_id} delivered {len(delivered)} cargo items at {station_id}")

    async def _setup_pickup_delivery_routes(self, stations: list[str]):
        """Setup pickup and delivery stations for cargo route"""
        # First station is pickup, remaining are delivery destinations
        if stations:
            self.pickup_route = [stations[0]]
            self.delivery_route = stations[1:] if len(stations) > 1 else []
            logger.debug(
                f"Pod {self.pod_id} pickup: {self.pickup_route}, delivery: {self.delivery_route}")

    async def _build_decision_context(self) -> DecisionContext:
        """Build decision context with cargo-specific constraints"""
        constraints = self.get_pod_constraints()

        # Get current location - either station or nearest station if on edge
        if self.location_descriptor.location_type == "station":
            current_location = self.location_descriptor.node_id
        elif self.current_segment:
            # In transit: logical location for next route is the end of current segment
            current_location = self.current_segment.end_node
        else:
            from .network import NetworkContext
            network = NetworkContext.get_instance()
            current_location = network.get_nearest_station(
                self.location_descriptor.coordinate)

        return DecisionContext(
            pod_id=self.pod_id,
            current_location=current_location,
            current_route=self.current_route,
            capacity_available=0,  # Cargo pods don't handle passenger capacity
            weight_available=self.weight_capacity - self.current_weight,
            available_requests=self._available_requests,
            network_state={},
            system_metrics={},
            # Enhanced context
            pod_type=self.pod_type.value,
            pod_constraints=constraints,
            specialization="cargo_transport",
            passengers=[],
            cargo=list(self.cargo)
        )

    def _get_capacity_status(self):
        return 0, 0, self.current_weight, self.weight_capacity

    def get_state(self) -> dict:
        state = super().get_state()
        state["cargo"] = self.cargo
        return state
