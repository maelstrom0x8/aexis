"""Pod service — runs as a standalone process managing autonomous pod behavior.

Each pod owns its movement simulation, routing decisions, and payload
pickup/delivery. Station interactions go through StationClient (Redis-backed)
so pods and stations can run in separate processes.

Redis key layout (owned by this pod):
  aexis:pod:{id}:state — STRING: JSON state snapshot
"""

import asyncio
import json
import logging
import sys
from collections import deque
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Deque, Optional

from redis.asyncio import Redis

from aexis.core.message_bus import MessageBus
from aexis.core.model import (
    CargoDelivered,
    CargoLoaded,
    Decision,
    DecisionContext,
    LocationDescriptor,
    PassengerArrival,
    PassengerDelivered,
    PassengerPickedUp,
    PodArrival,
    PodDecision,
    PodPositionUpdate,
    PodStatus,
    PodStatusUpdate,
    Route,
    Coordinate,
    EdgeSegment,
)
from aexis.core.routing import OfflineRouter, RoutingProvider
from aexis.core.station_client import StationClient

logger = logging.getLogger(__name__)


class PodType(Enum):
    PASSENGER = "passenger"
    CARGO = "cargo"


def _state_key(pod_id: str) -> str:
    return f"aexis:pod:{pod_id}:state"


class Pod:
    """Base autonomous pod — owns its physics, routing, and station interactions.

    Runs its own movement loop and subscribes to Redis events directly.
    Station queue operations go through StationClient.
    """

    def __init__(
        self,
        message_bus: MessageBus,
        redis_client: Redis,
        pod_id: str,
        station_client: StationClient,
        routing_provider: RoutingProvider | None = None,
    ):
        self.message_bus = message_bus
        self._redis = redis_client
        self.pod_id = pod_id
        self.station_client = station_client

        self.status = PodStatus.IDLE
        self._available_requests: list[dict] = []
        self.decision: Optional[Decision] = None

        # Concurrency guard for station arrival processing
        self._arrival_lock = asyncio.Lock()

        # Network positioning
        # Default to a generic numeric ID, will be updated during spawn/registration
        self.location_descriptor = LocationDescriptor("station", "1")
        self.route_queue: Deque[EdgeSegment] = deque()
        self.current_segment: Optional[EdgeSegment] = None
        self.segment_progress: float = 0.0
        self.speed: float = 20.0  # m/s

        self.current_route: Route | None = None
        self.movement_start_time: datetime | None = None
        self.estimated_arrival: datetime | None = None

        self.pod_type = self._get_pod_type()
        self._running = False

        # Routing
        if routing_provider:
            self.routing_provider = routing_provider
        else:
            self.routing_provider = RoutingProvider()
            self.routing_provider.add_router(OfflineRouter())

    @property
    def location(self) -> str:
        if self.location_descriptor.location_type == "station":
            return self.location_descriptor.node_id
        return self.location_descriptor.edge_id

    @location.setter
    def location(self, value: str):
        if value is None:
            return
        # Assuming that if a value is not an edge ID, it must be a station ID.
        # This simplifies the logic by removing the explicit 'station_' prefix check.
        # The system should ensure that station IDs and edge IDs are distinct.
        # Edges use "start->end" format, stations are raw numeric IDs
        if "->" not in value:
            self.location_descriptor = LocationDescriptor(
                location_type="station", node_id=value
            )
        else:
            self.location_descriptor = LocationDescriptor(
                location_type="edge", edge_id=value
            )

    def _get_pod_type(self) -> PodType:
        raise NotImplementedError("Subclasses must implement _get_pod_type")

    def get_pod_constraints(self) -> dict[str, Any]:
        cap_used, cap_total, w_used, w_total = self._get_capacity_status()
        return {
            "pod_type": self.pod_type.value,
            "capacity_available": cap_total - cap_used,
            "weight_available": w_total - w_used,
            "max_capacity": cap_total,
            "max_weight": w_total,
            "current_load": {"passengers": cap_used, "cargo_weight": w_used},
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Subscribe to events and begin the movement loop."""
        self._running = True
        await self._setup_subscriptions()
        await self._publish_state_snapshot()
        logger.info(f"Pod {self.pod_id} started")

    async def stop(self):
        self._running = False
        await self._cleanup_subscriptions()
        logger.info(f"Pod {self.pod_id} stopped")

    async def run_movement_loop(self):
        """Self-managed 10 Hz movement simulation loop.

        Each pod process runs this independently — no centralized tick.
        """
        target_interval = 0.1
        loop = asyncio.get_running_loop()
        last_time = loop.time()

        while self._running:
            try:
                now = loop.time()
                dt = min(now - last_time, 1.0)  # Cap to prevent warp on lag
                last_time = now
                await self.update(dt)
                await asyncio.sleep(target_interval)
            except Exception as e:
                logger.debug(f"Pod {self.pod_id} movement error: {e}", exc_info=True)
                await asyncio.sleep(target_interval)

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    async def _setup_subscriptions(self):
        self.message_bus.subscribe(
            MessageBus.CHANNELS["POD_COMMANDS"], self._handle_command
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["SYSTEM_EVENTS"], self._handle_system_event
        )

    async def _cleanup_subscriptions(self):
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["POD_COMMANDS"], self._handle_command
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["SYSTEM_EVENTS"], self._handle_system_event
        )

    async def _handle_command(self, data: dict):
        try:
            command_type = data.get("message", {}).get("command_type", "")
            target = data.get("message", {}).get("target", "")
            if target != self.pod_id:
                return
            if command_type == "AssignRoute":
                await self._handle_route_assignment(data)
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} command error: {e}", exc_info=True
            )

    async def _handle_system_event(self, data: dict):
        try:
            event_type = data.get("message", {}).get("event_type", "")
            if event_type == "CongestionAlert":
                await self._handle_congestion_alert(data)
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} event error: {e}", exc_info=True
            )

    async def _handle_congestion_alert(self, data: dict):
        try:
            alert_data = data.get("message", {}).get("data", {})
            affected_routes = alert_data.get("affected_routes", [])
            if not self.current_route or not self.current_route.stations:
                return
            current_route_str = "->".join(self.current_route.stations)
            if any(route in current_route_str for route in affected_routes):
                logger.info(f"Pod {self.pod_id} route affected by congestion")
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} congestion handling error: {e}",
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Route assignment
    # ------------------------------------------------------------------

    async def _handle_route_assignment(self, data: dict):
        try:
            msg = data.get("message", {})
            params = msg.get("parameters", {})
            route_data = params.get("route") or msg.get("route", [])

            if isinstance(route_data, list):
                route_obj = Route(
                    route_id=f"route_{datetime.now().timestamp()}",
                    stations=route_data,
                    estimated_duration=len(route_data) * 5,
                )
                self.current_route = route_obj
            elif isinstance(route_data, dict):
                required = {"route_id", "stations", "estimated_duration"}
                if not required.issubset(route_data.keys()):
                    logger.error(f"Invalid route: missing {required - route_data.keys()}")
                    return
                self.current_route = Route(
                    route_id=route_data["route_id"],
                    stations=route_data["stations"],
                    estimated_duration=route_data["estimated_duration"],
                )
            else:
                logger.error(f"Invalid route data type: {type(route_data)}")
                return

            if self.current_route and self.current_route.stations:
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
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} route assignment error: {e}", exc_info=True
            )

    async def _hydrate_route(self, stations: list[str]) -> bool:
        """Convert station IDs into a queue of EdgeSegments."""
        from aexis.core.network import NetworkContext

        network = NetworkContext.get_instance()

        if self.current_segment and stations and self.current_segment.end_node == stations[0]:
            self.route_queue.clear()
        else:
            self.route_queue.clear()
            self.current_segment = None
            self.segment_progress = 0.0

        if len(stations) < 2:
            return True

        for i in range(len(stations) - 1):
            start, end = stations[i], stations[i + 1]
            edge_id = f"{start}->{end}"
            if edge_id in network.edges:
                self.route_queue.append(network.edges[edge_id])
            else:
                if start in network.station_positions and end in network.station_positions:
                    logger.warning(f"Pod {self.pod_id} synthesising edge {edge_id}")
                    p1 = network.station_positions[start]
                    p2 = network.station_positions[end]
                    self.route_queue.append(
                        EdgeSegment(
                            segment_id=edge_id,
                            start_node=start,
                            end_node=end,
                            start_coord=Coordinate(p1[0], p1[1]),
                            end_coord=Coordinate(p2[0], p2[1]),
                        )
                    )
                else:
                    logger.error(
                        f"Pod {self.pod_id} hydration failed: unknown station in {edge_id}"
                    )
                    self.route_queue.clear()
                    self.current_segment = None
                    return False

        if self.route_queue:
            self.current_segment = self.route_queue.popleft()
            self.segment_progress = 0.0

        return True


    async def update(self, dt: float) -> bool:
        """Advance pod physics for a single time step."""
        if self.status != PodStatus.EN_ROUTE or not self.current_segment:
            return False

        dist_to_travel = min(self.speed * dt, 100.0)

        while dist_to_travel > 0:
            if not self.current_segment:
                await self._handle_route_completion()
                return True

            remaining = self.current_segment.length - self.segment_progress
            if dist_to_travel >= remaining:
                dist_to_travel -= remaining
                self._advance_segment()
                if not self.current_segment:
                    await self._handle_route_completion()
                    return True
            else:
                self.segment_progress += dist_to_travel
                dist_to_travel = 0

        self._update_location_descriptor()
        await self._publish_position_update()
        return False

    def _advance_segment(self):
        if self.route_queue:
            self.current_segment = self.route_queue.popleft()
            self.segment_progress = 0.0
        else:
            self.current_segment = None

    def _update_location_descriptor(self):
        if not self.current_segment:
            return
        coord = self.current_segment.get_point_at_distance(self.segment_progress)
        self.location_descriptor = LocationDescriptor(
            location_type="edge",
            edge_id=self.current_segment.segment_id,
            coordinate=coord,
            distance_on_edge=self.segment_progress,
        )

    async def _handle_route_completion(self):
        if not (self.current_route and self.current_route.stations):
            self.status = PodStatus.IDLE
            self.segment_progress = 0.0
            await self._publish_position_update()
            await self._publish_status_update()
            return

        final_station = self.current_route.stations[-1]
        from aexis.core.network import NetworkContext

        nc = NetworkContext.get_instance()
        pos = nc.station_positions.get(final_station, (0, 0))
        self.location_descriptor = LocationDescriptor(
            location_type="station",
            node_id=final_station,
            coordinate=Coordinate(pos[0], pos[1]),
        )
        await self._publish_position_update()
        
        # Clear current_route BEFORE arrival handler, so if the handler
        # triggers a new decision, the new route isn't immediately destroyed.
        self.current_route = None
        self.segment_progress = 0.0
        
        await self._handle_station_arrival(final_station)

        await self._publish_status_update()
        await self._publish_state_snapshot()
        logger.info(f"Pod {self.pod_id} arrived at {final_station}")

    async def _handle_station_arrival(self, station_id: str):
        """Override in subclasses for pickup/delivery."""
        pass


    async def make_decision(self):
        try:
            context = await self._build_decision_context()
            route = await self.routing_provider.route(context)
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
            await self._execute_decision(decision)
            await self._publish_decision_event(decision)
        except ValueError as e:
            logger.error(
                f"Pod {self.pod_id}: routing failure (all strategies exhausted) — {e}"
            )
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id} decision error: {e}", exc_info=True
            )

    async def _build_decision_context(self) -> DecisionContext:
        raise NotImplementedError

    async def _execute_decision(self, decision: Decision):
        if not decision:
            return
            
        await self._hydrate_route(decision.route)

        if self.current_segment or self.route_queue:
            self.current_route = Route(
                route_id=f"rt_{datetime.now().timestamp()}",
                stations=decision.route,
                estimated_duration=decision.estimated_duration,
            )
            self.status = PodStatus.EN_ROUTE
            self.movement_start_time = datetime.now(UTC)
        else:
            self.current_route = None
            self.status = PodStatus.IDLE

        # Publish state immediately so the snapshot reflects EN_ROUTE/IDLE
        # before any pickup/delivery logic changes it to LOADING/UNLOADING.
        await self._publish_state_snapshot()

        await self._setup_pickup_delivery_routes(decision.route)

        if self.location_descriptor.location_type == "station":
            station_id = self.location_descriptor.node_id
            await self._execute_pickup(station_id)
            await self._execute_delivery(station_id)

        logger.info(f"Pod {self.pod_id} executing decision: {decision.route}")
        await self._publish_state_snapshot()

    async def _setup_pickup_delivery_routes(self, stations: list[str]):
        pass

    async def _execute_pickup(self, station_id: str):
        pass

    async def _execute_delivery(self, station_id: str):
        pass

    async def _handle_request_broadcast(self, request: dict):
        pass

    # ------------------------------------------------------------------
    # Event publishing
    # ------------------------------------------------------------------

    async def _publish_event(self, event):
        event.source = self.pod_id
        channel = MessageBus.get_event_channel(event.event_type)
        await self.message_bus.publish_event(channel, event)

    async def _publish_decision_event(self, decision: Decision):
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
        await self._publish_event(event)

    async def _publish_status_update(self):
        cap_used, cap_total, w_used, w_total = self._get_capacity_status()
        event = PodStatusUpdate(
            pod_id=self.pod_id,
            location=(
                self.location_descriptor.node_id
                if self.location_descriptor.location_type == "station"
                else self.location_descriptor.edge_id
            ),
            status=self.status.value,
            capacity_used=cap_used,
            capacity_total=cap_total,
            weight_used=w_used,
            weight_total=w_total,
            current_route=self.current_route,
        )
        await self._publish_event(event)

    async def _publish_position_update(self):
        event = PodPositionUpdate(
            pod_id=self.pod_id,
            location=self.location_descriptor,
            status=self.status.value,
            speed=self.speed if self.status == PodStatus.EN_ROUTE else 0.0,
            current_route=(
                self.current_route.stations if self.current_route else None
            ),
        )
        await self._publish_event(event)

    async def _publish_state_snapshot(self):
        state = self.get_state()
        try:
            await self._redis.set(
                _state_key(self.pod_id),
                json.dumps(
                    state,
                    default=lambda o: o.isoformat()
                    if hasattr(o, "isoformat")
                    else str(o),
                ),
            )
        except Exception as e:
            logger.error(f"Pod {self.pod_id}: failed to publish state: {e}")

    def _get_capacity_status(self):
        return 0, 0, 0.0, 0.0

    def get_state(self) -> dict:
        cap_used, cap_total, w_used, w_total = self._get_capacity_status()
        if self.location_descriptor.location_type == "station":
            location_str = self.location_descriptor.node_id
        else:
            location_str = (
                f"on edge {self.location_descriptor.edge_id} "
                f"@ {self.segment_progress:.1f}m"
            )
        return {
            "pod_id": self.pod_id,
            "pod_type": self._get_pod_type().value,
            "status": self.status.value,
            "location": location_str,
            "coordinate": {
                "x": self.location_descriptor.coordinate.x,
                "y": self.location_descriptor.coordinate.y,
            },
            "spine_id": self.location_descriptor.edge_id,
            "distance": self.segment_progress,
            "capacity": {"used": cap_used, "total": cap_total},
            "weight": {"used": w_used, "total": w_total},
            "current_route": (
                list(self.current_route.stations) if self.current_route else []
            ),
            "estimated_arrival": (
                self.estimated_arrival.isoformat()
                if self.estimated_arrival
                else None
            ),
        }

class PassengerPod(Pod):
    """Pod specialised for passenger transport."""

    def __init__(
        self,
        message_bus: MessageBus,
        redis_client: Redis,
        pod_id: str,
        station_client: StationClient,
        routing_provider: RoutingProvider | None = None,
    ):
        super().__init__(
            message_bus, redis_client, pod_id, station_client, routing_provider
        )
        self.capacity = 19
        self.passengers: list[dict] = []
        self.pickup_route: list[str] = []
        self.delivery_route: list[str] = []
        self._decision_pending: bool = False
        self._decision_task: asyncio.Task | None = None

    def _get_pod_type(self) -> PodType:
        return PodType.PASSENGER

    async def _setup_subscriptions(self):
        await super()._setup_subscriptions()
        # Subscribe directly to passenger events for broadcast claims
        self.message_bus.subscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"],
            self._handle_passenger_event,
        )

    async def _cleanup_subscriptions(self):
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"],
            self._handle_passenger_event,
        )
        await super()._cleanup_subscriptions()

    async def _handle_passenger_event(self, data: dict):
        """React to PassengerArrival events — attempt to claim and route."""
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            if event_type != "PassengerArrival":
                return
            event_data = message.get("data", {}) or message
            station_id = event_data.get("station_id")
            request = {
                "type": "passenger",
                "passenger_id": event_data.get("passenger_id", ""),
                "origin": station_id,
                "destination": event_data.get("destination", ""),
                "priority": event_data.get("priority"),
            }
            await self._handle_request_broadcast(request)
        except Exception as e:
            logger.debug(
                f"Pod {self.pod_id} passenger event error: {e}", exc_info=True
            )

    async def _handle_request_broadcast(self, request: dict):
        try:
            if request.get("type") != "passenger":
                return
            remaining = self.capacity - len(self.passengers)
            if remaining <= 0:
                return

            origin = request.get("origin")
            passenger_id = request.get("passenger_id", "")
            if not origin or not passenger_id:
                return

            # Eligibility check
            eligible = False
            if self.status == PodStatus.IDLE:
                eligible = True
            elif self.status == PodStatus.EN_ROUTE and self.current_route:
                if origin in self.current_route.stations:
                    eligible = True
                elif origin in self.delivery_route:
                    eligible = True
            if not eligible:
                return

            # Distance-proportional claiming delay.
            # Closer pods claim first; distant pods find the claim already taken.
            # Docked pods (distance=0) claim immediately.
            at_station = (
                self.location_descriptor.location_type == "station" and
                self.location_descriptor.node_id == origin
            )
            if not at_station:
                from aexis.core.network import NetworkContext
                try:
                    nc = NetworkContext.get_instance()
                    current_station = (
                        self.location_descriptor.node_id
                        if self.location_descriptor.location_type == "station"
                        else (self.current_segment.end_node if self.current_segment else origin)
                    )
                    distance = nc.calculate_distance(current_station, origin)
                except Exception:
                    distance = 100.0  # fallback to a moderate delay
                # Scale: ~0.2s per 100 distance units, minimum 50ms
                delay = max(0.05, distance / 500.0)
                logger.debug(
                    f"Pod {self.pod_id} (remote, dist={distance:.0f}) "
                    f"delaying claim for {passenger_id} at {origin} by {delay:.3f}s"
                )
                await asyncio.sleep(delay)

            # Atomic claim via Redis
            if not await self.station_client.claim_passenger(
                origin, passenger_id, self.pod_id
            ):
                return

            logger.info(
                f"Pod {self.pod_id}: claimed passenger {passenger_id} at {origin}"
            )
            self._available_requests.append(request)
            # Debounce: schedule a single deferred decision instead of re-planning
            # on every individual claim. Multiple claims within the window are
            # batched into one make_decision() call.
            self._schedule_deferred_decision()
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id}: broadcast error: {e}", exc_info=True
            )

    def _schedule_deferred_decision(self):
        """Schedule a single make_decision() call after a 300ms debounce window.
        Multiple claims arriving within the window are batched into one decision."""
        if self._decision_pending:
            return  # already scheduled, new claims will be picked up
        self._decision_pending = True
        self._decision_task = asyncio.ensure_future(self._deferred_decision())

    async def _deferred_decision(self):
        """Wait for the debounce window, then execute a single make_decision()."""
        try:
            await asyncio.sleep(0.3)  # 300ms batching window
            self._decision_pending = False
            await self.make_decision()
        except asyncio.CancelledError:
            self._decision_pending = False
        except Exception as e:
            self._decision_pending = False
            logger.error(
                f"Pod {self.pod_id}: deferred decision error: {e}",
                exc_info=True,
            )

    async def _handle_station_arrival(self, station_id: str):
        if self._arrival_lock.locked():
            logger.warning(
                f"Pod {self.pod_id} ignoring concurrent arrival at {station_id}"
            )
            return

        async with self._arrival_lock:
            self.status = PodStatus.IDLE
            event = PodArrival(pod_id=self.pod_id, station_id=station_id)
            await self._publish_event(event)
            await self._execute_delivery(station_id)
            await self._execute_pickup(station_id)
            await self.make_decision()

    async def _execute_pickup(self, station_id: str):
        remaining = self.capacity - len(self.passengers)
        if remaining <= 0:
            return

        pickups: list[dict] = []

        # 1. Board passengers pre-claimed by this pod
        claimed = await self.station_client.get_claimed_passengers(
            station_id, self.pod_id
        )
        for p in claimed:
            pid = p.get("passenger_id")
            if any(e.get("passenger_id") == pid for e in self.passengers):
                continue
            pickups.append(p)
            self._available_requests = [
                r
                for r in self._available_requests
                if r.get("passenger_id") != pid
            ]

        # 2. Opportunistically claim unclaimed passengers
        extra_capacity = remaining - len(pickups)
        if extra_capacity > 0:
            pending = await self.station_client.get_pending_passengers(station_id)
            for p in pending[:extra_capacity]:
                pid = p.get("passenger_id")
                if any(e.get("passenger_id") == pid for e in self.passengers):
                    continue
                if await self.station_client.claim_passenger(
                    station_id, pid, self.pod_id
                ):
                    pickups.append(p)
                    self._available_requests = [
                        r
                        for r in self._available_requests
                        if r.get("passenger_id") != pid
                    ]

        if not pickups:
            return

        self.status = PodStatus.LOADING
        await self._publish_status_update()
        await self._publish_state_snapshot()
        await asyncio.sleep(len(pickups) * 5)  # 5s per passenger

        for p in pickups:
            passenger = {
                "passenger_id": p.get("passenger_id"),
                "destination": p.get("destination"),
                "pickup_time": datetime.now(UTC),
            }
            self.passengers.append(passenger)
            event = PassengerPickedUp(
                passenger_id=passenger["passenger_id"],
                pod_id=self.pod_id,
                station_id=station_id,
                pickup_time=passenger["pickup_time"],
            )
            await self._publish_event(event)

        self.status = PodStatus.EN_ROUTE

        # Re-broadcast unclaimed passengers left at the station so other pods
        # in the fleet can discover and claim them. Without this, overflow
        # passengers are silently stranded when a pod fills to capacity.
        await self._rebroadcast_unclaimed_passengers(station_id)

    async def _rebroadcast_unclaimed_passengers(self, station_id: str):
        """Check for unclaimed passengers remaining at station_id and re-publish
        PassengerArrival events for each one. This handles the case where a pod
        fills to capacity and leaves overflow passengers with no active broadcast
        to trigger other pods."""
        try:
            remaining = await self.station_client.get_pending_passengers(station_id)
            if not remaining:
                return
            logger.info(
                f"Pod {self.pod_id}: re-broadcasting {len(remaining)} unclaimed "
                f"passengers at station {station_id}"
            )
            for p in remaining:
                event = PassengerArrival(
                    passenger_id=p.get("passenger_id", ""),
                    station_id=station_id,
                    destination=p.get("destination", ""),
                )
                await self._publish_event(event)
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id}: rebroadcast error at {station_id}: {e}",
                exc_info=True,
            )

    async def _execute_delivery(self, station_id: str):
        delivered = [
            p for p in self.passengers if p.get("destination") == station_id
        ]
        if not delivered:
            return

        self.status = PodStatus.UNLOADING
        await self._publish_status_update()
        await self._publish_state_snapshot()
        await asyncio.sleep(len(delivered) * 5)

        for passenger in delivered:
            self.passengers.remove(passenger)
            travel_time = 0
            if "pickup_time" in passenger:
                travel_time = int(
                    (datetime.now(UTC) - passenger["pickup_time"]).total_seconds()
                )
            event = PassengerDelivered(
                passenger_id=passenger.get("passenger_id", ""),
                pod_id=self.pod_id,
                station_id=station_id,
                delivery_time=datetime.now(UTC),
                total_travel_time=travel_time,
                satisfaction_score=0.9,
            )
            await self._publish_event(event)

        self.status = PodStatus.EN_ROUTE
        logger.info(
            f"Pod {self.pod_id} delivered {len(delivered)} passengers at {station_id}"
        )

    async def _setup_pickup_delivery_routes(self, stations: list[str]):
        if stations:
            self.pickup_route = [stations[0]]
            self.delivery_route = stations[1:] if len(stations) > 1 else []

    async def _build_decision_context(self) -> DecisionContext:
        constraints = self.get_pod_constraints()
        if self.location_descriptor.location_type == "station":
            current_location = self.location_descriptor.node_id
        elif self.current_segment:
            current_location = self.current_segment.end_node
        else:
            from aexis.core.network import NetworkContext

            network = NetworkContext.get_instance()
            current_location = network.get_nearest_station(
                self.location_descriptor.coordinate
            )

        # Rebuild available requests from ground truth (Redis) each cycle.
        # This prevents stale demand from accumulating in _available_requests
        # and causing phantom routing after all passengers are delivered.
        self._available_requests.clear()
        onboard_ids = {p.get("passenger_id") for p in self.passengers}
        try:
            station_ids = await self.station_client.get_all_station_ids()
            for sid in station_ids:
                # 1. Truly unclaimed passengers — available for any pod to grab
                pending = await self.station_client.get_pending_passengers(sid)
                for p in pending:
                    self._available_requests.append({
                        "type": "passenger",
                        "passenger_id": p.get("passenger_id", ""),
                        "origin": sid,
                        "destination": p.get("destination", ""),
                        "priority": p.get("priority", 1),
                    })

                # 2. Passengers claimed by THIS pod but not yet onboard.
                #    These need to stay in available_requests so the routing
                #    provider directs the pod toward their pickup station.
                claimed = await self.station_client.get_claimed_passengers(
                    sid, self.pod_id
                )
                for p in claimed:
                    pid = p.get("passenger_id", "")
                    if pid not in onboard_ids:
                        self._available_requests.append({
                            "type": "passenger",
                            "passenger_id": pid,
                            "origin": sid,
                            "destination": p.get("destination", ""),
                            "priority": p.get("priority", 1),
                        })
        except Exception as e:
            logger.error(f"Pod {self.pod_id} failed to fetch global demand: {e}")

        return DecisionContext(
            pod_id=self.pod_id,
            current_location=current_location,
            current_route=self.current_route,
            capacity_available=self.capacity - len(self.passengers),
            weight_available=0.0,
            available_requests=self._available_requests,
            network_state={},
            system_metrics={},
            pod_type=self.pod_type.value,
            pod_constraints=constraints,
            specialization="passenger_transport",
            passengers=list(self.passengers),
            cargo=[],
        )

    def _get_capacity_status(self):
        return len(self.passengers), self.capacity, 0.0, 0.0

    def get_state(self) -> dict:
        state = super().get_state()
        state["passengers"] = self.passengers
        return state


class CargoPod(Pod):
    """Pod specialised for cargo transport."""

    def __init__(
        self,
        message_bus: MessageBus,
        redis_client: Redis,
        pod_id: str,
        station_client: StationClient,
        routing_provider: RoutingProvider | None = None,
    ):
        super().__init__(
            message_bus, redis_client, pod_id, station_client, routing_provider
        )
        self.weight_capacity = 500.0
        self.current_weight = 0.0
        self.cargo: list[dict] = []
        self.pickup_route: list[str] = []
        self.delivery_route: list[str] = []

    def _get_pod_type(self) -> PodType:
        return PodType.CARGO

    async def _setup_subscriptions(self):
        await super()._setup_subscriptions()
        self.message_bus.subscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"],
            self._handle_cargo_event,
        )

    async def _cleanup_subscriptions(self):
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"],
            self._handle_cargo_event,
        )
        await super()._cleanup_subscriptions()

    async def _handle_cargo_event(self, data: dict):
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            if event_type != "CargoRequest":
                return
            event_data = message.get("data", {}) or message
            origin = event_data.get("origin")
            request = {
                "type": "cargo",
                "request_id": event_data.get("request_id", ""),
                "origin": origin,
                "destination": event_data.get("destination", ""),
                "weight": event_data.get("weight", 0.0),
                "priority": event_data.get("priority"),
            }
            await self._handle_request_broadcast(request)
        except Exception as e:
            logger.debug(
                f"Pod {self.pod_id} cargo event error: {e}", exc_info=True
            )

    async def _handle_request_broadcast(self, request: dict):
        try:
            if request.get("type") != "cargo":
                return
            req_weight = float(request.get("weight", 0.0) or 0.0)
            remaining = self.weight_capacity - self.current_weight
            if remaining < req_weight:
                return

            origin = request.get("origin")
            request_id = request.get("request_id", "")
            if not origin or not request_id:
                return

            eligible = False
            if self.status == PodStatus.IDLE:
                eligible = True
            elif self.status == PodStatus.EN_ROUTE and self.current_route:
                if origin in self.current_route.stations:
                    eligible = True
                elif origin in self.delivery_route:
                    eligible = True
            if not eligible:
                return

            # Proximity-Based Claiming Delay (Bug #1 Fix for Cargo)
            at_station = (
                self.location_descriptor.location_type == "station" and
                self.location_descriptor.node_id == origin
            )
            if not at_station:
                from aexis.core.network import NetworkContext
                network = NetworkContext.get_instance()
                distance = network.calculate_distance(
                    self.location_descriptor.node_id, origin
                )
                delay_seconds = max(0.05, distance / 500.0)
                logger.debug(
                    f"Pod {self.pod_id} (remote, dist={distance:.0f}) delaying "
                    f"claim for cargo {request_id} at {origin} by {delay_seconds:.3f}s"
                )
                await asyncio.sleep(delay_seconds)

            if not await self.station_client.claim_cargo(
                origin, request_id, self.pod_id
            ):
                return

            logger.info(
                f"Pod {self.pod_id}: claimed cargo {request_id} at {origin}"
            )
            self._available_requests.append(request)
            # Bug #3 Fix: Use debounce instead of immediate decision
            self._schedule_deferred_decision()
        except Exception as e:
            logger.error(
                f"Pod {self.pod_id}: broadcast error: {e}", exc_info=True
            )

    def _schedule_deferred_decision(self):
        """Schedule a single make_decision call to batch rapidly incoming claims."""
        if not getattr(self, "_decision_pending", False):
            self._decision_pending = True
            
            # Cancel any existing task just in case
            if hasattr(self, "_decision_task") and self._decision_task and not self._decision_task.done():
                self._decision_task.cancel()
                
            self._decision_task = asyncio.create_task(self._deferred_decision())

    async def _deferred_decision(self):
        """Wait out the debounce window, then compute the route once."""
        try:
            await asyncio.sleep(0.3)  # 300ms debounce window
            self._decision_pending = False
            await self.make_decision()
        except asyncio.CancelledError:
            self._decision_pending = False

    async def _handle_station_arrival(self, station_id: str):
        if self._arrival_lock.locked():
            return

        async with self._arrival_lock:
            self.status = PodStatus.IDLE
            self.location_descriptor = LocationDescriptor(
                location_type="station",
                node_id=station_id,
                coordinate=self.location_descriptor.coordinate,
            )
            event = PodArrival(pod_id=self.pod_id, station_id=station_id)
            await self._publish_event(event)
            await asyncio.sleep(2.0)  # Docking time
            await self._execute_delivery(station_id)
            await self._execute_pickup(station_id)
            await self.make_decision()

    async def _execute_pickup(self, station_id: str):
        remaining = self.weight_capacity - self.current_weight
        if remaining <= 0:
            return

        loaded_count = 0
        loaded_weight = 0.0

        # 1. Load pre-claimed cargo
        claimed = await self.station_client.get_claimed_cargo(
            station_id, self.pod_id
        )
        for c in claimed:
            req_weight = float(c.get("weight", 0.0) or 0.0)
            if req_weight <= 0 or self.current_weight + loaded_weight + req_weight > self.weight_capacity:
                continue
            req_id = c.get("request_id", "")
            if any(e.get("request_id") == req_id for e in self.cargo):
                continue
            cargo_item = {
                "request_id": req_id,
                "destination": c.get("destination", ""),
                "weight": req_weight,
                "pickup_time": datetime.now(UTC),
            }
            self.cargo.append(cargo_item)
            loaded_count += 1
            loaded_weight += req_weight
            event = CargoLoaded(
                request_id=req_id,
                pod_id=self.pod_id,
                station_id=station_id,
                load_time=cargo_item["pickup_time"],
            )
            await self._publish_event(event)
            self._available_requests = [
                r for r in self._available_requests if r.get("request_id") != req_id
            ]

        # 2. Opportunistically load unclaimed cargo
        pending = await self.station_client.get_pending_cargo(station_id)
        for c in pending:
            req_weight = float(c.get("weight", 0.0) or 0.0)
            if req_weight <= 0 or self.current_weight + loaded_weight + req_weight > self.weight_capacity:
                continue
            req_id = c.get("request_id", "")
            if any(e.get("request_id") == req_id for e in self.cargo):
                continue
            if not await self.station_client.claim_cargo(
                station_id, req_id, self.pod_id
            ):
                continue
            cargo_item = {
                "request_id": req_id,
                "destination": c.get("destination", ""),
                "weight": req_weight,
                "pickup_time": datetime.now(UTC),
            }
            self.cargo.append(cargo_item)
            loaded_count += 1
            loaded_weight += req_weight
            event = CargoLoaded(
                request_id=req_id,
                pod_id=self.pod_id,
                station_id=station_id,
                load_time=cargo_item["pickup_time"],
            )
            await self._publish_event(event)
            self._available_requests = [
                r for r in self._available_requests if r.get("request_id") != req_id
            ]

        if loaded_count == 0:
            return

        self.status = PodStatus.LOADING
        await self._publish_status_update()
        self.current_weight += loaded_weight
        loading_time = max(1.0, (loaded_weight / 100.0) * 10.0)
        await asyncio.sleep(loading_time)
        self.status = PodStatus.EN_ROUTE
        await self._publish_state_snapshot()
        logger.info(
            f"Pod {self.pod_id} loaded {loaded_count} cargo "
            f"({loaded_weight:.1f}kg) at {station_id}"
        )

    async def _execute_delivery(self, station_id: str):
        delivered = [c for c in self.cargo if c.get("destination") == station_id]
        if not delivered:
            return

        self.status = PodStatus.UNLOADING
        await self._publish_status_update()
        await asyncio.sleep(len(delivered) * 10)

        for cargo in delivered:
            self.cargo.remove(cargo)
            self.current_weight -= cargo.get("weight", 0)
            event = CargoDelivered(
                request_id=cargo.get("request_id", ""),
                pod_id=self.pod_id,
                station_id=station_id,
                delivery_time=datetime.now(UTC),
                condition="intact",
                on_time=True,
            )
            await self._publish_event(event)

        self.status = PodStatus.EN_ROUTE
        logger.info(
            f"Pod {self.pod_id} delivered {len(delivered)} cargo at {station_id}"
        )

    async def _setup_pickup_delivery_routes(self, stations: list[str]):
        if stations:
            self.pickup_route = [stations[0]]
            self.delivery_route = stations[1:] if len(stations) > 1 else []

    async def _build_decision_context(self) -> DecisionContext:
        constraints = self.get_pod_constraints()
        if self.location_descriptor.location_type == "station":
            current_location = self.location_descriptor.node_id
        elif self.current_segment:
            current_location = self.current_segment.end_node
        else:
            from aexis.core.network import NetworkContext

            network = NetworkContext.get_instance()
            current_location = network.get_nearest_station(
                self.location_descriptor.coordinate
            )

        # Bug #2 Fix: Clear stale requests to prevent phantom oscillation
        self._available_requests.clear()
        
        onboard_ids = {c.get("request_id") for c in self.cargo}
        try:
            station_ids = await self.station_client.get_all_station_ids()
            for sid in station_ids:
                # 1. Truly unclaimed cargo
                pending = await self.station_client.get_pending_cargo(sid)
                for c in pending:
                    self._available_requests.append({
                        "type": "cargo",
                        "request_id": c.get("request_id", ""),
                        "origin": sid,
                        "destination": c.get("destination", ""),
                        "weight": c.get("weight", 0.0),
                        "priority": c.get("priority", 1),
                    })
                    
                # 2. Cargo claimed by THIS pod but not yet onboard
                claimed = await self.station_client.get_claimed_cargo(
                    sid, self.pod_id
                )
                for c in claimed:
                    rid = c.get("request_id", "")
                    if rid not in onboard_ids:
                        self._available_requests.append({
                            "type": "cargo",
                            "request_id": rid,
                            "origin": sid,
                            "destination": c.get("destination", ""),
                            "weight": c.get("weight", 0.0),
                            "priority": c.get("priority", 1),
                        })
        except Exception as e:
            logger.error(f"Pod {self.pod_id} failed to fetch global cargo demand: {e}")

        return DecisionContext(
            pod_id=self.pod_id,
            current_location=current_location,
            current_route=self.current_route,
            capacity_available=0,
            weight_available=self.weight_capacity - self.current_weight,
            available_requests=self._available_requests,
            network_state={},
            system_metrics={},
            pod_type=self.pod_type.value,
            pod_constraints=constraints,
            specialization="cargo_transport",
            passengers=[],
            cargo=list(self.cargo),
        )

    def _get_capacity_status(self):
        return 0, 0, self.current_weight, self.weight_capacity

    def get_state(self) -> dict:
        state = super().get_state()
        state["cargo"] = self.cargo
        return state
