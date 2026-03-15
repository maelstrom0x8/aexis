"""Station service — runs as a standalone process managing passenger and cargo queues.

Each station maintains its queue state in Redis hashes and reacts to events via
Redis pub/sub. Claims are handled atomically so multiple pod processes can race
safely.

Redis key layout (owned by this station instance):
  aexis:station:{id}:passengers        — HASH: passenger_id → JSON
  aexis:station:{id}:cargo             — HASH: request_id → JSON
  aexis:station:{id}:claims:passengers — HASH: passenger_id → pod_id
  aexis:station:{id}:claims:cargo      — HASH: request_id → pod_id
  aexis:station:{id}:state             — STRING: JSON state snapshot
"""

import asyncio
import json
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from redis.asyncio import Redis

from aexis.core.message_bus import MessageBus
from aexis.core.model import (
    CargoRequest,
    CongestionAlert,
    PassengerArrival,
    Priority,
    StationStatus,
)

logger = logging.getLogger(__name__)


def _passengers_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:passengers"


def _cargo_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:cargo"


def _passenger_claims_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:claims:passengers"


def _cargo_claims_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:claims:cargo"


def _state_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:state"


# Lua script: atomically claim an item only if it exists in the queue and is unclaimed.
_CLAIM_LUA = """
local queue_key = KEYS[1]
local claims_key = KEYS[2]
local item_id = ARGV[1]
local pod_id = ARGV[2]

if redis.call('HEXISTS', queue_key, item_id) == 0 then
    return 0
end
return redis.call('HSETNX', claims_key, item_id, pod_id)
"""


class Station:
    """Transportation station with Redis-backed queue management.

    Subscribes to passenger/cargo/pod/system events via the MessageBus and
    writes all queue state to Redis hashes so pod processes in other
    OS processes can query and claim items through StationClient.
    """

    def __init__(
        self,
        message_bus: MessageBus,
        redis_client: Redis,
        station_id: str,
    ):
        self.message_bus = message_bus
        self._redis = redis_client
        self.station_id = station_id

        self.status = StationStatus.OPERATIONAL
        self.loading_bays = 4
        self.available_bays = 4
        self.processing_rate = 2.5  # passengers/minute
        self.connected_stations: list[str] = []
        self.congestion_level = 0.0

        # Metrics
        self.total_passengers_processed = 0
        self.total_cargo_processed = 0
        self.average_wait_time = 0.0
        self.max_wait_time = 0.0

        # Cached local counts for congestion calculations (avoid round-tripping
        # to Redis on every event just for a length check).
        self._passenger_count = 0
        self._cargo_count = 0

        self._claim_script = self._redis.register_script(_CLAIM_LUA)
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Subscribe to event channels and begin processing."""
        self._running = True
        await self._setup_subscriptions()
        # Publish initial state so the API can discover this station
        await self._publish_state_snapshot()
        logger.info(f"Station {self.station_id} started")

    async def stop(self):
        """Unsubscribe and clean up."""
        self._running = False
        await self._cleanup_subscriptions()
        logger.info(f"Station {self.station_id} stopped")

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    async def _setup_subscriptions(self):
        self.message_bus.subscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"], self._handle_passenger_event
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"], self._handle_cargo_event
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["POD_EVENTS"], self._handle_pod_event
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["SYSTEM_COMMANDS"], self._handle_system_command
        )

    async def _cleanup_subscriptions(self):
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"], self._handle_passenger_event
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"], self._handle_cargo_event
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["POD_EVENTS"], self._handle_pod_event
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["SYSTEM_COMMANDS"], self._handle_system_command
        )

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _handle_passenger_event(self, data: dict):
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            event_data = message.get("data", {}) or message

            if event_type == "PassengerArrival":
                await self._handle_passenger_arrival(event_data)
            elif event_type == "PassengerPickedUp":
                await self._handle_passenger_pickup(event_data)
            elif event_type == "PassengerDelivered":
                await self._handle_passenger_delivery(event_data)
        except Exception as e:
            logger.debug(
                f"Station {self.station_id} passenger event error: {e}",
                exc_info=True,
            )

    async def _handle_cargo_event(self, data: dict):
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            event_data = message.get("data", {}) or message

            if event_type == "CargoRequest":
                await self._handle_cargo_request(event_data)
            elif event_type == "CargoLoaded":
                await self._handle_cargo_loading(event_data)
            elif event_type == "CargoDelivered":
                await self._handle_cargo_delivery(event_data)
        except Exception as e:
            logger.debug(
                f"Station {self.station_id} cargo event error: {e}",
                exc_info=True,
            )

    async def _handle_pod_event(self, data: dict):
        """Manage loading bay availability based on pod arrivals/departures."""
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")

            if event_type == "PodArrival":
                station_id = message.get("station_id", "")
                if station_id == self.station_id:
                    self.available_bays = max(0, self.available_bays - 1)
                    self._update_congestion_level()
                    logger.debug(
                        f"Station {self.station_id}: pod docked, "
                        f"bays {self.available_bays}/{self.loading_bays}"
                    )
            elif event_type == "PodStatusUpdate":
                location = message.get("location", "")
                status = message.get("status", "")
                if location == self.station_id and status == "en_route":
                    self.available_bays = min(
                        self.loading_bays, self.available_bays + 1
                    )
                    self._update_congestion_level()
        except Exception as e:
            logger.debug(
                f"Station {self.station_id} pod event error: {e}", exc_info=True
            )

    async def _handle_system_command(self, data: dict):
        try:
            command_type = data.get("message", {}).get("command_type", "")
            target = data.get("message", {}).get("target", "")
            if target != self.station_id:
                return
            if command_type == "UpdateCapacity":
                await self._handle_capacity_update(data)
        except Exception as e:
            logger.debug(
                f"Station {self.station_id} command error: {e}", exc_info=True
            )

    # ------------------------------------------------------------------
    # Passenger operations
    # ------------------------------------------------------------------

    async def _handle_passenger_arrival(self, event_data: dict):
        station_id = event_data.get("station_id")
        if station_id != self.station_id:
            return

        passenger_id = event_data.get("passenger_id")
        if not passenger_id:
            return

        passenger = {
            "passenger_id": passenger_id,
            "destination": event_data.get("destination"),
            "priority": event_data.get("priority", Priority.NORMAL.value),
            "group_size": event_data.get("group_size", 1),
            "special_needs": event_data.get("special_needs", []),
            "arrival_time": datetime.now(UTC).isoformat(),
            "wait_time_limit": event_data.get("wait_time_limit", 30),
        }

        await self._redis.hset(
            _passengers_key(self.station_id),
            passenger_id,
            json.dumps(passenger),
        )
        self._passenger_count += 1
        self._update_congestion_level()

        if self.congestion_level > 0.7:
            await self._publish_congestion_alert()

        await self._publish_state_snapshot()

    async def _handle_passenger_pickup(self, event_data: dict):
        station_id = event_data.get("station_id")
        if station_id != self.station_id:
            return

        passenger_id = event_data.get("passenger_id")
        if not passenger_id:
            return

        # Remove from queue and claims
        await self._redis.hdel(_passengers_key(self.station_id), passenger_id)
        await self._redis.hdel(
            _passenger_claims_key(self.station_id), passenger_id
        )
        self._passenger_count = max(0, self._passenger_count - 1)
        self.total_passengers_processed += 1
        self._update_congestion_level()
        await self._publish_state_snapshot()

    async def _handle_passenger_delivery(self, event_data: dict):
        """Passengers delivered TO this station — just update metrics."""
        station_id = event_data.get("station_id")
        if station_id != self.station_id:
            return
        # Delivery at this station is informational; no queue mutation needed.

    # ------------------------------------------------------------------
    # Cargo operations
    # ------------------------------------------------------------------

    async def _handle_cargo_request(self, event_data: dict):
        origin = event_data.get("origin")
        if origin != self.station_id:
            return

        request_id = event_data.get("request_id")
        if not request_id:
            return

        cargo = {
            "request_id": request_id,
            "destination": event_data.get("destination"),
            "weight": event_data.get("weight", 0.0),
            "volume": event_data.get("volume", 0.0),
            "priority": event_data.get("priority", Priority.NORMAL.value),
            "hazardous": event_data.get("hazardous", False),
            "temperature_controlled": event_data.get(
                "temperature_controlled", False
            ),
            "deadline": event_data.get("deadline"),
            "arrival_time": datetime.now(UTC).isoformat(),
        }

        await self._redis.hset(
            _cargo_key(self.station_id), request_id, json.dumps(cargo)
        )
        self._cargo_count += 1
        self._update_congestion_level()

        if self.congestion_level > 0.7:
            await self._publish_congestion_alert()

        logger.info(
            f"Station {self.station_id}: Cargo {request_id} added "
            f"(queue size: {self._cargo_count})"
        )
        await self._publish_state_snapshot()

    async def _handle_cargo_loading(self, event_data: dict):
        station_id = event_data.get("station_id")
        if station_id != self.station_id:
            return

        request_id = event_data.get("request_id")
        if not request_id:
            return

        await self._redis.hdel(_cargo_key(self.station_id), request_id)
        await self._redis.hdel(_cargo_claims_key(self.station_id), request_id)
        self._cargo_count = max(0, self._cargo_count - 1)
        self.total_cargo_processed += 1
        self._update_congestion_level()
        await self._publish_state_snapshot()

    async def _handle_cargo_delivery(self, event_data: dict):
        station_id = event_data.get("station_id")
        if station_id != self.station_id:
            return
        # Delivery at this station is informational.

    # ------------------------------------------------------------------
    # Capacity / congestion
    # ------------------------------------------------------------------

    async def _handle_capacity_update(self, data: dict):
        try:
            parameters = data.get("message", {}).get("parameters", {})
            self.loading_bays = parameters.get("max_pods", self.loading_bays)
            self.processing_rate = parameters.get(
                "processing_rate", self.processing_rate
            )
            logger.info(
                f"Station {self.station_id}: Capacity updated — "
                f"bays: {self.loading_bays}, rate: {self.processing_rate}"
            )
            await self._publish_state_snapshot()
        except Exception as e:
            logger.debug(
                f"Station {self.station_id} capacity update error: {e}",
                exc_info=True,
            )

    def _update_congestion_level(self):
        passenger_congestion = min(1.0, self._passenger_count / 20.0)
        cargo_congestion = min(1.0, self._cargo_count / 10.0)
        bay_utilization = 1.0 - (self.available_bays / max(1, self.loading_bays))
        self.congestion_level = (
            passenger_congestion * 0.4
            + cargo_congestion * 0.3
            + bay_utilization * 0.3
        )
        if self.congestion_level > 0.8:
            self.status = StationStatus.CONGESTED
        elif self.congestion_level < 0.3:
            self.status = StationStatus.OPERATIONAL

    async def _publish_congestion_alert(self):
        if self.congestion_level < 0.7:
            return

        affected_routes = [
            f"{self.station_id}->{s}" for s in self.connected_stations
        ]
        total_items = self._passenger_count + self._cargo_count
        estimated_clear_minutes = (
            total_items / self.processing_rate if self.processing_rate > 0 else 0
        )
        estimated_clear_time = datetime.now(UTC) + timedelta(
            minutes=estimated_clear_minutes
        )

        if self.congestion_level > 0.9:
            severity = "critical"
        elif self.congestion_level > 0.8:
            severity = "high"
        else:
            severity = "medium"

        alert = CongestionAlert(
            station_id=self.station_id,
            congestion_level=self.congestion_level,
            queue_length=total_items,
            average_wait_time=self.average_wait_time,
            affected_routes=affected_routes,
            estimated_clear_time=estimated_clear_time,
            severity=severity,
        )
        channel = MessageBus.get_event_channel(alert.event_type)
        await self.message_bus.publish_event(channel, alert)
        logger.warning(
            f"Station {self.station_id}: Congestion alert — "
            f"level {self.congestion_level:.2f}"
        )

    # ------------------------------------------------------------------
    # State snapshot
    # ------------------------------------------------------------------

    async def _publish_state_snapshot(self):
        """Write full station state to a Redis key for API/UI reads."""
        state = self.get_state()
        try:
            await self._redis.set(
                _state_key(self.station_id),
                json.dumps(
                    state,
                    default=lambda o: o.isoformat()
                    if hasattr(o, "isoformat")
                    else str(o),
                ),
            )
        except Exception as e:
            logger.error(
                f"Station {self.station_id}: failed to publish state: {e}"
            )

    def get_state(self) -> dict[str, Any]:
        return {
            "station_id": self.station_id,
            "status": self.status.value,
            "congestion_level": self.congestion_level,
            "queues": {
                "passengers": {
                    "waiting": self._passenger_count,
                    "average_wait_time": self.average_wait_time,
                    "max_wait_time": self.max_wait_time,
                },
                "cargo": {
                    "waiting": self._cargo_count,
                    "average_wait_time": self.average_wait_time,
                },
            },
            "resources": {
                "loading_bays": {
                    "available": self.available_bays,
                    "total": self.loading_bays,
                },
                "processing_rate": self.processing_rate,
            },
            "metrics": {
                "total_passengers_processed": self.total_passengers_processed,
                "total_cargo_processed": self.total_cargo_processed,
            },
            "connected_stations": self.connected_stations,
        }


# ========================================================================
# Payload generators (station-scoped)
# ========================================================================


class PassengerGenerator:
    """Generates random passenger arrivals for simulation."""

    def __init__(self, message_bus: MessageBus, stations: list[str]):
        self.message_bus = message_bus
        self.stations = stations
        self.running = False
        self.generation_rate = 0.5  # probability per station per cycle

    async def start(self):
        self.running = True
        logger.info("Started passenger generator")
        while self.running:
            await self._generate_passengers()
            await asyncio.sleep(120)

    async def stop(self):
        self.running = False
        logger.info("Stopped passenger generator")

    async def _generate_passengers(self):
        for station in self.stations:
            if random.random() < self.generation_rate:
                count = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
                for _ in range(count):
                    await self._create_passenger(station)

    async def _create_passenger(self, origin: str):
        destinations = [s for s in self.stations if s != origin]
        if not destinations:
            return
        destination = random.choice(destinations)
        priority_weights = {1: 0.1, 2: 0.7, 3: 0.15, 4: 0.04, 5: 0.01}
        priority = random.choices(
            list(priority_weights.keys()),
            weights=list(priority_weights.values()),
        )[0]
        group_size = random.choices(
            [1, 2, 3, 4], weights=[0.6, 0.25, 0.1, 0.05]
        )[0]
        special_needs = []
        if random.random() < 0.1:
            special_needs.append("wheelchair")

        passenger_id = (
            f"p_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_"
            f"{random.randint(1000, 9999)}"
        )
        event = PassengerArrival(
            passenger_id=passenger_id,
            station_id=origin,
            destination=destination,
            priority=priority,
            group_size=group_size,
            special_needs=special_needs,
            wait_time_limit=random.randint(15, 45),
        )
        await self.message_bus.publish_event(
            MessageBus.get_event_channel(event.event_type), event
        )

    def create_manual_event(
        self, passenger_id: str, origin: str, dest: str
    ) -> PassengerArrival:
        return PassengerArrival(
            passenger_id=passenger_id,
            station_id=origin,
            destination=dest,
            priority=3,
            group_size=1,
            special_needs=[],
            wait_time_limit=45,
        )


class CargoGenerator:
    """Generates random cargo requests for simulation."""

    def __init__(self, message_bus: MessageBus, stations: list[str]):
        self.message_bus = message_bus
        self.stations = stations
        self.running = False
        self.generation_rate = 0.3

    async def start(self):
        self.running = True
        logger.info("Started cargo generator")
        while self.running:
            await self._generate_cargo()
            await asyncio.sleep(180)

    async def stop(self):
        self.running = False
        logger.info("Stopped cargo generator")

    async def _generate_cargo(self):
        for station in self.stations:
            if random.random() < self.generation_rate:
                await self._create_cargo_request(station)

    async def _create_cargo_request(self, origin: str):
        destinations = [s for s in self.stations if s != origin]
        if not destinations:
            return
        destination = random.choice(destinations)
        weight = random.choices(
            [10, 25, 50, 100, 200], weights=[0.3, 0.3, 0.2, 0.15, 0.05]
        )[0]
        volume = weight / 500.0
        priority_weights = {1: 0.2, 2: 0.6, 3: 0.15, 4: 0.04, 5: 0.01}
        priority = random.choices(
            list(priority_weights.keys()),
            weights=list(priority_weights.values()),
        )[0]
        hazardous = random.random() < 0.05
        temperature_controlled = random.random() < 0.15
        deadline = None
        if random.random() < 0.3:
            deadline = datetime.now(UTC) + timedelta(
                hours=random.randint(2, 24)
            )

        request_id = (
            f"c_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_"
            f"{random.randint(1000, 9999)}"
        )
        event = CargoRequest(
            request_id=request_id,
            origin=origin,
            destination=destination,
            weight=weight,
            volume=volume,
            priority=priority,
            hazardous=hazardous,
            temperature_controlled=temperature_controlled,
            deadline=deadline,
        )
        await self.message_bus.publish_event(
            MessageBus.get_event_channel(event.event_type), event
        )

    def create_manual_event(
        self, request_id: str, origin: str, dest: str, weight: float
    ) -> CargoRequest:
        return CargoRequest(
            request_id=request_id,
            origin=origin,
            destination=dest,
            weight=weight,
            volume=weight / 500.0,
            priority=3,
            hazardous=False,
            temperature_controlled=False,
            deadline=None,
        )
