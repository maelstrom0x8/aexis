import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta

from .message_bus import EventProcessor, MessageBus
from .model import (
    CargoRequest,
    CongestionAlert,
    PassengerArrival,
    Priority,
    StationStatus,
)

logger = logging.getLogger(__name__)


class Station(EventProcessor):
    """Transportation station with event-driven processing"""

    def __init__(self, message_bus: MessageBus, station_id: str):
        super().__init__(message_bus, station_id)
        self.station_id = station_id
        self.status = StationStatus.OPERATIONAL
        self.passenger_queue = []
        self.cargo_queue = []
        self.loading_bays = 4
        self.available_bays = 4
        self.processing_rate = 2.5  # passengers/minute
        self.connected_stations = []
        self.congestion_level = 0.0
        self.queue_history = []

        # Metrics
        self.total_passengers_processed = 0
        self.total_cargo_processed = 0
        self.average_wait_time = 0.0
        self.max_wait_time = 0.0

    async def _setup_subscriptions(self):
        """Subscribe to relevant channels"""
        self.message_bus.subscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"], self._handle_passenger_event
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["POD_EVENTS"], self._handle_pod_event)
        self.message_bus.subscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"], self._handle_cargo_event
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["SYSTEM_COMMANDS"], self._handle_system_command
        )

    async def _cleanup_subscriptions(self):
        """Unsubscribe from channels"""
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"], self._handle_passenger_event
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["POD_EVENTS"], self._handle_pod_event)
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"], self._handle_cargo_event
        )
        self.message_bus.unsubscribe(
            MessageBus.CHANNELS["SYSTEM_COMMANDS"], self._handle_system_command
        )

    async def _handle_pod_event(self, data: dict):
        """Handle pod-related events (e.g. arrivals, departures)"""
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            event_data = message.get("data", {})
            if not event_data:
                event_data = message

            # from the PodPositionUpdate event, we can determine if a pod has arrived at or departed from the station, and update available bays accordingly
            # if the pod position update indicates the pod is now at the station, we can assume it has arrived and decrease available bays by 1.

        except Exception as e:
            logger.debug(
                f"Station {self.station_id} pod event error: {e}", exc_info=True
            )


    async def _handle_passenger_event(self, data: dict):
        """Handle passenger-related events"""
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            logger.debug(f"Station {self.station_id} received event {event_type}")
            # Robustly get event data - might be in 'data' field or top-level entries
            event_data = message.get("data", {})
            if not event_data:
                event_data = message

            if event_type == "PassengerArrival":
                await self._handle_passenger_arrival(event_data)
            elif event_type == "PassengerPickedUp":
                await self._handle_passenger_pickup(event_data)
            elif event_type == "PassengerDelivered":
                await self._handle_passenger_delivery(event_data)

        except Exception as e:
            logger.debug(
                f"Station {self.station_id} passenger event error: {e}", exc_info=True
            )

    async def _handle_cargo_event(self, data: dict):
        """Handle cargo-related events"""
        try:
            message = data.get("message", {})
            event_type = message.get("event_type", "")
            event_data = message.get("data", {})
            if not event_data:
                event_data = message

            if event_type == "CargoRequest":
                await self._handle_cargo_request(event_data)
            elif event_type == "CargoLoaded":
                await self._handle_cargo_loading(event_data)
            elif event_type == "CargoDelivered":
                await self._handle_cargo_delivery(event_data)

        except Exception as e:
            logger.debug(
                f"Station {self.station_id} cargo event error: {e}", exc_info=True
            )

    async def _handle_system_command(self, data: dict):
        """Handle system commands"""
        try:
            command_type = data.get("message", {}).get("command_type", "")
            target = data.get("message", {}).get("target", "")

            if target != self.station_id:
                return

            if command_type == "UpdateCapacity":
                await self._handle_capacity_update(data)

        except Exception as e:
            logger.debug(
                f"Station {self.station_id} command error: {e}", exc_info=True)

    async def _handle_passenger_arrival(self, event_data: dict):
        """Handle new passenger arrival"""
        passenger_id = event_data.get("passenger_id")
        station_id = event_data.get("station_id")

        if station_id != self.station_id:
            return

        # Add to queue
        passenger = {
            "passenger_id": passenger_id,
            "destination": event_data.get("destination"),
            "priority": event_data.get("priority", Priority.NORMAL.value),
            "group_size": event_data.get("group_size", 1),
            "special_needs": event_data.get("special_needs", []),
            "arrival_time": datetime.now(UTC),
            "wait_time_limit": event_data.get("wait_time_limit", 30),
        }

        self.passenger_queue.append(passenger)
        self._update_congestion_level()

        # logger.warning(
        #     f"Station {self.station_id}: Passenger {passenger_id} added to queue (queue size: {len(self.passenger_queue)})"
        # )

        # Check if congestion alert is needed
        if self.congestion_level > 0.7:
            await self._publish_congestion_alert()

    async def _handle_cargo_request(self, event_data: dict):
        """Handle new cargo request"""
        request_id = event_data.get("request_id")
        origin = event_data.get("origin")

        if origin != self.station_id:
            return

        # Add to queue
        cargo = {
            "request_id": request_id,
            "destination": event_data.get("destination"),
            "weight": event_data.get("weight", 0.0),
            "volume": event_data.get("volume", 0.0),
            "priority": event_data.get("priority", Priority.NORMAL.value),
            "hazardous": event_data.get("hazardous", False),
            "temperature_controlled": event_data.get("temperature_controlled", False),
            "deadline": event_data.get("deadline"),
            "arrival_time": datetime.now(UTC),
        }
        # logger.warning(
        #     f"Station {self.station_id}: Received cargo request {request_id} with weight {cargo['weight']} and volume {cargo['volume']}")

        self.cargo_queue.append(cargo)
        self._update_congestion_level()
        # if there are pods in the station, we should trigger the loading process immediately for this new cargo, otherwise it will wait until the next pod arrives and checks the queue

        logger.info(
            f"Station {self.station_id}: Cargo {request_id} added to queue (queue size: {len(self.cargo_queue)})"
        )

        # Check if congestion alert is needed
        if self.congestion_level > 0.7:
            await self._publish_congestion_alert()

    async def _handle_passenger_pickup(self, event_data: dict):
        """Handle passenger pickup by pod"""
        passenger_id = event_data.get("passenger_id")
        station_id = event_data.get("station_id")

        if station_id != self.station_id:
            logger.debug(f"Station {self.station_id} ignoring pickup for station {station_id}")
            return

        # Remove from queue
        logger.debug(f"Station {self.station_id} removing passenger {passenger_id} from queue")
        self.passenger_queue = [
            p for p in self.passenger_queue if p["passenger_id"] != passenger_id
        ]

        self.total_passengers_processed += 1
        self._update_congestion_level()
        self._update_wait_time_metrics()

        # logger.warning(
        #     f"Station {self.station_id}: Passenger {passenger_id} picked up")

    async def _handle_cargo_loading(self, event_data: dict):
        """Handle cargo loading by pod"""
        request_id = event_data.get("request_id")
        station_id = event_data.get("station_id")

        if station_id != self.station_id:
            return

        # Remove from queue
        self.cargo_queue = [
            c for c in self.cargo_queue if c["request_id"] != request_id
        ]

        self.total_cargo_processed += 1
        self._update_congestion_level()

        logger.info(f"Station {self.station_id}: Cargo {request_id} loaded")

    async def _handle_capacity_update(self, data: dict):
        """Handle capacity update command"""
        try:
            parameters = data.get("message", {}).get("parameters", {})

            self.loading_bays = parameters.get("max_pods", self.loading_bays)
            self.processing_rate = parameters.get(
                "processing_rate", self.processing_rate
            )

            logger.info(
                f"Station {self.station_id}: Capacity updated - bays: {self.loading_bays}, rate: {self.processing_rate}"
            )

        except Exception as e:
            logger.debug(
                f"Station {self.station_id} capacity update error: {e}", exc_info=True
            )

    def _update_congestion_level(self):
        """Calculate current congestion level"""
        # Base congestion from queue lengths
        # 20 passengers = full congestion
        passenger_congestion = min(1.0, len(self.passenger_queue) / 20.0)
        # 10 cargo = full congestion
        cargo_congestion = min(1.0, len(self.cargo_queue) / 10.0)

        # Bay utilization
        bay_utilization = 1.0 - (self.available_bays / self.loading_bays)

        # Weighted combination
        self.congestion_level = (
            passenger_congestion * 0.4 + cargo_congestion * 0.3 + bay_utilization * 0.3
        )

        # Update status based on congestion
        if self.congestion_level > 0.8:
            self.status = StationStatus.CONGESTED
        elif self.congestion_level < 0.3:
            self.status = StationStatus.OPERATIONAL

    def _update_wait_time_metrics(self):
        """Update wait time metrics"""
        if not self.passenger_queue:
            return

        current_time = datetime.now(UTC)
        wait_times = []

        for passenger in self.passenger_queue:
            wait_time = (
                # minutes
                current_time - passenger["arrival_time"]
            ).total_seconds() / 60
            wait_times.append(wait_time)

        if wait_times:
            self.average_wait_time = sum(wait_times) / len(wait_times)
            self.max_wait_time = max(wait_times)

    async def _publish_congestion_alert(self):
        """Publish congestion alert if needed"""
        if self.congestion_level < 0.7:
            return

        # Determine affected routes (simplified)
        affected_routes = [
            f"{self.station_id}->{station}" for station in self.connected_stations
        ]

        # Estimate clear time based on processing rate
        total_items = len(self.passenger_queue) + len(self.cargo_queue)
        estimated_clear_minutes = total_items / self.processing_rate
        estimated_clear_time = datetime.now(UTC) + timedelta(
            minutes=estimated_clear_minutes
        )

        # Determine severity
        if self.congestion_level > 0.9:
            severity = "critical"
        elif self.congestion_level > 0.8:
            severity = "high"
        else:
            severity = "medium"

        alert = CongestionAlert(
            station_id=self.station_id,
            congestion_level=self.congestion_level,
            queue_length=len(self.passenger_queue) + len(self.cargo_queue),
            average_wait_time=self.average_wait_time,
            affected_routes=affected_routes,
            estimated_clear_time=estimated_clear_time,
            severity=severity,
        )

        await self.publish_event(alert)
        logger.warning(
            f"Station {self.station_id}: Congestion alert - level {self.congestion_level:.2f}"
        )

    def get_state(self) -> dict:
        """Get current station state"""
        return {
            "station_id": self.station_id,
            "status": self.status.value,
            "congestion_level": self.congestion_level,
            "queues": {
                "passengers": {
                    "waiting": len(self.passenger_queue),
                    "average_wait_time": self.average_wait_time,
                    "max_wait_time": self.max_wait_time,
                },
                "cargo": {
                    "waiting": len(self.cargo_queue),
                    "average_wait_time": self.average_wait_time,  # Simplified
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
    # Query & Claim APIs (for pod live queue access)
    # ========================================================================

    def get_pending_passengers(self, destination: str = None) -> list[dict]:
        """Return passengers still waiting at this station (not claimed by any pod).
        
        Args:
            destination: Optional filter by destination station
            
        Returns:
            List of passenger dicts that are unclaimed
        """
        pending = [p for p in self.passenger_queue if not p.get("claimed_by")]
        if destination:
            pending = [p for p in pending if p.get("destination") == destination]
        return pending

    def get_pending_cargo(self, destination: str = None) -> list[dict]:
        """Return cargo still waiting at this station (not claimed by any pod).
        
        Args:
            destination: Optional filter by destination station
            
        Returns:
            List of cargo dicts that are unclaimed
        """
        pending = [c for c in self.cargo_queue if not c.get("claimed_by")]
        if destination:
            pending = [c for c in pending if c.get("destination") == destination]
        return pending

    def claim_passenger(self, passenger_id: str, pod_id: str) -> bool:
        """Atomically claim a passenger for a pod.
        
        This prevents multiple pods from picking up the same passenger.
        
        Args:
            passenger_id: ID of the passenger to claim
            pod_id: ID of the pod claiming the passenger
            
        Returns:
            True if claim succeeded, False if already claimed or not found
        """
        for p in self.passenger_queue:
            if p.get("passenger_id") == passenger_id:
                if p.get("claimed_by"):
                    logger.debug(f"Station {self.station_id}: Passenger {passenger_id} already claimed by {p['claimed_by']}")
                    return False
                p["claimed_by"] = pod_id
                logger.info(f"Station {self.station_id}: Passenger {passenger_id} claimed by {pod_id}")
                return True
        logger.debug(f"Station {self.station_id}: Passenger {passenger_id} not found in queue")
        return False

    def claim_cargo(self, request_id: str, pod_id: str) -> bool:
        """Atomically claim cargo for a pod.
        
        This prevents multiple pods from loading the same cargo.
        
        Args:
            request_id: ID of the cargo request to claim
            pod_id: ID of the pod claiming the cargo
            
        Returns:
            True if claim succeeded, False if already claimed or not found
        """
        for c in self.cargo_queue:
            if c.get("request_id") == request_id:
                if c.get("claimed_by"):
                    logger.debug(f"Station {self.station_id}: Cargo {request_id} already claimed by {c['claimed_by']}")
                    return False
                c["claimed_by"] = pod_id
                logger.info(f"Station {self.station_id}: Cargo {request_id} claimed by {pod_id}")
                return True
        logger.debug(f"Station {self.station_id}: Cargo {request_id} not found in queue")
        return False


class PassengerGenerator:
    """Generates passenger arrivals for simulation"""

    def __init__(self, message_bus: MessageBus, stations: list[str]):
        self.message_bus = message_bus
        self.stations = stations
        self.running = False
        self.generation_rate = 0.5  # passengers per minute per station

    async def start(self):
        """Start generating passengers"""
        self.running = True
        logger.info("Started passenger generator")

        while self.running:
            await self._generate_passengers()
            await asyncio.sleep(120)  # Generate every 2 minutes

    async def stop(self):
        """Stop generating passengers"""
        self.running = False
        logger.info("Stopped passenger generator")

    async def _generate_passengers(self):
        """Generate random passenger arrivals"""
        for station in self.stations:
            # Poisson-like distribution for arrivals
            num_passengers = 0
            if random.random() < self.generation_rate:
                num_passengers = random.choices(
                    [1, 2, 3], weights=[0.7, 0.25, 0.05])[0]

            for _ in range(num_passengers):
                await self._create_passenger(station)

    async def _create_passenger(self, origin: str):
        """Create a single passenger"""
        # Random destination (not the same as origin)
        destinations = [s for s in self.stations if s != origin]
        destination = random.choice(destinations)

        # Random priority (mostly normal, some high/urgent)
        priority_weights = {1: 0.1, 2: 0.7, 3: 0.15, 4: 0.04, 5: 0.01}
        priority = random.choices(
            list(priority_weights.keys()), weights=list(priority_weights.values())
        )[0]

        # Random group size
        group_size = random.choices([1, 2, 3, 4], weights=[
                                    0.6, 0.25, 0.1, 0.05])[0]

        # Random special needs
        special_needs = []
        if random.random() < 0.1:  # 10% chance
            special_needs.append("wheelchair")

        passenger_id = f"p_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{random.randint(1000, 9999)}"

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

    def _create_manual_event(self, passenger_id: str, origin: str, dest: str):
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
    """Generates cargo requests for simulation"""

    def __init__(self, message_bus: MessageBus, stations: list[str]):
        self.message_bus = message_bus
        self.stations = stations
        self.running = False
        self.generation_rate = 0.3  # cargo requests per minute per station

    async def start(self):
        """Start generating cargo requests"""
        self.running = True
        logger.info("Started cargo generator")

        while self.running:
            await self._generate_cargo()
            await asyncio.sleep(180)  # Generate every 3 minutes

    async def stop(self):
        """Stop generating cargo requests"""
        self.running = False
        logger.info("Stopped cargo generator")

    async def _generate_cargo(self):
        """Generate random cargo requests"""
        for station in self.stations:
            # Poisson-like distribution for requests
            if random.random() < self.generation_rate:
                await self._create_cargo_request(station)

    async def _create_cargo_request(self, origin: str):
        """Create a single cargo request"""
        # Random destination (not the same as origin)
        destinations = [s for s in self.stations if s != origin]
        destination = random.choice(destinations)

        # Random cargo properties
        weight = random.choices(
            [10, 25, 50, 100, 200], weights=[0.3, 0.3, 0.2, 0.15, 0.05]
        )[0]

        volume = weight / 500.0  # Simplified volume calculation

        # Random priority
        priority_weights = {1: 0.2, 2: 0.6, 3: 0.15, 4: 0.04, 5: 0.01}
        priority = random.choices(
            list(priority_weights.keys()), weights=list(priority_weights.values())
        )[0]

        # Random special properties
        hazardous = random.random() < 0.05  # 5% chance
        temperature_controlled = random.random() < 0.15  # 15% chance

        # Random deadline (30% have deadlines)
        deadline = None
        if random.random() < 0.3:
            deadline = datetime.now(datetime.UTC) + timedelta(
                hours=random.randint(2, 24)
            )

        request_id = f"c_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{random.randint(1000, 9999)}"

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

    def _create_manual_event(
        self, request_id: str, origin: str, dest: str, weight: float
    ):
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
