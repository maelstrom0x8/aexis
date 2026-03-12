import asyncio
import json
import logging
import os
import random
from datetime import datetime, UTC
from typing import Any, Mapping

from .ai_provider import AIProviderFactory
from .errors import handle_exception
from .message_bus import LocalMessageBus, MessageBus
from .model import SystemSnapshot
from .network import (
    NetworkContext,
    load_network_data,
)
from .pod import CargoPod, PassengerPod, Pod
from .routing import RoutingProvider
from .station import CargoGenerator, PassengerGenerator, Station

logger = logging.getLogger(__name__)


class AexisConfig:
    """Configuration for Aexis system"""

    def __init__(
            self, debug: bool = False, network_data_path: str | None = None, **kwargs
    ):
        self.debug = debug
        self.network_data_path = network_data_path
        for key, value in kwargs.items():
            setattr(self, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key with optional default.
        Priority: 
        1. Environment Variables (for core settings)
        2. Config file / object attributes
        3. Default value
        """
        # Environment variable overrides for core settings
        env_map = {
            "pods.count": "POD_COUNT",
            "stations.count": "STATION_COUNT",
            "ai.provider": "AI_PROVIDER",
            "pods.cargoPercentage": "CARGO_PERCENTAGE",
            "redis.url": "REDIS_URL"
        }
        if key in env_map:
            env_val = os.getenv(env_map[key])
            if env_val is not None:
                try:
                    if isinstance(default, int):
                        return int(env_val)
                    if isinstance(default, float):
                        return float(env_val)
                    return env_val
                except (ValueError, TypeError):
                    pass

        keys = key.split(".")
        value = self
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                value = getattr(value, k, default)
            if value is default:
                break
        return value

    def to_dict(self) -> dict[str, Any]:
        return {
            "debug": self.debug,
            "network_data_path": self.network_data_path,
            **{
                k: v
                for k, v in self.__dict__.items()
                if k not in ["debug", "network_data_path"]
            },
        }


class SystemContext:
    """Centralized system context managing configuration and network state"""

    _instance = None
    _initialized = False
    _lock = None
    _lock = None

    def __init__(self):
        if not SystemContext._initialized:
            self._config = None
            self._network_context = None
            self._config_path = None
            SystemContext._initialized = True

    @classmethod
    def get_instance(cls) -> 'SystemContext':
        """Get the SystemContext instance, initializing with defaults if necessary."""
        if cls._instance is None:
            # Fallback to automatic initialization with default path
            # Try to find aexis.json in common locations
            config_path = "aexis.json"
            if not os.path.exists(config_path):
                config_path = "aexis/aexis.json"

            cls.initialize_sync(config_path)

        return cls._instance

    @classmethod
    async def initialize(cls, config_path: str = "aexis/aexis.json") -> 'SystemContext':
        """Async wrapper for initialization (maintaining backward compatibility)"""
        return cls.initialize_sync(config_path)

    @classmethod
    def initialize_sync(cls, config_path: str = "aexis/aexis.json") -> 'SystemContext':
        """Synchronous initialization of SystemContext"""
        if cls._instance is None:
            if cls._lock is None:
                import threading
                cls._lock = threading.Lock()

            with cls._lock:
                if cls._instance is None:
                    instance = cls()
                    instance._load_configuration(config_path)
                    cls._instance = instance

        return cls._instance

    @classmethod
    def set_instance(cls, instance: 'SystemContext'):
        """Set the SystemContext instance (for testing)"""
        cls._instance = instance
        cls._initialized = True

    def _load_configuration(self, config_path: str):
        """Load configuration from aexis.json (synchronous)"""
        self._config_path = config_path

        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)

            # Extract configuration section
            config_section = config_data.get('config', {})

            # Initialize AexisConfig with loaded data
            self._config = AexisConfig(
                debug=config_section.get('debug', False),
                network_data_path=config_section.get(
                    'networkDataPath', 'network.json'),
                **{k: v for k, v in config_section.items() if k not in ['debug', 'networkDataPath']}
            )

            # Load network data and initialize NetworkContext
            network_path = self._config.network_data_path
            logger.warning(f"Loading network data from {network_path}")
            network_data = load_network_data(network_path)
            self._network_context = NetworkContext(network_data)
            NetworkContext.set_instance(self._network_context)

            logger.warning(
                f"SystemContext initialized with config: {config_path}")
            for k, v in self._config.to_dict().items():
                print(f"  {k}={v}")

        except FileNotFoundError:
            logger.warning(
                f"Configuration file not found: {config_path}, using defaults")
            self._config = AexisConfig()
            self._network_context = NetworkContext()
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}, using defaults")
            self._config = AexisConfig()
            self._network_context = NetworkContext()

    def get_config(self) -> AexisConfig:
        """Get system configuration"""
        if self._config is None:
            raise RuntimeError(
                "Configuration not loaded. Call await SystemContext.initialize() first.")
        return self._config

    def get_network_context(self) -> NetworkContext:
        """Get network context"""
        if self._network_context is None:
            raise RuntimeError(
                "Network context not initialized. Call await SystemContext.initialize() first.")
        return self._network_context

    def reload_configuration(self) -> bool:
        """Reload configuration from file (synchronous version for compatibility)"""
        try:
            with open(self._config_path, 'r') as f:
                config_data = json.load(f)

            config_section = config_data.get('config', {})

            # Update existing configuration
            for key, value in config_section.items():
                if hasattr(self._config, key):
                    setattr(self._config, key, value)

            logger.info("Configuration reloaded successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return False


class AexisSystem:
    """Main system coordinator for AEXIS transportation network"""

    def __init__(self, system_context: SystemContext = None, message_bus: MessageBus = None):
        # Use provided SystemContext or get the instance
        self.system_context = system_context or SystemContext.get_instance()
        self.config = self.system_context.get_config()
        self.network_context = self.system_context.get_network_context()

        # Initialize message bus with configuration or use injected one
        if message_bus:
            self.message_bus = message_bus
        else:
            redis_url = self.config.get('redis.url', "redis://localhost:6379")
            if redis_url == "local://":
                self.message_bus = LocalMessageBus()
            else:
                self.message_bus = MessageBus(
                    redis_url=redis_url,
                    password=self.config.get('redis.password'),
                )
        self.ai_provider = None
        self.pods: Mapping[str, Pod] = {}
        self.stations = {}
        self.passenger_generator = None
        self.cargo_generator = None
        self.running = False
        self.start_time = None

        # System metrics
        self.metrics = {
            "total_pods": 0,
            "active_pods": 0,
            "total_stations": 0,
            "operational_stations": 0,
            "pending_passengers": 0,
            "pending_cargo": 0,
            "average_wait_time": 0.0,
            "system_efficiency": 0.0,
            "throughput_per_hour": 0,
            "fallback_usage_rate": 0.0,
        }

        # Configuration from SystemContext instead of environment variables
        self.pod_count = self.config.get('pods.count', 4)
        self.cargo_percentage = self.config.get(
            'pods.cargoPercentage', 50)  # 0-100
        self.station_count = self.config.get('stations.count', 8)
        self.snapshot_interval = self.config.get(
            'system.snapshotInterval', 300)  # 5 minutes

    async def initialize(self) -> bool:
        """Initialize system"""
        print(f"Loaded {self.pod_count} pods")
        try:
            # Connect to Redis
            if not await self.message_bus.connect():
                logger.error("Failed to connect to Redis message bus")
                return False

            # Initialize AI provider
            await self._initialize_ai_provider()

            # Create stations
            await self._create_stations()

            # Create pods
            await self._create_pods()

            # Setup generators
            await self._setup_generators()

            # Setup system subscriptions
            await self._setup_subscriptions()

            # Start system monitoring
            self.start_time = datetime.now()

            logger.info(
                f"AEXIS system initialized with {len(self.stations)} stations and {len(self.pods)} pods"
            )
            return True

        except Exception as e:
            error_details = handle_exception(e, "AexisSystem")
            logger.error(
                f"System initialization failed: {error_details.message}")
            return False

    async def _setup_subscriptions(self):
        """Subscribe to system-wide events for reactive behavior"""
        self.message_bus.subscribe(
            MessageBus.CHANNELS["PASSENGER_EVENTS"], self._handle_event
        )
        self.message_bus.subscribe(
            MessageBus.CHANNELS["CARGO_EVENTS"], self._handle_event
        )

    async def _handle_event(self, data: dict):
        """Handle incoming events and trigger reactive actions"""
        try:
            from .model import PodStatus
            message = data.get("message", {})
            event_type = message.get("event_type", "")

            event_data = message.get("data", {})
            if not event_data:
                event_data = message

            seeded_request: dict | None = None
            if event_type == "PassengerArrival":
                seeded_request = {
                    "type": "passenger",
                    "passenger_id": event_data.get("passenger_id", ""),
                    "origin": event_data.get("station_id"),
                    "destination": event_data.get("destination", ""),
                    "priority": event_data.get("priority"),
                }
            elif event_type == "CargoRequest":
                seeded_request = {
                    "type": "cargo",
                    "request_id": event_data.get("request_id", ""),
                    "origin": event_data.get("origin"),
                    "destination": event_data.get("destination", ""),
                    "weight": event_data.get("weight", 0.0),
                    "priority": event_data.get("priority"),
                }

            # React to arrivals by triggering idle pods
            if event_type in ["PassengerArrival", "CargoRequest"]:
                station_id = event_data.get("station_id") or event_data.get("origin")
                # logger.warning(f"Station {station_id} processing event")
                
                # First, prioritize pods already at the station
                if station_id:
                    for pod in self.pods.values():
                        st = pod.status.value 
                        at_station = self._pod_is_at_station(pod, station_id)
                        # logger.warning(f"Pod {pod.pod_id} is {st} and {at_station or 'not'} at station {station_id}")
                        if (pod.status == PodStatus.IDLE or pod.status.value == "idle") and self._pod_is_at_station(pod, station_id):
                            # logger.warning(f"Pod {pod.pod_id} is idle => {pod.status != PodStatus.IDLE}")
                            # logger.warning(f"Prioritizing docked pod {pod.pod_id} at {station_id} for new {event_type}")
                            await self._populate_pod_requests(pod)
                            if seeded_request:
                                if seeded_request.get("type") == "passenger":
                                    pid = seeded_request.get("passenger_id")
                                    if pid and not any(r.get("passenger_id") == pid for r in pod._available_requests):
                                        pod._available_requests.append(seeded_request)
                                elif seeded_request.get("type") == "cargo":
                                    rid = seeded_request.get("request_id")
                                    if rid and not any(r.get("request_id") == rid for r in pod._available_requests):
                                        pod._available_requests.append(seeded_request)
                            if len(pod._available_requests) > 0:
                                await pod.make_decision()
                
                # Then, trigger other idle pods globally if no local pods available
                for pod in self.pods.values():
                    # logger.warning(f"Checking pod {pod.pod_id} for event {event_type} at station {station_id}")
                    if pod.status.value == "idle" and not self._pod_is_at_station(pod, station_id):
                        await self._populate_pod_requests(pod)
                        if seeded_request:
                            if seeded_request.get("type") == "passenger":
                                pid = seeded_request.get("passenger_id")
                                if pid and not any(r.get("passenger_id") == pid for r in pod._available_requests):
                                    pod._available_requests.append(seeded_request)
                            elif seeded_request.get("type") == "cargo":
                                rid = seeded_request.get("request_id")
                                if rid and not any(r.get("request_id") == rid for r in pod._available_requests):
                                    pod._available_requests.append(seeded_request)
                        if len(pod._available_requests) > 0:
                            await pod.make_decision()

        except Exception as e:
            logger.debug(f"AexisSystem event handling error: {e}")

    def _pod_is_at_station(self, pod: Pod, station_id: str) -> bool:
        """Check if pod is currently at the specified station"""
        try:
            # Check if pod's location descriptor indicates it's at the station
            if (pod.location_descriptor.location_type == "station" and 
                pod.location_descriptor.node_id == station_id):
                return True
            
            # Check if pod is on an edge connected to this station
            if (pod.location_descriptor.location_type == "edge" and 
                pod.current_segment):
                # Pod is considered "at" station if it's very close to either end
                edge = pod.current_segment
                if (edge.start_node == station_id or edge.end_node == station_id) and \
                   pod.segment_progress < 5.0:  # Within 5 meters of station
                    return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking pod location: {e}")
            return False

    async def _initialize_ai_provider(self):
        """Initialize AI provider based on configuration"""
        try:
            ai_type = self.config.get('ai.provider', 'mock').lower()

            # The factory handles token validation and provider creation
            self.ai_provider = AIProviderFactory.create_provider(ai_type)
            logger.info(f"{ai_type.title()} AI provider initialized")

        except Exception as e:
            logger.error(f"AI provider initialization failed: {e}")
            # Fallback to mock provider for resilience in dev/demo
            self.ai_provider = AIProviderFactory.create_provider("mock")
            logger.info("Fallback to mock AI provider")

    async def start(self):
        """Start the system"""
        if not await self.initialize():
            return False

        self.running = True

        # Start message bus listening
        message_bus_task = asyncio.create_task(
            self.message_bus.start_listening())

        # Start all stations
        station_tasks = []
        for station in self.stations.values():
            station_tasks.append(asyncio.create_task(station.start()))

        # Start all pods
        pod_tasks = []
        for pod in self.pods.values():
            pod_tasks.append(asyncio.create_task(pod.start()))

        # Start generators
        # generator_tasks = []
        # if self.passenger_generator:
        #     generator_tasks.append(
        #         asyncio.create_task(self.passenger_generator.start())
        #     )
        # if self.cargo_generator:
        #     generator_tasks.append(asyncio.create_task(
        #         self.cargo_generator.start()))

        # Start system monitoring
        monitor_task = asyncio.create_task(self._system_monitor())

        # Start periodic decision making
        decision_task = asyncio.create_task(self._periodic_decision_making())

        # Start pod movement simulation (Phase 1)
        movement_task = asyncio.create_task(self._simulate_pod_movement())

        # Setup subscriptions for reactive behavior AFTER components are started
        await self._setup_subscriptions()

        logger.info("AEXIS system started")

        try:
            # Wait for all tasks
            await asyncio.gather(
                message_bus_task,
                *station_tasks,
                *pod_tasks,
                # *generator_tasks,
                monitor_task,
                decision_task,
                movement_task,
                return_exceptions=True,
            )
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        finally:
            await self.shutdown()

        # Stop AI Provider
        if self.ai_provider:
            await self.ai_provider.close()

        logger.info("AEXIS system shutdown complete")

    async def _create_stations(self):
        """Create station instances from network.json nodes using SystemContext"""
        # NetworkContext is already initialized in SystemContext
        network_data = None

        # Try to get network data from the already initialized NetworkContext
        if hasattr(self.network_context, 'network_graph') and self.network_context.network_graph.nodes():
            # Extract network data from NetworkContext for station creation
            network_data = {"nodes": []}
            for node_id in self.network_context.network_graph.nodes():
                # Extract node ID from station_XXX format
                if node_id.startswith("station_"):
                    station_num = node_id[8:]  # Remove "station_" prefix
                    pos = self.network_context.station_positions.get(
                        node_id, {"x": 0, "y": 0})

                    # Get adjacency information
                    adj = []
                    if self.network_context.network_graph.has_node(node_id):
                        for neighbor in self.network_context.network_graph.neighbors(node_id):
                            if neighbor.startswith("station_"):
                                neighbor_num = neighbor[8:]
                                weight = self.network_context.network_graph[node_id][neighbor].get(
                                    'weight', 1.0)
                                adj.append(
                                    {"node_id": neighbor_num, "weight": weight})

                    network_data["nodes"].append({
                        "id": station_num,
                        "label": station_num,
                        "coordinate": {"x": pos[0], "y": pos[1]},
                        "adj": adj
                    })

        if not network_data or "nodes" not in network_data:
            logger.warning(
                "No network data found in SystemContext, falling back to generated stations")
            # Fallback to old behavior
            station_ids = [f"station_{i:03d}" for i in range(
                1, self.station_count + 1)]
            for station_id in station_ids:
                station = Station(self.message_bus, station_id)
                station.connected_stations = self._get_connected_stations(
                    station_id, station_ids
                )
                self.stations[station_id] = station
            return

        # Build stations from network nodes
        nodes = network_data["nodes"]

        # First pass: create all stations
        for node in nodes:
            # Use 3-digit padding for numeric IDs for backward compatibility (e.g. station_001)
            raw_id = node['id']
            try:
                station_num = int(raw_id)
                station_id = f"station_{station_num:03d}"
            except (ValueError, TypeError):
                station_id = f"station_{raw_id}"
                
            station = Station(self.message_bus, station_id)
            logger.debug(f"Loaded station: {station_id}")

            # Store coordinate for potential future use
            station.coordinate = node.get("coordinate", {"x": 0, "y": 0})

            self.stations[station_id] = station

        # Second pass: set connected stations from adjacency
        for node in nodes:
            station_id = f"station_{node['id']}"
            station = self.stations.get(station_id)
            if station:
                connected = []
                for adj in node.get("adj", []):
                    raw_adj_id = adj['node_id']
                    try:
                        adj_num = int(raw_adj_id)
                        connected_station_id = f"station_{adj_num:03d}"
                    except (ValueError, TypeError):
                        connected_station_id = f"station_{raw_adj_id}"
                        
                    if connected_station_id in self.stations:
                        connected.append(connected_station_id)
                station.connected_stations = connected
                logger.info(
                    f"Created station: {station_id} connected to {connected}")

        # Update station_count to reflect actual count
        self.station_count = len(self.stations)

    async def _create_pods(self):
        """Create pod instances with network positioning on edges

        PHASE 1: Pods spawn at random positions on network edges
        Pod types (passenger/cargo) determined by cargoPercentage from config

        Precondition: Network must have at least one edge (spine/path)
        """
        # Validate network has edges before spawning
        if not self.network_context.edges:
            logger.error(
                "Cannot spawn pods: network has no edges. "
                "Verify network.json has stations with adjacencies."
            )
            return

        logging.warning(f"Spawning {self.pod_count} pods")
        for i in range(1, self.pod_count + 1):
            pod_id = f"pod_{i:03d}"

            # Create routing provider for this pod
            # Pod layer is agnostic: it doesn't know if routing uses AI or offline strategies
            # The routing provider transparently handles both
            routing_provider = self._create_routing_provider(pod_id)

            # Determine pod type based on config percentage
            # If cargoPercentage=50, first 50% are cargo, rest are passenger
            pod_index_percentage = ((i - 1) / self.pod_count) * 100
            is_cargo = pod_index_percentage < self.cargo_percentage

            if is_cargo:
                pod = CargoPod(self.message_bus, pod_id, routing_provider, stations=self.stations)
            else:
                pod = PassengerPod(self.message_bus, pod_id, routing_provider, stations=self.stations)

            # PHASE 1: Spawn pod at random network edge
            # spawn_pod_at_random_edge() returns (edge_id, coordinate, distance) where edge_id is always an edge
            edge_id, coordinate, distance_on_edge = self.network_context.spawn_pod_at_random_edge()

            # Retrieve the actual EdgeSegment object
            edge_segment = self.network_context.edges.get(edge_id)

            if not edge_segment:
                # This should never happen if spawn_pod_at_random_edge() works correctly
                logger.error(
                    f"Pod {pod_id}: spawned on edge {edge_id} but edge not found in network context"
                )
                continue

            # Set up pod for edge-based movement
            from .model import LocationDescriptor, PodStatus

            pod.current_segment = edge_segment
            pod.segment_progress = distance_on_edge

            # Mark as en route so movement simulation will update it
            pod.status = PodStatus.EN_ROUTE

            # Set location descriptor with precise position
            pod.location_descriptor = LocationDescriptor(
                location_type="edge",
                edge_id=edge_id,
                coordinate=coordinate,
                distance_on_edge=distance_on_edge
            )

            self.pods[pod_id] = pod
            pod_type = "Cargo" if is_cargo else "Passenger"

            logger.info(
                f"Created {pod_type}Pod: {pod_id} on edge {edge_id} "
                f"at position ({coordinate.x:.1f}, {coordinate.y:.1f})"
            )

            await pod.make_decision()

            # Publish initial position update for UI rendering
            # Use asyncio.create_task to fire-and-forget without blocking pod creation
            asyncio.create_task(pod._publish_position_update())

    def _create_routing_provider(self, pod_id: str) -> RoutingProvider:
        """Create a configured routing provider for a pod using SystemContext

        Returns a RoutingProvider with:
        - Offline routing as primary strategy
        - AI routing as fallback (if AI provider available)

        This encapsulates the routing strategy configuration so pods don't need to know about it.
        """
        routing_provider = RoutingProvider()

        # Always add offline router as primary
        # Pass the NetworkContext from SystemContext to avoid singleton access
        from .routing import OfflineRouter
        offline_router = OfflineRouter(self.network_context)
        routing_provider.add_router(offline_router)

        # Add AI router as fallback if AI provider is available
        if self.ai_provider:
            from .routing import AIRouter, OfflineRoutingStrategy
            ai_router = AIRouter(pod_id, self.ai_provider,
                                 OfflineRoutingStrategy(self.network_context))
            routing_provider.add_router(ai_router)

        return routing_provider

    async def _setup_generators(self):
        """Setup passenger and cargo generators"""
        station_ids = list(self.stations.keys())

        self.passenger_generator = PassengerGenerator(
            self.message_bus, station_ids)
        self.cargo_generator = CargoGenerator(self.message_bus, station_ids)

        logger.info("Setup passenger and cargo generators")

    def _get_connected_stations(
            self, station_id: str, all_stations: list[str]
    ) -> list[str]:
        """Get connected stations creating a complex mesh topology"""
        station_index = all_stations.index(station_id)
        total_stations = len(all_stations)
        connected = []

        # 1. Ring connections (Next and Prev)
        next_index = (station_index + 1) % total_stations
        prev_index = (station_index - 1) % total_stations
        connected.append(all_stations[next_index])
        connected.append(all_stations[prev_index])

        # 2. 'Hub' connections (Every 4th station is a hub connected to many)
        if station_index % 4 == 0:
            # Connect to other hubs
            for i in range(0, total_stations, 4):
                if i != station_index:
                    connected.append(all_stations[i])
        else:
            # Connect to nearest hub
            nearest_hub_idx = (station_index // 4) * 4
            if (
                    all_stations[nearest_hub_idx] not in connected
                    and nearest_hub_idx != station_index
            ):
                connected.append(all_stations[nearest_hub_idx])

        # 3. Random 'Shortcut' (deterministic based on ID to remain consistent)
        import hashlib

        hash_val = int(hashlib.md5(
            f"{station_id}_salt".encode()).hexdigest(), 16)
        random_offset = (hash_val % (total_stations - 3)) + \
                        2  # Avoid self, prev, next
        random_idx = (station_index + random_offset) % total_stations
        target = all_stations[random_idx]
        if target not in connected and target != station_id:
            connected.append(target)

        return list(set(connected))  # Deduplicate

    async def _system_monitor(self):
        """Monitor system metrics and publish snapshots"""
        while self.running:
            try:
                # Update metrics
                await self._update_metrics()

                # Publish periodic snapshot
                await self._publish_snapshot()

                # Log system status
                await self._log_system_status()

                await asyncio.sleep(self.snapshot_interval)

            except Exception as e:
                logger.debug(f"System monitor error: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait before retrying

    async def _periodic_decision_making(self):
        """Trigger periodic decision making for pods"""
        while self.running:
            try:
                # Trigger decision making for idle pods
                for pod in self.pods.values():
                    if pod.status.value == "idle":
                        # Populate available requests from queues before decision
                        await self._populate_pod_requests(pod)
                        await pod.make_decision()

                # Wait before next round
                await asyncio.sleep(30)  # Every 30 seconds

            except Exception as e:
                logger.debug(
                    f"Periodic decision making error: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _populate_pod_requests(self, pod: Pod):
        """Populate available requests in pod's decision context
        
        Gathers waiting passengers/cargo from all stations and
        makes them available for the pod's routing decision.
        """
        try:
            from .pod import PassengerPod, CargoPod
            from .model import Priority

            available_requests = []

            if isinstance(pod, PassengerPod):
                # Gather passenger requests from all stations
                for station_id, station in self.stations.items():
                    for passenger in station.passenger_queue:
                        available_requests.append({
                            "type": "passenger",
                            "passenger_id": passenger.get("passenger_id", ""),
                            "origin": station_id,
                            "destination": passenger.get("destination", ""),
                            "priority": passenger.get("priority", Priority.NORMAL.value),
                            "wait_time": (datetime.now(UTC) - passenger.get("arrival_time",
                                                                            datetime.now(UTC))).total_seconds()
                        })

            elif isinstance(pod, CargoPod):
                # Gather cargo requests from all stations
                for station_id, station in self.stations.items():
                    for cargo in station.cargo_queue:
                        available_requests.append({
                            "type": "cargo",
                            "request_id": cargo.get("request_id", ""),
                            "origin": station_id,
                            "destination": cargo.get("destination", ""),
                            "weight": cargo.get("weight", 0.0),
                            "priority": cargo.get("priority", Priority.NORMAL.value),
                            "wait_time": (datetime.now(UTC) - cargo.get("arrival_time",
                                                                        datetime.now(UTC))).total_seconds()
                        })

            # Store in pod's context (will be used by router)
            pod._available_requests = available_requests

        except Exception as e:
            logger.debug(f"Error populating requests for {pod.pod_id}: {e}")

    async def _simulate_pod_movement_once(self, dt: float):
        """Perform a single simulation step (used for testing)"""
        for pod in self.pods.values():
            await pod.update(dt)

    async def shutdown(self):
        """Shutdown the system"""
        self.running = False

        logger.info("Shutting down AEXIS system...")

        # Stop generators
        if self.passenger_generator:
            await self.passenger_generator.stop()
        if self.cargo_generator:
            await self.cargo_generator.stop()

        # Stop all pods
        for pod in self.pods.values():
            await pod.stop()

        # Stop all stations
        for station in self.stations.values():
            await station.stop()

        # Stop message bus
        await self.message_bus.stop_listening()
        await self.message_bus.disconnect()

        # Stop AI Provider
        if self.ai_provider:
            await self.ai_provider.close()

        logger.info("AEXIS system shutdown complete")

    async def _simulate_pod_movement(self):
        """Simulate pod movement with continuous path integration

        Uses precise delta-time calculation to ensure smooth, speed-consistent movement
        regardless of the actual update frequency (server lag resilience).
        """
        target_interval = 0.1  # Target 10 Hz
        loop = asyncio.get_running_loop()
        last_time = loop.time()

        while self.running:
            try:
                now = loop.time()
                dt = now - last_time
                last_time = now

                # Cap dt to avoid massive jumps if thread hangs (e.g. max 1.0s)
                dt = min(dt, 1.0)

                # Update all pods
                # In Phase 2, this could be parallelized if pod count > 1000
                for pod in self.pods.values():
                    await pod.update(dt)

                # Sleep strict remainder to maintain roughly target rate
                # processing_time = loop.time() - now
                # sleep_time = max(0.01, target_interval - processing_time)

                # Simple sleep is fine for now, dt handles the physics correctness
                await asyncio.sleep(target_interval)

            except Exception as e:
                logger.debug(
                    f"Pod movement simulation error: {e}", exc_info=True)
                await asyncio.sleep(target_interval)

    async def _update_metrics(self):
        """Update system metrics"""
        # Count active pods
        active_pods = sum(
            1 for pod in self.pods.values() if pod.status.value != "maintenance"
        )

        # Count operational stations
        operational_stations = sum(
            1
            for station in self.stations.values()
            if station.status.value == "operational"
        )

        # Count pending requests
        pending_passengers = sum(
            len(station.passenger_queue) for station in self.stations.values()
        )
        pending_cargo = sum(
            len(station.cargo_queue) for station in self.stations.values()
        )

        # Calculate average wait time
        wait_times = [
            station.average_wait_time
            for station in self.stations.values()
            if station.average_wait_time > 0
        ]
        avg_wait_time = sum(wait_times) / \
                        len(wait_times) if wait_times else 0.0

        # Calculate system efficiency (simplified)
        total_processed = sum(
            station.total_passengers_processed + station.total_cargo_processed
            for station in self.stations.values()
        )
        total_requests = total_processed + pending_passengers + pending_cargo
        system_efficiency = (
            total_processed / total_requests if total_requests > 0 else 0.0
        )

        # Calculate throughput per hour
        if self.start_time:
            hours_running = (datetime.now() -
                             self.start_time).total_seconds() / 3600
            throughput_per_hour = (
                total_processed / hours_running if hours_running > 0 else 0
            )
        else:
            throughput_per_hour = 0

        # Calculate fallback rate
        total_decisions = sum(
            len(pod.decision_engine.decision_history) for pod in self.pods.values()
        )
        fallback_usage_rate = 0.0
        if total_decisions > 0:
            fallback_decisions = sum(
                sum(1 for d in pod.decision_engine.decision_history if d.fallback_used)
                for pod in self.pods.values()
            )
            fallback_usage_rate = fallback_decisions / total_decisions

        # Update metrics
        self.metrics.update(
            {
                "total_pods": len(self.pods),
                "active_pods": active_pods,
                "total_stations": len(self.stations),
                "operational_stations": operational_stations,
                "pending_passengers": pending_passengers,
                "pending_cargo": pending_cargo,
                "average_wait_time": avg_wait_time,
                "system_efficiency": system_efficiency,
                "throughput_per_hour": throughput_per_hour,
                "fallback_usage_rate": fallback_usage_rate,
            }
        )

    async def _publish_snapshot(self):
        """Publish system snapshot"""
        snapshot = SystemSnapshot(system_state=self.metrics.copy())

        await self.message_bus.publish_event(
            MessageBus.get_event_channel(snapshot.event_type), snapshot
        )

    async def _log_system_status(self):
        """Log system status"""
        logger.info(
            f"System Status - Pods: {self.metrics['active_pods']}/{self.metrics['total_pods']}, "
            f"Stations: {self.metrics['operational_stations']}/{self.metrics['total_stations']}, "
            f"Queue: {self.metrics['pending_passengers']}P/{self.metrics['pending_cargo']}C, "
            f"Efficiency: {self.metrics['system_efficiency']:.1%}, "
            f"Fallback Rate: {self.metrics['fallback_usage_rate']:.1%}"
        )

    def get_system_state(self) -> dict:
        """Get complete system state"""
        return {
            "system_id": "aexis_main",
            "timestamp": datetime.now().isoformat(),
            "running": self.running,
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds()
            if self.start_time
            else 0,
            "metrics": self.metrics,
            "stations": {
                sid: station.get_state() for sid, station in self.stations.items()
            },
            "pods": {pid: pod.get_state() for pid, pod in self.pods.items()},
        }

    def get_pod_state(self, pod_id: str) -> dict | None:
        """Get specific pod state"""
        pod = self.pods.get(pod_id)
        return pod.get_state() if pod else None

    async def inject_passenger_request(
            self, origin_id: str, dest_id: str, count: int = 1
    ):
        """Manually inject passenger request"""
        if not self.passenger_generator:
            raise RuntimeError("Passenger generator not initialized")

        for _ in range(count):
            passenger_id = f"manual_p_{datetime.now().strftime('%H%M%S')}_{random.randint(100, 999)}"
            event = self.passenger_generator._create_manual_event(
                passenger_id, origin_id, dest_id
            )
            await self.message_bus.publish_event(
                MessageBus.get_event_channel(event.event_type), event
            )
            logger.info(
                f"Manually injected passenger {passenger_id} at {origin_id} -> {dest_id}"
            )

    async def inject_cargo_request(
            self, origin_id: str, dest_id: str, weight: float = 100.0
    ):
        """Manually inject cargo request"""
        if not self.cargo_generator:
            raise RuntimeError("Cargo generator not initialized")

        request_id = f"manual_c_{datetime.now().strftime('%H%M%S')}_{random.randint(100, 999)}"
        event = self.cargo_generator._create_manual_event(
            request_id, origin_id, dest_id, weight
        )
        channel = MessageBus.get_event_channel(event.event_type)
        # logger.warning(f"Publishing manual cargo event to channel {channel} with request_id {request_id}")
        await self.message_bus.publish_event(channel, event)
        # logger.warning(
        #     f"Manually injected cargo {request_id} at {origin_id} -> {dest_id}")

    def get_station_state(self, station_id: str) -> dict | None:
        """Get specific station state"""
        station = self.stations.get(station_id)
        return station.get_state() if station else None
