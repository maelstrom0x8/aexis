import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest
import fakeredis.aioredis

from aexis.core.message_bus import LocalMessageBus, MessageBus
from aexis.core.model import (
    CargoRequest,
    Coordinate,
    LocationDescriptor,
    PassengerArrival,
    PodStatus,
)
from aexis.core.network import NetworkContext
from aexis.core.station_client import StationClient
from aexis.pod import CargoPod, PassengerPod
from aexis.station import Station


@dataclass
class StationNode:
    """Describes a station and its connections."""
    station_id: str
    x: float
    y: float
    neighbors: list[str] = field(default_factory=list)
    edge_weights: dict[str, float] = field(default_factory=dict)


@dataclass
class PodSpec:
    """Describes a pod to spawn."""
    pod_id: str
    pod_type: str  # "passenger" or "cargo"
    start_station: str


@dataclass
class SeededPassenger:
    """A passenger pre-placed at a station queue."""
    passenger_id: str
    station_id: str
    destination: str


@dataclass
class SeededCargo:
    """A cargo item pre-placed at a station queue."""
    request_id: str
    station_id: str
    destination: str
    weight: float


@dataclass
class ScenarioConfig:
    """Full scenario specification — everything needed to spin up a test."""
    name: str
    stations: list[StationNode]
    pods: list[PodSpec]
    passengers: list[SeededPassenger] = field(default_factory=list)
    cargo: list[SeededCargo] = field(default_factory=list)


# ======================================================================
# System under test — the wired-up stack
# ======================================================================


class _LogCaptureHandler(logging.Handler):
    """Collects all log records into a list for post-test inspection."""

    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord):
        self.records.append(record)


class SystemUnderTest:
    """Holds all live components after Arrange. Query/mutate freely in Act."""

    def __init__(
        self,
        config: ScenarioConfig,
        redis_client,
        message_bus: LocalMessageBus,
        network: NetworkContext,
        station_client: StationClient,
        stations: dict[str, Station],
        pods: dict[str, PassengerPod | CargoPod],
    ):
        self.config = config
        self.redis = redis_client
        self.bus = message_bus
        self.network = network
        self.station_client = station_client
        self.stations = stations
        self.pods = pods
        self.captured_events: list[dict] = []
        self._log_handler = _LogCaptureHandler()
        self._log_handler.setLevel(logging.DEBUG)

    # -- convenience accessors --

    def pod(self, pod_id: str) -> PassengerPod | CargoPod:
        return self.pods[pod_id]

    def station(self, station_id: str) -> Station:
        return self.stations[station_id]

    @property
    def passenger_pods(self) -> list[PassengerPod]:
        return [p for p in self.pods.values() if isinstance(p, PassengerPod)]

    @property
    def cargo_pods(self) -> list[CargoPod]:
        return [p for p in self.pods.values() if isinstance(p, CargoPod)]

    def events_of_type(self, event_type: str) -> list[dict]:
        return [
            e for e in self.captured_events
            if e.get("message", {}).get("event_type") == event_type
        ]

    @property
    def captured_logs(self) -> list[logging.LogRecord]:
        """All log records emitted by aexis.* loggers during this test."""
        return self._log_handler.records

    def logs_containing(self, substring: str) -> list[str]:
        """Return formatted log lines whose message contains the substring."""
        return [
            f"[{r.levelname}] {r.name}: {r.getMessage()}"
            for r in self._log_handler.records
            if substring in r.getMessage()
        ]

    def dump_logs(self, level: int = logging.DEBUG) -> str:
        """Return all captured logs at or above `level` as a formatted string."""
        lines = []
        for r in self._log_handler.records:
            if r.levelno >= level:
                lines.append(
                    f"{r.created:.3f} [{r.levelname:>7s}] {r.name}: {r.getMessage()}"
                )
        return "\n".join(lines)


# ======================================================================
# Builder — turns a ScenarioConfig into a live SystemUnderTest
# ======================================================================


async def _build_system(config: ScenarioConfig) -> SystemUnderTest:
    """Wire up the full in-memory stack from a ScenarioConfig."""

    # --- Redis ---
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)

    # --- Message Bus ---
    bus = LocalMessageBus()
    await bus.connect()

    # --- Network ---
    nodes = []
    for sn in config.stations:
        adj = []
        for nbr in sn.neighbors:
            weight = sn.edge_weights.get(nbr, 1.0)
            adj.append({"node_id": nbr, "weight": weight})
        nodes.append({
            "id": sn.station_id,
            "label": sn.station_id,
            "coordinate": {"x": sn.x, "y": sn.y},
            "adj": adj,
        })
    network = NetworkContext({"nodes": nodes})
    NetworkContext.set_instance(network)

    # --- StationClient ---
    station_client = StationClient(redis_client)

    # --- Stations ---
    stations: dict[str, Station] = {}
    for sn in config.stations:
        station = Station(bus, redis_client, sn.station_id)
        station.connected_stations = list(sn.neighbors)
        await station.start()
        stations[sn.station_id] = station

    # --- Seed passengers ---
    for p in config.passengers:
        key = f"aexis:station:{p.station_id}:passengers"
        data = json.dumps({
            "passenger_id": p.passenger_id,
            "destination": p.destination,
            "priority": 3,
            "arrival_time": datetime.now(UTC).isoformat(),
        })
        await redis_client.hset(key, p.passenger_id, data)
        if p.station_id in stations:
            stations[p.station_id]._passenger_count += 1

    # --- Seed cargo ---
    for c in config.cargo:
        key = f"aexis:station:{c.station_id}:cargo"
        data = json.dumps({
            "request_id": c.request_id,
            "destination": c.destination,
            "weight": c.weight,
            "arrival_time": datetime.now(UTC).isoformat(),
        })
        await redis_client.hset(key, c.request_id, data)
        if c.station_id in stations:
            stations[c.station_id]._cargo_count += 1

    # --- Pods ---
    pods: dict[str, PassengerPod | CargoPod] = {}
    for spec in config.pods:
        pos = network.station_positions.get(spec.start_station, (0, 0))
        loc = LocationDescriptor(
            location_type="station",
            node_id=spec.start_station,
            coordinate=Coordinate(pos[0], pos[1]),
        )
        if spec.pod_type == "passenger":
            pod = PassengerPod(bus, redis_client, spec.pod_id, station_client)
        elif spec.pod_type == "cargo":
            pod = CargoPod(bus, redis_client, spec.pod_id, station_client)
        else:
            raise ValueError(f"Unknown pod type: {spec.pod_type!r}")
        pod.location_descriptor = loc
        pods[spec.pod_id] = pod

    # --- Event capture (records everything across all channels) ---
    sut = SystemUnderTest(
        config=config,
        redis_client=redis_client,
        message_bus=bus,
        network=network,
        station_client=station_client,
        stations=stations,
        pods=pods,
    )

    async def _capture(data: dict):
        sut.captured_events.append(data)

    for channel in MessageBus.CHANNELS.values():
        bus.subscribe(channel, _capture)

    # Attach log capture to the root aexis logger so all pod/station/bus
    # log output is collected into sut.captured_logs for inspection.
    aexis_logger = logging.getLogger("aexis")
    aexis_logger.addHandler(sut._log_handler)
    aexis_logger.setLevel(logging.DEBUG)

    return sut


async def _teardown_system(sut: SystemUnderTest):
    """Clean shutdown of all components."""
    # Detach log capture handler to prevent pollution across tests
    aexis_logger = logging.getLogger("aexis")
    aexis_logger.removeHandler(sut._log_handler)

    # Dump logs to file for post-mortem inspection
    log_output = sut.dump_logs()
    if log_output:
        with open("/tmp/aexis_test_logs.txt", "w") as f:
            f.write(log_output)

    for station in sut.stations.values():
        await station.stop()
    await sut.bus.disconnect()
    await sut.redis.aclose()


# ======================================================================
# Test helpers
# ======================================================================


async def _run_system(sut: SystemUnderTest):
    """Start all pods and launch movement loops as background tasks.

    Returns a list of asyncio.Task objects that must be cancelled after the
    test completes.  Pods are started with ``pod.start()`` (subscribes to
    events) then ``run_movement_loop()`` is launched as a concurrent task.
    """
    tasks: list[asyncio.Task] = []
    for pod in sut.pods.values():
        await pod.start()
        tasks.append(asyncio.create_task(pod.run_movement_loop()))
    return tasks


async def _cancel_tasks(tasks: list[asyncio.Task]):
    """Cancel all background movement-loop tasks and suppress cancellation errors."""
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _wait_for_delivery(
    sut: SystemUnderTest,
    passenger_id: str,
    timeout_seconds: float = 60.0,
    poll_interval: float = 0.1,
):
    """Poll captured events until a PassengerDelivered event appears for the given passenger.

    Raises TimeoutError if the delivery doesn't happen within the deadline.
    """
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        for ev in sut.events_of_type("PassengerDelivered"):
            msg = ev.get("message", {})
            if msg.get("passenger_id") == passenger_id:
                return msg
        await asyncio.sleep(poll_interval)
    raise TimeoutError(
        f"Passenger {passenger_id} was not delivered within {timeout_seconds}s"
    )


async def _seed_and_announce_passenger(
    sut: SystemUnderTest,
    passenger_id: str,
    station_id: str,
    destination: str,
):
    """Seed a passenger into the Redis queue and publish a PassengerArrival event.

    This is the public-API way to inject a passenger into the system: the
    station handles the arrival event (adds to its queue) and all subscribed
    pods receive the broadcast for claiming.
    """
    event = PassengerArrival(
        passenger_id=passenger_id,
        station_id=station_id,
        destination=destination,
    )
    channel = MessageBus.get_event_channel(event.event_type)
    await sut.bus.publish_event(channel, event)


async def _wait_for_cargo_delivery(
    sut: SystemUnderTest,
    request_id: str,
    timeout_seconds: float = 60.0,
    poll_interval: float = 0.1,
):
    """Poll captured events until a CargoDelivered event appears for the given cargo.

    Raises TimeoutError if the delivery doesn't happen within the deadline.
    """
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        for ev in sut.events_of_type("CargoDelivered"):
            msg = ev.get("message", {})
            if msg.get("request_id") == request_id:
                return msg
        await asyncio.sleep(poll_interval)
    raise TimeoutError(
        f"Cargo {request_id} was not delivered within {timeout_seconds}s"
    )


async def _seed_and_announce_cargo(
    sut: SystemUnderTest,
    request_id: str,
    station_id: str,
    destination: str,
    weight: float,
):
    """Seed cargo into the Redis queue and publish a CargoRequest event."""
    key = f"aexis:station:{station_id}:cargo"
    data = json.dumps({
        "request_id": request_id,
        "destination": destination,
        "weight": weight,
        "arrival_time": datetime.now(UTC).isoformat(),
    })
    await sut.redis.hset(key, request_id, data)

    event = CargoRequest(
        request_id=request_id,
        origin=station_id,
        destination=destination,
        weight=weight,
    )
    channel = MessageBus.get_event_channel(event.event_type)
    await sut.bus.publish_event(channel, event)


# ======================================================================
# Preset scenarios — add, edit, or parametrise these
# ======================================================================


TRIANGLE = ScenarioConfig(
    name="triangle_2pods",
    stations=[
        StationNode("1", 0, 0, neighbors=["2", "3"]),
        StationNode("2", 100, 0, neighbors=["1", "3"]),
        StationNode("3", 50, 87, neighbors=["1", "2"]),
    ],
    pods=[
        PodSpec("p1", "passenger", "1"),
        PodSpec("p2", "passenger", "2"),
    ],
    passengers=[
        SeededPassenger("pax_a", "1", "3"),
        SeededPassenger("pax_b", "2", "1"),
    ],
)


LINEAR_5 = ScenarioConfig(
    name="linear_chain_3pods",
    stations=[
        StationNode("1", 0, 0, neighbors=["2"]),
        StationNode("2", 100, 0, neighbors=["1", "3"]),
        StationNode("3", 200, 0, neighbors=["2", "4"]),
        StationNode("4", 300, 0, neighbors=["3", "5"]),
        StationNode("5", 400, 0, neighbors=["4"]),
    ],
    pods=[
        PodSpec("p1", "passenger", "1"),
        PodSpec("c1", "cargo", "3"),
        PodSpec("p2", "passenger", "5"),
    ],
    passengers=[
        SeededPassenger("pax_far", "1", "5"),
        SeededPassenger("pax_mid", "3", "1"),
    ],
    cargo=[
        SeededCargo("cgo_1", "3", "5", 80.0),
        SeededCargo("cgo_2", "3", "1", 120.0),
    ],
)


STAR_HUB = ScenarioConfig(
    name="star_hub_stress",
    stations=[
        StationNode("hub", 0, 0, neighbors=["s1", "s2", "s3", "s4"]),
        StationNode("s1", 100, 0, neighbors=["hub"]),
        StationNode("s2", 0, 100, neighbors=["hub"]),
        StationNode("s3", -100, 0, neighbors=["hub"]),
        StationNode("s4", 0, -100, neighbors=["hub"]),
    ],
    pods=[
        PodSpec("p1", "passenger", "s1"),
        PodSpec("p2", "passenger", "s2"),
        PodSpec("c1", "cargo", "hub"),
    ],
    passengers=[
        SeededPassenger("pax_1", "s1", "s3"),
        SeededPassenger("pax_2", "s2", "s4"),
        SeededPassenger("pax_3", "hub", "s1"),
    ],
    cargo=[
        SeededCargo("cgo_hub", "hub", "s4", 200.0),
    ],
)


DISCONNECTED = ScenarioConfig(
    name="disconnected_clusters",
    stations=[
        StationNode("a1", 0, 0, neighbors=["a2"]),
        StationNode("a2", 100, 0, neighbors=["a1"]),
        StationNode("b1", 500, 0, neighbors=["b2"]),
        StationNode("b2", 600, 0, neighbors=["b1"]),
    ],
    pods=[
        PodSpec("p_a", "passenger", "a1"),
        PodSpec("p_b", "passenger", "b1"),
    ],
    passengers=[
        SeededPassenger("pax_cross", "a1", "b2"),  # unreachable
        SeededPassenger("pax_local", "a1", "a2"),  # reachable
    ],
)


SINGLE_STATION = ScenarioConfig(
    name="single_station_degenerate",
    stations=[
        StationNode("solo", 0, 0, neighbors=[]),
    ],
    pods=[
        PodSpec("p_solo", "passenger", "solo"),
    ],
    passengers=[
        SeededPassenger("pax_nowhere", "solo", "solo"),
    ],
)

SIMPLE = ScenarioConfig(
    name="gungir",
    stations=[StationNode('1', -20, 80, neighbors=['2', '3', '4']),
              StationNode('2', 0, 0, neighbors=['1', '3', '4']),
              StationNode('3', 80, 0, neighbors=['1', '2', '4']),
              StationNode('4', -46, -35, neighbors=['1', '2', '3']),
              ],
    pods=[PodSpec('p1', 'passenger', '1'),
          PodSpec('p2', 'passenger', '2'),
          PodSpec('p3', 'cargo', '4')
          ],
)


def _load_production_network() -> ScenarioConfig:
    """Build a ScenarioConfig from the real production network.json file.

    This mirrors the exact topology the system runs in production — 21 stations,
    real Euclidean edge weights, and pods distributed across the network.
    """
    import os
    json_path = os.path.join(os.path.dirname(__file__), "..", "network.json")
    with open(json_path) as f:
        data = json.load(f)

    stations = []
    for node in data["nodes"]:
        sid = node["id"]
        coord = node["coordinate"]
        neighbors = [a["node_id"] for a in node["adj"]]
        weights = {a["node_id"]: a["weight"] for a in node["adj"]}
        stations.append(StationNode(
            station_id=sid,
            x=coord["x"],
            y=coord["y"],
            neighbors=neighbors,
            edge_weights=weights,
        ))

    return ScenarioConfig(
        name="production",
        stations=stations,
        pods=[
            PodSpec("p1", "passenger", "1"),   # south-west corner
            PodSpec("p2", "passenger", "10"),   # central hub
            PodSpec("p3", "passenger", "17"),   # north-east corner
            PodSpec("c1", "cargo", "11"),        # west edge
        ],
    )


PRODUCTION = _load_production_network()


# 31-station high-complexity mesh with strategic inter-cluster connections and 18 pods.
# Organized as 4 regional clusters with bridge stations enabling cross-cluster routing.
def _build_metroplex_network():
    """Build a 31-station high-complexity network with 4 regional clusters."""
    import math

    stations = []
    pods = []

    # Central Hub (stations 1-5): 5-station fully meshed core
    hub_positions = [
        (0, 0), (80, 0), (40, 70), (-40, 70), (-80, 0)
    ]
    hub_ids = [str(i) for i in range(1, 6)]
    for idx, (x, y) in enumerate(hub_positions):
        neighbors = [s for s in hub_ids if s != hub_ids[idx]]
        stations.append(StationNode(hub_ids[idx], x, y, neighbors=neighbors))

    # North Cluster (stations 6-12): Ring topology with 2 hub bridges
    north_ring = []
    for i in range(7):
        angle = (i / 7) * 2 * math.pi
        x = 400 + 150 * math.cos(angle)
        y = 300 + 150 * math.sin(angle)
        station_id = str(6 + i)
        north_ring.append((station_id, x, y))

    for i, (sid, x, y) in enumerate(north_ring):
        prev_idx = (i - 1) % len(north_ring)
        next_idx = (i + 1) % len(north_ring)
        neighbors = [north_ring[prev_idx][0], north_ring[next_idx][0]]
        # Add bridge connections to hub
        if i == 0:
            neighbors.extend(["1", "2"])
        elif i == 3:
            neighbors.extend(["2", "3"])
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

    # East Cluster (stations 13-19): Tree topology with hub bridge
    east_base = [(500, 0), (500, 120), (500, -120)]
    east_ids = ["13", "14", "15"]
    for sid, (x, y) in zip(east_ids, east_base):
        neighbors = []
        if sid == "13":
            neighbors = ["14", "15", "3", "4"]  # Hub bridges
        else:
            neighbors = ["13"] + [str(16 + j) for j in range(4)]
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

    # East subtree (stations 16-19)
    for i in range(4):
        angle = (i / 4) * 2 * math.pi
        x = 620 + 100 * math.cos(angle)
        y = 60 + 100 * math.sin(angle)
        sid = str(16 + i)
        neighbors = ["14"] if i == 0 else ["15"]
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

    # West Cluster (stations 20-26): Ring topology with hub bridge
    west_ring = []
    for i in range(7):
        angle = (i / 7) * 2 * math.pi
        x = -400 + 150 * math.cos(angle)
        y = 300 + 150 * math.sin(angle)
        station_id = str(20 + i)
        west_ring.append((station_id, x, y))

    for i, (sid, x, y) in enumerate(west_ring):
        prev_idx = (i - 1) % len(west_ring)
        next_idx = (i + 1) % len(west_ring)
        neighbors = [west_ring[prev_idx][0], west_ring[next_idx][0]]
        if i == 0:
            neighbors.extend(["4", "5"])
        elif i == 3:
            neighbors.extend(["5", "1"])
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

    # South Cluster (stations 27-31): Star topology with hub bridge
    south_center = (0, -300)
    south_spokes = [
        (0, -450),      # spoke 0
        (120, -350),    # spoke 1
        (120, -250),    # spoke 2
        (-120, -250),   # spoke 3
        (-120, -350),   # spoke 4
    ]

    stations.append(StationNode("27", south_center[0], south_center[1],
                                neighbors=["28", "29", "30", "31", "1", "5"]))
    for i, (x, y) in enumerate(south_spokes):
        sid = str(28 + i)
        stations.append(StationNode(sid, x, y, neighbors=["27"]))

    # Distribute 18 pods: 14 passenger + 4 cargo
    # Hub core: 4 passenger pods
    pods.extend([
        PodSpec("p1", "passenger", "1"),
        PodSpec("p2", "passenger", "2"),
        PodSpec("p3", "passenger", "5"),
        PodSpec("p4", "passenger", "3"),
    ])

    # Regional hubs: 10 passenger pods
    pods.extend([
        PodSpec("p5", "passenger", "9"),   # North
        PodSpec("p6", "passenger", "13"),  # East
        PodSpec("p7", "passenger", "22"),  # West
        PodSpec("p8", "passenger", "27"),  # South
        PodSpec("p9", "passenger", "6"),   # North rim
        PodSpec("p10", "passenger", "16"),  # East tree
        PodSpec("p11", "passenger", "20"),  # West rim
        PodSpec("p12", "passenger", "28"),  # South spoke
        PodSpec("p13", "passenger", "12"),  # North rim
        PodSpec("p14", "passenger", "25"),  # West rim
    ])

    # Cargo pods: 4 distributed strategically
    pods.extend([
        PodSpec("c1", "cargo", "4"),   # Hub (central distribution)
        PodSpec("c2", "cargo", "14"),  # East regional hub
        PodSpec("c3", "cargo", "23"),  # West regional hub
        PodSpec("c4", "cargo", "19"),  # East perimeter
    ])

    return stations, pods


_metroplex_stations, _metroplex_pods = _build_metroplex_network()

METROPLEX = ScenarioConfig(
    name="metroplex",
    stations=_metroplex_stations,
    pods=_metroplex_pods,
)


ALL_SCENARIOS = [SIMPLE, PRODUCTION, METROPLEX]


@pytest.fixture(params=ALL_SCENARIOS, ids=lambda s: s.name)
async def aexis_sys(request):
    config: ScenarioConfig = request.param
    sut = await _build_system(config)
    yield sut
    await _teardown_system(sut)


class TestAexisystem:

    async def test_injecting_a_passenger_at_station_1_going_to_station_4(self, aexis_sys: SystemUnderTest):
        """Inject a passenger at station 1 destined for station 4.
        p1 (at station 1) should pick them up and deliver them.
        p2 (at station 2) should remain idle.
        p3 (cargo, at station 4) should be unaffected."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            p1 = sut.pod("p1")
            p2 = sut.pod("p2")
            p1_passengers_before = len(p1.passengers)
            p2_passengers_before = len(p2.passengers)

            # Identify a station ID that exists in every scenario for destination.
            # All scenarios have station "1"; pick a valid destination that is
            # reachable from "1". In SIMPLE/PRODUCTION/METROPLEX, stations "1"
            # and "4" (or "2") always exist with a path between them.
            # We use the first neighbor of "1" that is not "1" itself as fallback.
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            # -- Act --
            await _seed_and_announce_passenger(
                sut, "test_pax_inject", origin, destination
            )
            delivery_event = await _wait_for_delivery(
                sut, "test_pax_inject", timeout_seconds=60.0
            )

            # -- Assert --
            # The passenger was delivered at the correct destination
            assert delivery_event["station_id"] == destination

            # p1 performed the delivery (it was docked at station 1, closest to origin)
            assert delivery_event["pod_id"] == "p1"

            # p1 no longer carries the delivered passenger
            assert not any(
                p.get("passenger_id") == "test_pax_inject"
                for p in p1.passengers
            )

            # p2 was not involved — its passenger count should not have increased
            # from passengers injected in this test
            assert not any(
                p.get("passenger_id") == "test_pax_inject"
                for p in p2.passengers
            )

            # Cargo pods are unaffected — verify via the first cargo pod if present
            for cpod in sut.cargo_pods:
                assert not any(
                    c.get("passenger_id") == "test_pax_inject"
                    for c in cpod.cargo
                )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_after_dropoff_p1_should_be_idle_at_station_4_and_ready_to_pick_up_another_passenger(self, aexis_sys: SystemUnderTest):
        """After delivering a passenger at station 4, p1 should remain IDLE
        at station 4 with no passengers — it should not phantom-route anywhere."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            p1 = sut.pod("p1")
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            await _seed_and_announce_passenger(
                sut, "test_pax_idle", origin, destination
            )
            await _wait_for_delivery(
                sut, "test_pax_idle", timeout_seconds=60.0
            )

            # -- Act --
            # Allow settle time for any phantom re-routing to manifest
            await asyncio.sleep(1.0)

            # -- Assert --
            # p1 is IDLE at the destination with no passengers onboard
            assert p1.status == PodStatus.IDLE, (
                f"Expected p1 to be IDLE after delivery, got {p1.status}"
            )
            assert p1.location_descriptor.location_type == "station", (
                f"Expected p1 at a station, got {p1.location_descriptor.location_type}"
            )
            assert p1.location_descriptor.node_id == destination, (
                f"Expected p1 at station {destination}, "
                f"got {p1.location_descriptor.node_id}"
            )
            assert not any(
                p.get("passenger_id") == "test_pax_idle"
                for p in p1.passengers
            ), "Delivered passenger should no longer be onboard"

            # p1 should have no active route (idle means no phantom routing)
            assert p1.current_route is None or p1.current_route.stations == [destination], (
                f"Expected no active route or idle-route at {destination}, "
                f"got {p1.current_route}"
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_docked_pod_picksup_all_payloads_to_full_capacity_before_calculating_route(self, aexis_sys: SystemUnderTest):
        """Flood station 1 with 5 passengers destined for station 4.
        p1 (docked at station 1) should claim all via proximity priority.
        p2 (remote, at station 2) should claim none."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            p1 = sut.pod("p1")
            p2 = sut.pod("p2")
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            passenger_ids = [f"flood_pax_{i}" for i in range(5)]

            # Seed all 5 passengers directly into Redis at station 1 BEFORE
            # announcing any of them. This ensures they are all present in the
            # queue when p1's first decision cycle fires _execute_pickup, which
            # opportunistically claims all unclaimed passengers at the station.
            for pid in passenger_ids:
                key = f"aexis:station:{origin}:passengers"
                data = json.dumps({
                    "passenger_id": pid,
                    "destination": destination,
                    "priority": 3,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, pid, data)

            # -- Act --
            # Announce the first passenger to trigger p1's claim + decision
            # cycle. During _execute_pickup at station 1, p1 will also
            # opportunistically claim the other 4 unclaimed passengers.
            await _seed_and_announce_passenger(
                sut, passenger_ids[0], origin, destination
            )

            # Wait for p1 to pick up all 5 — some may already be delivered by
            # the time we poll, so count via PassengerPickedUp events instead
            # of just onboard passengers.
            deadline = asyncio.get_event_loop().time() + 60.0
            while asyncio.get_event_loop().time() < deadline:
                p1_pickup_events = [
                    ev for ev in sut.events_of_type("PassengerPickedUp")
                    if ev.get("message", {}).get("pod_id") == "p1"
                    and ev.get("message", {}).get("passenger_id", "").startswith("flood_pax_")
                ]
                if len(p1_pickup_events) >= 5:
                    break
                await asyncio.sleep(0.1)

            # -- Assert --
            # p1 (docked at origin) claimed and picked up all 5 passengers.
            # Use PassengerPickedUp events as ground truth since some passengers
            # may have already been delivered (removed from p1.passengers).
            p1_pickup_events = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("pod_id") == "p1"
                and ev.get("message", {}).get("passenger_id", "").startswith("flood_pax_")
            ]
            assert len(p1_pickup_events) == 5, (
                f"Expected p1 to pick up 5 flood passengers, got {len(p1_pickup_events)}"
            )

            # p2 (remote, at station 2) should have claimed none of the flood
            # passengers — the docked pod wins every race via proximity delay.
            p2_pickup_events = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("pod_id") == "p2"
                and ev.get("message", {}).get("passenger_id", "").startswith("flood_pax_")
            ]
            assert len(p2_pickup_events) == 0, (
                f"Expected p2 to claim none of the flood passengers, "
                f"got {len(p2_pickup_events)}"
            )

            # Verify the Redis queue at station 1 has no unclaimed flood passengers
            pending = await sut.station_client.get_pending_passengers(origin)
            pending_flood = [
                p for p in pending
                if p.get("passenger_id", "").startswith("flood_pax_")
            ]
            assert len(pending_flood) == 0, (
                f"Expected no flood passengers remaining in queue, "
                f"got {len(pending_flood)}"
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_two_pods_converging_on_same_station_contend_for_single_payload(self, aexis_sys: SystemUnderTest):
        """p1 at station 1, p2 at station 2. Route both toward station 3:
        inject a passenger at station 1→3 (p1 claims it, routes to 3) and
        a passenger at station 2→3 (p2 claims it, routes to 3).
        While both are en route, inject a THIRD passenger at station 3
        destined for station 1.
        When both pods arrive at station 3 and deliver, only ONE pod should
        claim the third passenger. The other must remain idle at station 3
        or re-route elsewhere. No double-pickup. No duplicate delivery."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            origin_p1 = "1"
            origin_p2 = "2"
            # Pick a convergence station reachable from both pods' start stations
            station_ids = list(sut.stations.keys())
            convergence = "3" if "3" in sut.stations else station_ids[2]
            final_dest = origin_p1  # third passenger goes back to station 1

            # Route p1 toward convergence by injecting a passenger at 1→convergence
            await _seed_and_announce_passenger(
                sut, "converge_pax_p1", origin_p1, convergence
            )
            # Route p2 toward convergence by injecting a passenger at 2→convergence
            await _seed_and_announce_passenger(
                sut, "converge_pax_p2", origin_p2, convergence
            )

            # Seed the contested passenger at the convergence station into Redis
            # before either pod arrives, so it's waiting when they dock
            key = f"aexis:station:{convergence}:passengers"
            data = json.dumps({
                "passenger_id": "contested_pax",
                "destination": final_dest,
                "priority": 3,
                "arrival_time": datetime.now(UTC).isoformat(),
            })
            await sut.redis.hset(key, "contested_pax", data)

            # -- Act --
            # Wait for both initial passengers to be delivered at convergence
            await _wait_for_delivery(sut, "converge_pax_p1", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "converge_pax_p2", timeout_seconds=60.0)

            # Wait for the contested passenger to be delivered
            await _wait_for_delivery(sut, "contested_pax", timeout_seconds=60.0)

            # -- Assert --
            # Exactly one PassengerPickedUp event for the contested passenger
            contested_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("passenger_id") == "contested_pax"
            ]
            assert len(contested_pickups) == 1, (
                f"Expected exactly 1 pickup of contested_pax, got {len(contested_pickups)}"
            )

            # Exactly one PassengerDelivered event for the contested passenger
            contested_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "contested_pax"
            ]
            assert len(contested_deliveries) == 1, (
                f"Expected exactly 1 delivery of contested_pax, got {len(contested_deliveries)}"
            )

            # Delivered at the correct destination
            assert contested_deliveries[0]["message"]["station_id"] == final_dest

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_demand_injected_at_intermediate_station_while_pod_passes_through(self, aexis_sys: SystemUnderTest):
        """Route p1 from station 1 toward station 4 (carrying a passenger).
        While p1 is traversing an intermediate station (e.g. station 2),
        inject a new passenger at station 2 destined for station 1.
        p1 must deliver its original passenger to station 4 first — it
        should not silently drop the delivery obligation.
        After delivery, p1 should eventually return and pick up the
        station-2 passenger (or p2 should handle it).
        The invariant: no onboard passenger is ever abandoned mid-route."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )
            # Pick an intermediate station (not origin, not destination)
            intermediate = "2" if "2" in sut.stations else next(
                sid for sid in sut.stations
                if sid != origin and sid != destination
            )

            # Inject the primary passenger: origin → destination
            await _seed_and_announce_passenger(
                sut, "primary_pax", origin, destination
            )

            # Wait briefly for p1 to claim and begin routing, then inject at
            # intermediate. The exact timing doesn't matter for the invariant.
            await asyncio.sleep(0.5)

            # Inject the secondary passenger at intermediate → origin
            await _seed_and_announce_passenger(
                sut, "intermediate_pax", intermediate, origin
            )

            # -- Act --
            # Wait for BOTH passengers to be delivered
            await _wait_for_delivery(sut, "primary_pax", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "intermediate_pax", timeout_seconds=60.0)

            # -- Assert --
            # Primary passenger delivered at the correct destination
            primary_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "primary_pax"
            ]
            assert len(primary_deliveries) == 1
            assert primary_deliveries[0]["message"]["station_id"] == destination

            # Secondary passenger delivered at origin (station 1)
            secondary_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "intermediate_pax"
            ]
            assert len(secondary_deliveries) == 1
            assert secondary_deliveries[0]["message"]["station_id"] == origin

            # No passenger remains onboard any pod
            for pod in sut.passenger_pods:
                stranded = [
                    p for p in pod.passengers
                    if p.get("passenger_id") in ("primary_pax", "intermediate_pax")
                ]
                assert len(stranded) == 0, (
                    f"Pod {pod.pod_id} still carrying {stranded}"
                )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_chain_delivery_triggers_cascading_demand(self, aexis_sys: SystemUnderTest):
        """Seed passenger A at station 1 destined for station 2.
        Seed passenger B at station 2 destined for station 3.
        Seed passenger C at station 3 destined for station 4.
        p1 starts at station 1. As p1 delivers A at station 2, it should
        discover B waiting there, pick up B, route to station 3, deliver B,
        discover C, pick up C, route to station 4, deliver C.
        This tests the re-decision loop: each station arrival triggers
        a new decision that discovers the next link in the chain.
        All three passengers must receive PassengerDelivered events at
        their intended destinations."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            # Build the chain: pick 4 stations that exist in the scenario
            station_ids = sorted(sut.stations.keys())
            chain = station_ids[:4]  # first 4 stations in sorted order

            # Seed all 3 passengers into Redis before starting.
            # Each waits at station N destined for station N+1.
            chain_pax = []
            for i in range(3):
                pid = f"chain_pax_{i}"
                chain_pax.append(pid)
                key = f"aexis:station:{chain[i]}:passengers"
                data = json.dumps({
                    "passenger_id": pid,
                    "destination": chain[i + 1],
                    "priority": 3,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, pid, data)

            # -- Act --
            # Announce the first passenger to kick off the chain
            await _seed_and_announce_passenger(
                sut, chain_pax[0], chain[0], chain[1]
            )

            # Wait for all 3 deliveries
            for pid in chain_pax:
                await _wait_for_delivery(sut, pid, timeout_seconds=90.0)

            # -- Assert --
            # Each passenger delivered at the correct station
            for i, pid in enumerate(chain_pax):
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1, (
                    f"Expected exactly 1 delivery for {pid}, got {len(deliveries)}"
                )
                assert deliveries[0]["message"]["station_id"] == chain[i + 1], (
                    f"{pid} delivered at {deliveries[0]['message']['station_id']}, "
                    f"expected {chain[i + 1]}"
                )

            # Each passenger picked up exactly once
            for pid in chain_pax:
                pickups = [
                    ev for ev in sut.events_of_type("PassengerPickedUp")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(pickups) == 1, (
                    f"Expected exactly 1 pickup for {pid}, got {len(pickups)}"
                )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_en_route_pod_claims_demand_over_docked_pod_when_origin_is_on_route(self, aexis_sys: SystemUnderTest):
        """p1 is en route from station 1 to station 4, passing through station 2.
        p2 is docked idle at station 2.
        Inject a passenger at station 2 destined for station 4.
        Both pods are eligible: p2 is IDLE at station 2 (proximity advantage),
        p1 is EN_ROUTE with station 2 on its route.
        Due to proximity-based claiming delay, p2 (docked) should win the claim.
        Verify p2 picks up the passenger, not p1.
        If p1 does win (because it arrives first), the test should still
        verify exactly one pod claims it — never both."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            p1 = sut.pod("p1")
            p2 = sut.pod("p2")
            origin = "1"
            p2_station = "2"  # p2's start station in all scenarios
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations
                if sid != origin and sid != p2_station
            )

            # Route p1 away from station 1 by giving it a passenger to deliver
            await _seed_and_announce_passenger(
                sut, "decoy_pax", origin, destination
            )

            # Brief delay to let p1 claim and begin routing
            await asyncio.sleep(0.3)

            # -- Act --
            # Inject a passenger at p2's station (where p2 is docked idle)
            await _seed_and_announce_passenger(
                sut, "contested_local", p2_station, destination
            )

            # Wait for the contested passenger to be delivered
            await _wait_for_delivery(sut, "contested_local", timeout_seconds=60.0)

            # -- Assert --
            # Exactly one pod picked up the contested passenger
            contested_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("passenger_id") == "contested_local"
            ]
            assert len(contested_pickups) == 1, (
                f"Expected exactly 1 pickup of contested_local, got {len(contested_pickups)}"
            )

            # The pickup pod should be p2 (docked at the origin station, wins
            # the proximity race). We assert the invariant but note the winner.
            claiming_pod = contested_pickups[0]["message"]["pod_id"]
            assert claiming_pod in ("p1", "p2"), (
                f"Unexpected pod {claiming_pod} claimed contested_local"
            )

            # Exactly one delivery, at the correct destination
            contested_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "contested_local"
            ]
            assert len(contested_deliveries) == 1
            assert contested_deliveries[0]["message"]["station_id"] == destination

            # The decoy passenger must also be delivered (no abandonment)
            await _wait_for_delivery(sut, "decoy_pax", timeout_seconds=60.0)

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_payload_injected_at_station_pod_is_departing_from(self, aexis_sys: SystemUnderTest):
        """p1 is at station 1, idle. Inject passenger A at station 1→4.
        p1 claims A, begins routing toward station 4.
        Immediately after p1's status transitions to EN_ROUTE, inject
        passenger B at station 1→3.
        p1 has already left station 1. p2 (at station 2) is the next
        closest pod.
        Passenger B must not be silently lost. Either:
          - p2 claims and delivers B, or
          - p1 re-decides if station 1 is still on its route.
        A PassengerDelivered event for B must eventually appear."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            origin = "1"
            dest_a = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )
            dest_b = "3" if "3" in sut.stations else next(
                sid for sid in sut.stations
                if sid != origin and sid != dest_a
            )

            # Inject passenger A — p1 claims it and starts routing
            await _seed_and_announce_passenger(
                sut, "departure_pax_a", origin, dest_a
            )

            # Wait for p1 to begin departing (EN_ROUTE or beyond)
            p1 = sut.pod("p1")
            deadline = asyncio.get_event_loop().time() + 30.0
            while asyncio.get_event_loop().time() < deadline:
                if p1.status in (PodStatus.EN_ROUTE, PodStatus.LOADING):
                    break
                await asyncio.sleep(0.05)

            # -- Act --
            # Inject passenger B at the station p1 is leaving
            await _seed_and_announce_passenger(
                sut, "departure_pax_b", origin, dest_b
            )

            # Wait for both passengers to be delivered
            await _wait_for_delivery(sut, "departure_pax_a", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "departure_pax_b", timeout_seconds=60.0)

            # -- Assert --
            # Both passengers delivered at their correct destinations
            for pid, expected_dest in [("departure_pax_a", dest_a), ("departure_pax_b", dest_b)]:
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1, (
                    f"Expected exactly 1 delivery for {pid}, got {len(deliveries)}"
                )
                assert deliveries[0]["message"]["station_id"] == expected_dest, (
                    f"{pid} delivered at {deliveries[0]['message']['station_id']}, "
                    f"expected {expected_dest}"
                )

            # No passengers left on any pod
            for pod in sut.passenger_pods:
                stranded = [
                    p for p in pod.passengers
                    if p.get("passenger_id", "").startswith("departure_pax_")
                ]
                assert len(stranded) == 0, (
                    f"Pod {pod.pod_id} still carrying {stranded}"
                )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_hub_station_contention_with_multiple_simultaneous_arrivals(self, aexis_sys: SystemUnderTest):
        """In production/metroplex, pick a hub station with 3+ connections.
        Route p1 and p2 toward the hub from different directions by
        injecting passengers destined for the hub.
        Seed 3 passengers at the hub destined for 3 different spokes.
        Both pods arrive, deliver, then contend for the 3 waiting passengers.
        Invariants:
          - Each passenger is claimed by exactly one pod
          - No passenger is picked up by both pods
          - Total pickups across p1 and p2 equals 3
          - All 3 passengers eventually receive PassengerDelivered events"""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            # Find the most-connected station as the hub
            hub_id = max(
                sut.stations.keys(),
                key=lambda sid: len([
                    sn for sn in sut.config.stations
                    if sn.station_id == sid
                ][0].neighbors)
            )
            hub_node = next(
                sn for sn in sut.config.stations if sn.station_id == hub_id
            )
            # Pick 3 distinct spoke destinations from hub's neighbors
            spokes = hub_node.neighbors[:3]
            # Pad with any reachable stations if hub has fewer than 3 neighbors
            if len(spokes) < 3:
                extras = [
                    sid for sid in sut.stations
                    if sid != hub_id and sid not in spokes
                ]
                spokes.extend(extras[:3 - len(spokes)])

            # Seed 3 passengers at the hub, each destined for a different spoke
            hub_pax_ids = []
            for i, spoke in enumerate(spokes):
                pid = f"hub_pax_{i}"
                hub_pax_ids.append(pid)
                key = f"aexis:station:{hub_id}:passengers"
                data = json.dumps({
                    "passenger_id": pid,
                    "destination": spoke,
                    "priority": 3,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, pid, data)

            # Route p1 and p2 toward the hub by injecting decoy passengers
            p1_start = next(
                spec.start_station for spec in sut.config.pods
                if spec.pod_id == "p1"
            )
            p2_start = next(
                spec.start_station for spec in sut.config.pods
                if spec.pod_id == "p2"
            )

            # If a pod already starts at the hub, no decoy needed — just use
            # any non-hub station for the other pod's decoy destination.
            if p1_start != hub_id:
                await _seed_and_announce_passenger(
                    sut, "hub_decoy_p1", p1_start, hub_id
                )
            if p2_start != hub_id:
                await _seed_and_announce_passenger(
                    sut, "hub_decoy_p2", p2_start, hub_id
                )

            # -- Act --
            # Wait for all hub passengers to be delivered
            for pid in hub_pax_ids:
                await _wait_for_delivery(sut, pid, timeout_seconds=90.0)

            # -- Assert --
            # Each hub passenger was picked up exactly once
            for pid in hub_pax_ids:
                pickups = [
                    ev for ev in sut.events_of_type("PassengerPickedUp")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(pickups) == 1, (
                    f"Expected exactly 1 pickup for {pid}, got {len(pickups)}"
                )

            # Each hub passenger was delivered exactly once at correct destination
            for i, pid in enumerate(hub_pax_ids):
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1, (
                    f"Expected exactly 1 delivery for {pid}, got {len(deliveries)}"
                )
                assert deliveries[0]["message"]["station_id"] == spokes[i]

            # Total hub pickups distributed across passenger pods (no overlap)
            all_hub_pickup_pods = [
                ev.get("message", {}).get("pod_id")
                for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("passenger_id", "").startswith("hub_pax_")
            ]
            assert len(all_hub_pickup_pods) == 3

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_mixed_payload_flood_cargo_and_passenger_at_same_station(self, aexis_sys: SystemUnderTest):
        """Flood station 1 with 3 passengers AND 3 cargo items, all destined
        for station 4. p1 (passenger pod) and p3/c1 (cargo pod) handle their
        respective types.
        Passenger pods must pick up only passengers. Cargo pods only cargo.
        Neither pod type should cross-contaminate payload types.
        All 6 items must eventually be delivered to station 4.
        Verify via PassengerDelivered and CargoDelivered event counts."""

        sut = aexis_sys

        # This test requires at least one cargo pod in the scenario
        if not sut.cargo_pods:
            pytest.skip("No cargo pods in this scenario")

        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            # Seed 3 passengers into Redis at origin
            pax_ids = [f"mixed_pax_{i}" for i in range(3)]
            for pid in pax_ids:
                key = f"aexis:station:{origin}:passengers"
                data = json.dumps({
                    "passenger_id": pid,
                    "destination": destination,
                    "priority": 3,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, pid, data)

            # Seed 3 cargo items into Redis at origin
            cargo_ids = [f"mixed_cgo_{i}" for i in range(3)]
            for cid in cargo_ids:
                key = f"aexis:station:{origin}:cargo"
                data = json.dumps({
                    "request_id": cid,
                    "destination": destination,
                    "weight": 50.0,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, cid, data)

            # -- Act --
            # Announce one passenger and one cargo item to trigger decision
            # cycles for the respective pod types
            await _seed_and_announce_passenger(
                sut, pax_ids[0], origin, destination
            )
            await _seed_and_announce_cargo(
                sut, cargo_ids[0], origin, destination, 50.0
            )

            # Wait for all deliveries
            for pid in pax_ids:
                await _wait_for_delivery(sut, pid, timeout_seconds=90.0)
            for cid in cargo_ids:
                await _wait_for_cargo_delivery(sut, cid, timeout_seconds=90.0)

            # -- Assert --
            # All passengers delivered by passenger pods only
            for pid in pax_ids:
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1
                assert deliveries[0]["message"]["station_id"] == destination
                # Verify the delivering pod is a passenger pod
                delivering_pod_id = deliveries[0]["message"]["pod_id"]
                delivering_pod = sut.pods[delivering_pod_id]
                assert isinstance(delivering_pod, PassengerPod), (
                    f"Passenger {pid} delivered by non-passenger pod {delivering_pod_id}"
                )

            # All cargo delivered by cargo pods only
            for cid in cargo_ids:
                deliveries = [
                    ev for ev in sut.events_of_type("CargoDelivered")
                    if ev.get("message", {}).get("request_id") == cid
                ]
                assert len(deliveries) == 1
                assert deliveries[0]["message"]["station_id"] == destination
                delivering_pod_id = deliveries[0]["message"]["pod_id"]
                delivering_pod = sut.pods[delivering_pod_id]
                assert isinstance(delivering_pod, CargoPod), (
                    f"Cargo {cid} delivered by non-cargo pod {delivering_pod_id}"
                )

            # No passenger pod carries cargo, no cargo pod carries passengers
            for pod in sut.passenger_pods:
                assert not hasattr(pod, "cargo") or len(pod.cargo) == 0
            for pod in sut.cargo_pods:
                assert not hasattr(pod, "passengers") or len(pod.passengers) == 0

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_round_trip_demand_does_not_create_infinite_routing_loop(self, aexis_sys: SystemUnderTest):
        """Seed passenger A at station 1→4 and passenger B at station 4→1.
        p1 starts at station 1. p1 claims A, routes to 4, delivers A.
        At station 4, p1 discovers B, claims B, routes back to 1, delivers B.
        After both deliveries, p1 must converge to IDLE — it must NOT
        begin oscillating between stations 1 and 4 looking for phantom demand.
        Verify: after a settle period following B's delivery, p1 is IDLE
        with no current route and no passengers onboard."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            station_a = "1"
            station_b = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != station_a
            )

            # Seed passenger A at station_a → station_b
            key_a = f"aexis:station:{station_a}:passengers"
            data_a = json.dumps({
                "passenger_id": "roundtrip_pax_a",
                "destination": station_b,
                "priority": 3,
                "arrival_time": datetime.now(UTC).isoformat(),
            })
            await sut.redis.hset(key_a, "roundtrip_pax_a", data_a)

            # Seed passenger B at station_b → station_a
            key_b = f"aexis:station:{station_b}:passengers"
            data_b = json.dumps({
                "passenger_id": "roundtrip_pax_b",
                "destination": station_a,
                "priority": 3,
                "arrival_time": datetime.now(UTC).isoformat(),
            })
            await sut.redis.hset(key_b, "roundtrip_pax_b", data_b)

            # -- Act --
            # Announce passenger A to start the chain
            await _seed_and_announce_passenger(
                sut, "roundtrip_pax_a", station_a, station_b
            )

            # Wait for both passengers to be delivered
            await _wait_for_delivery(sut, "roundtrip_pax_a", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "roundtrip_pax_b", timeout_seconds=60.0)

            # Settle period — let any phantom re-routing manifest
            await asyncio.sleep(1.5)

            # -- Assert --
            p1 = sut.pod("p1")

            # p1 must be IDLE (not perpetually routing)
            assert p1.status == PodStatus.IDLE, (
                f"Expected p1 IDLE after round-trip, got {p1.status}"
            )

            # p1 must not carry any passengers
            stranded = [
                p for p in p1.passengers
                if p.get("passenger_id", "").startswith("roundtrip_pax_")
            ]
            assert len(stranded) == 0, (
                f"p1 still carrying {stranded} after round-trip delivery"
            )

            # p1's route must be idle (None or single-station)
            assert p1.current_route is None or len(p1.current_route.stations) <= 1, (
                f"p1 has phantom route {p1.current_route} after all demand exhausted"
            )

            # p1 must be at a station, not on an edge
            assert p1.location_descriptor.location_type == "station", (
                f"p1 is on edge {p1.location_descriptor.edge_id} after settling"
            )

            # Both passengers delivered at correct destinations
            for pid, expected in [("roundtrip_pax_a", station_b), ("roundtrip_pax_b", station_a)]:
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1
                assert deliveries[0]["message"]["station_id"] == expected

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    @pytest.mark.parametrize("k", [0, -5], ids=["exactly_full", "overflow_by_5"])
    async def test_capacity_boundary_m_minus_k_passengers_injected_at_docked_station(self, aexis_sys: SystemUnderTest, k: int):
        """Inject n = (m - k) passengers at station 1, where m = PassengerPod.capacity (19).
        Parametrise over k in {0, -5} — meaning n = 19 (exactly full) and n = 24 (overflow by 5).
        p1 is docked at station 1. p2 is at station 2.
        All passengers are destined for station 4.

        CAPACITY ENFORCEMENT (per pod):
          - p1 must NEVER carry more than m passengers at any point in time.
          - When n > m: p1 picks up exactly m. The remaining (n - m) passengers
            must stay in the station queue or be claimed by p2.

        CLAIMING INTEGRITY:
          - Total PassengerPickedUp events across ALL pods must equal n.
          - No passenger may have two PassengerPickedUp events (double-claim).

        OVERFLOW HANDLING (when n > m):
          - p2 must eventually claim and deliver the overflow passengers.
          - After all deliveries: station 1's passenger queue must be empty.

        DELIVERY COMPLETENESS:
          - Every injected passenger must eventually receive a PassengerDelivered
            event at destination. PassengerDelivered count must equal n.

        EVENT ORDERING:
          - The pod_id must be consistent between pickup and delivery for each
            passenger (the same pod that picked up must deliver)."""

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:
            # -- Arrange --
            p1 = sut.pod("p1")
            m = p1.capacity  # 19
            n = m - k         # k=0 → 19, k=-5 → 24
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            # Seed all n passengers into Redis at station 1 before triggering
            pax_ids = [f"cap_pax_{i}" for i in range(n)]
            for pid in pax_ids:
                key = f"aexis:station:{origin}:passengers"
                data = json.dumps({
                    "passenger_id": pid,
                    "destination": destination,
                    "priority": 3,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, pid, data)

            # -- Act --
            # Announce the first passenger to trigger p1's decision cycle.
            # p1's _execute_pickup will opportunistically claim up to capacity.
            await _seed_and_announce_passenger(
                sut, pax_ids[0], origin, destination
            )

            # Wait for ALL n passengers to be delivered. Use a generous timeout
            # because the overflow passengers may take extra time if they need
            # p2 to route from station 2 to station 1 to pick them up.
            timeout = 120.0
            deadline = asyncio.get_event_loop().time() + timeout
            while asyncio.get_event_loop().time() < deadline:
                delivered_ids = {
                    ev.get("message", {}).get("passenger_id")
                    for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id", "").startswith("cap_pax_")
                }
                if len(delivered_ids) >= n:
                    break
                await asyncio.sleep(0.2)

            # -- Assert --

            # 1. CAPACITY ENFORCEMENT: p1 never exceeded m passengers
            p1_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("pod_id") == "p1"
                and ev.get("message", {}).get("passenger_id", "").startswith("cap_pax_")
            ]
            assert len(p1_pickups) <= m, (
                f"p1 picked up {len(p1_pickups)} passengers, exceeding capacity {m}"
            )

            # 2. CLAIMING INTEGRITY: every passenger picked up exactly once
            all_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("passenger_id", "").startswith("cap_pax_")
            ]
            picked_up_ids = [
                ev.get("message", {}).get("passenger_id") for ev in all_pickups
            ]
            assert len(picked_up_ids) == n, (
                f"Expected {n} total pickups, got {len(picked_up_ids)}: "
                f"missing={set(pax_ids) - set(picked_up_ids)}"
            )
            assert len(set(picked_up_ids)) == n, (
                f"Duplicate pickups detected: {[pid for pid in picked_up_ids if picked_up_ids.count(pid) > 1]}"
            )

            # 3. DELIVERY COMPLETENESS: every passenger delivered exactly once
            all_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id", "").startswith("cap_pax_")
            ]
            delivered_ids_list = [
                ev.get("message", {}).get("passenger_id") for ev in all_deliveries
            ]
            assert len(delivered_ids_list) == n, (
                f"Expected {n} deliveries, got {len(delivered_ids_list)}: "
                f"missing={set(pax_ids) - set(delivered_ids_list)}"
            )
            assert len(set(delivered_ids_list)) == n, (
                f"Duplicate deliveries: {[pid for pid in delivered_ids_list if delivered_ids_list.count(pid) > 1]}"
            )

            # 4. CORRECT DESTINATION: all delivered at station 4
            for ev in all_deliveries:
                assert ev.get("message", {}).get("station_id") == destination, (
                    f"Passenger {ev['message']['passenger_id']} delivered at "
                    f"{ev['message']['station_id']}, expected {destination}"
                )

            # 5. EVENT CONSISTENCY: pickup pod_id matches delivery pod_id
            pickup_pod_map = {
                ev.get("message", {}).get("passenger_id"): ev.get("message", {}).get("pod_id")
                for ev in all_pickups
            }
            for ev in all_deliveries:
                pid = ev.get("message", {}).get("passenger_id")
                delivery_pod = ev.get("message", {}).get("pod_id")
                pickup_pod = pickup_pod_map.get(pid)
                assert pickup_pod == delivery_pod, (
                    f"Passenger {pid}: picked up by {pickup_pod} but delivered by {delivery_pod}"
                )

            # 6. OVERFLOW DISTRIBUTION (when n > m)
            if n > m:
                p2_pickups = [
                    ev for ev in sut.events_of_type("PassengerPickedUp")
                    if ev.get("message", {}).get("pod_id") == "p2"
                    and ev.get("message", {}).get("passenger_id", "").startswith("cap_pax_")
                ]
                overflow = n - m
                assert len(p2_pickups) == overflow, (
                    f"Expected p2 to pick up {overflow} overflow passengers, "
                    f"got {len(p2_pickups)}"
                )

            # 7. QUEUE DRAIN: station 1 has no leftover cap_pax passengers
            pending = await sut.station_client.get_pending_passengers(origin)
            leftover = [
                p for p in pending
                if p.get("passenger_id", "").startswith("cap_pax_")
            ]
            assert len(leftover) == 0, (
                f"Stranded passengers in queue: {[p.get('passenger_id') for p in leftover]}"
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)


# ======================================================================
# Bug-specific tests — gungir only (topology-independent bugs)
# ======================================================================


@pytest.fixture
async def gungir_sys():
    """Single-scenario fixture for targeted bug reproduction."""
    sut = await _build_system(SIMPLE)
    yield sut
    await _teardown_system(sut)


class TestPodCoordinationBugs:
    """Tests targeting specific pod coordination bugs observed in production logs."""

    async def test_pods_settle_to_idle_after_all_deliveries_complete(self, gungir_sys: SystemUnderTest):
        """BUG #2 — PHANTOM OSCILLATION

        Inject 1 passenger at station 1 (where p1 is docked) destined for
        station 4. After delivery, BOTH pods must settle to IDLE status.

        Root cause: _build_decision_context fetches global pending demand and
        injects stale/claimed requests into _available_requests, causing the
        routing provider to return non-idle routes perpetually.

        INVARIANTS:
          - After the PassengerDelivered event, allow 15s settling window.
          - At the end of the window, p1.status == IDLE.
          - p2.status == IDLE (it should never have left IDLE).
          - Neither pod should have an active current_route.
          - The total number of decision events after delivery should be ≤ 2
            (at most one re-decision per pod upon reaching the final station)."""

        sut = gungir_sys
        tasks = await _run_system(sut)
        try:
            # -- Act --
            await _seed_and_announce_passenger(sut, "phantom_pax", "1", "4")

            # Wait for delivery
            delivered = await _wait_for_delivery(sut, "phantom_pax", timeout_seconds=60.0)
            assert delivered, "Passenger was never delivered"

            # Record the event count at delivery time
            decisions_at_delivery = len(sut.events_of_type("PodDecision"))

            # Allow 15s settling window — pods should reach IDLE
            await asyncio.sleep(15.0)

            # -- Assert --
            p1 = sut.pod("p1")
            p2 = sut.pod("p2")

            assert p1.status == PodStatus.IDLE, (
                f"p1 status is {p1.status.value}, expected IDLE after delivery. "
                f"Route: {p1.current_route}"
            )
            assert p2.status == PodStatus.IDLE, (
                f"p2 status is {p2.status.value}, expected IDLE. "
                f"Route: {p2.current_route}"
            )

            # No active routes
            assert p1.current_route is None, (
                f"p1 still has route {p1.current_route.stations} after settling"
            )

            # Decision count after delivery should be minimal — not 20+ from oscillation
            decisions_after_settling = len(sut.events_of_type("PodDecision"))
            extra_decisions = decisions_after_settling - decisions_at_delivery
            assert extra_decisions <= 2, (
                f"Pods made {extra_decisions} additional decisions after delivery "
                f"(phantom oscillation). Logs:\n"
                + "\n".join(sut.logs_containing("executing decision")[-10:])
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_docked_pod_does_not_flip_flop_route_on_rapid_claims(self, gungir_sys: SystemUnderTest):
        """BUG #3 — ROUTE FLIP-FLOP DURING RAPID CLAIMING

        Inject 3 passengers simultaneously at station 1 (where p1 is docked),
        all destined for station 4. p1 should batch-process the claims and make
        a SINGLE routing decision, not 3 separate decisions that flip-flop.

        Root cause: each _handle_request_broadcast claim immediately calls
        make_decision(), overwriting the previous route before the pod moves.

        INVARIANTS:
          - p1 is docked at station 1 (local to all 3 passengers).
          - p1 should claim all 3 passengers (proximity advantage).
          - p1 should make at most 2 PodDecision events before departing
            station 1: one initial decision + at most one re-plan after
            opportunistic pickup.
          - The route in the final decision should be coherent (covering the
            destination), not flip-flopping between ['1','4'] and ['4','1']."""

        sut = gungir_sys
        tasks = await _run_system(sut)
        try:
            # -- Act --
            # Inject 3 passengers rapidly (announce each to trigger broadcast)
            for i in range(3):
                await _seed_and_announce_passenger(
                    sut, f"flipflop_pax_{i}", "1", "4"
                )

            # Wait for p1 to depart station 1 (status changes to EN_ROUTE)
            deadline = asyncio.get_event_loop().time() + 30.0
            while asyncio.get_event_loop().time() < deadline:
                p1 = sut.pod("p1")
                if p1.status == PodStatus.EN_ROUTE and p1.current_route:
                    break
                await asyncio.sleep(0.1)

            # -- Assert --
            # Count decisions made by p1 BEFORE departing station 1
            p1_decisions = [
                ev for ev in sut.events_of_type("PodDecision")
                if ev.get("message", {}).get("pod_id") == "p1"
            ]

            assert len(p1_decisions) <= 2, (
                f"p1 made {len(p1_decisions)} decisions before departure "
                f"(route flip-flop). Expected ≤2. Routes computed:\n"
                + "\n".join(
                    str(ev.get("message", {}).get("decision", {}).get("route"))
                    for ev in p1_decisions
                )
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_closer_remote_pod_claims_before_farther_remote_pod(self, gungir_sys: SystemUnderTest):
        """BUG #1 — MULTI-POD CLAIM SPLITTING (THUNDERING HERD)

        Inject 1 passenger at station 4 (where NO pod is docked). p1 is at
        station 1, p2 is at station 2. Station 4 is connected to both.
        The closer pod should win the claim; the farther pod should stay IDLE.

        With the flat 200ms delay, both remote pods wake simultaneously and
        race, causing non-deterministic splitting.

        INVARIANTS:
          - Exactly 1 pod claims the passenger (atomicity guaranteed by Redis).
          - Only the claiming pod should have status EN_ROUTE with a route
            toward station 4.
          - The non-claiming pod should remain IDLE (not routing toward 4).
          - No route decision events for the non-claiming pod that target
            station 4."""

        sut = gungir_sys
        tasks = await _run_system(sut)
        try:
            # -- Act --
            await _seed_and_announce_passenger(sut, "remote_pax", "4", "2")

            # Wait for claiming to settle (both pods receive broadcast + delay)
            await asyncio.sleep(2.0)

            # -- Assert --

            # 1. Verify distance-proportional delay mechanism:
            #    p2 (at station 2, dist=58) should get a SHORTER delay than
            #    p1 (at station 1, dist=118). Check the log lines.
            delay_logs = sut.logs_containing("delaying claim for remote_pax")
            assert len(delay_logs) >= 1, (
                f"Expected at least 1 delay log, got {len(delay_logs)}: {delay_logs}"
            )

            # Extract delay values from log lines
            p1_delay = None
            p2_delay = None
            for log_line in delay_logs:
                if "p1" in log_line:
                    # Parse "...delaying claim for remote_pax at 4 by 0.236s"
                    parts = log_line.split("by ")
                    if len(parts) >= 2:
                        p1_delay = float(parts[-1].rstrip("s"))
                elif "p2" in log_line:
                    parts = log_line.split("by ")
                    if len(parts) >= 2:
                        p2_delay = float(parts[-1].rstrip("s"))

            if p1_delay is not None and p2_delay is not None:
                assert p2_delay < p1_delay, (
                    f"Closer pod p2 should have shorter delay than p1. "
                    f"p2_delay={p2_delay:.3f}s, p1_delay={p1_delay:.3f}s"
                )

            # 2. Exactly one pod should have claimed the passenger.
            #    Use station_client claim log as ground truth.
            claim_logs = sut.logs_containing("claimed passenger remote_pax")
            claiming_pods = set()
            for cl in claim_logs:
                if "pod p1" in cl.lower() or "pod 1" in cl.lower():
                    claiming_pods.add("p1")
                elif "pod p2" in cl.lower() or "pod 2" in cl.lower():
                    claiming_pods.add("p2")
            assert len(claiming_pods) == 1, (
                f"Expected exactly 1 pod to claim passenger, got {len(claiming_pods)}: "
                f"{claiming_pods}. Logs:\n" + "\n".join(claim_logs)
            )

            # 3. The non-claiming pod should not have routing decisions
            #    targeting the passenger's station.
            claimer_id = claiming_pods.pop()
            non_claimer_id = "p2" if claimer_id == "p1" else "p1"
            non_claimer = sut.pod(non_claimer_id)

            # Non-claimer should be IDLE or at least not have a route to station 4
            non_claimer_decisions_to_4 = [
                ev for ev in sut.events_of_type("PodDecision")
                if ev.get("message", {}).get("pod_id") == non_claimer_id
                and "4" in (ev.get("message", {}).get("decision", {}).get("route", []))
            ]
            assert len(non_claimer_decisions_to_4) == 0, (
                f"{non_claimer_id} routed toward station 4 despite not claiming. "
                f"Decisions: {non_claimer_decisions_to_4}"
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)
