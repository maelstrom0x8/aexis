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
    station_id: str
    x: float
    y: float
    neighbors: list[str] = field(default_factory=list)
    edge_weights: dict[str, float] = field(default_factory=dict)

@dataclass
class PodSpec:
    pod_id: str
    pod_type: str
    start_station: str

@dataclass
class SeededPassenger:
    passenger_id: str
    station_id: str
    destination: str

@dataclass
class SeededCargo:
    request_id: str
    station_id: str
    destination: str
    weight: float

@dataclass
class ScenarioConfig:
    name: str
    stations: list[StationNode]
    pods: list[PodSpec]
    passengers: list[SeededPassenger] = field(default_factory=list)
    cargo: list[SeededCargo] = field(default_factory=list)

class _LogCaptureHandler(logging.Handler):

    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord):
        self.records.append(record)

class SystemUnderTest:

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
        return self._log_handler.records

    def logs_containing(self, substring: str) -> list[str]:
        return [
            f"[{r.levelname}] {r.name}: {r.getMessage()}"
            for r in self._log_handler.records
            if substring in r.getMessage()
        ]

    def dump_logs(self, level: int = logging.DEBUG) -> str:
        lines = []
        for r in self._log_handler.records:
            if r.levelno >= level:
                lines.append(
                    f"{r.created:.3f} [{r.levelname:>7s}] {r.name}: {r.getMessage()}"
                )
        return "\n".join(lines)

async def _build_system(config: ScenarioConfig) -> SystemUnderTest:

    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)

    bus = LocalMessageBus()
    await bus.connect()

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

    station_client = StationClient(redis_client)

    stations: dict[str, Station] = {}
    for sn in config.stations:
        station = Station(bus, redis_client, sn.station_id)
        station.connected_stations = list(sn.neighbors)
        await station.start()
        stations[sn.station_id] = station

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

    aexis_logger = logging.getLogger("aexis")
    aexis_logger.addHandler(sut._log_handler)
    aexis_logger.setLevel(logging.DEBUG)

    return sut

async def _teardown_system(sut: SystemUnderTest):

    aexis_logger = logging.getLogger("aexis")
    aexis_logger.removeHandler(sut._log_handler)

    log_output = sut.dump_logs()
    if log_output:
        with open("/tmp/aexis_test_logs.txt", "w") as f:
            f.write(log_output)

    for station in sut.stations.values():
        await station.stop()
    await sut.bus.disconnect()
    await sut.redis.aclose()

async def _run_system(sut: SystemUnderTest):
    tasks: list[asyncio.Task] = []
    for pod in sut.pods.values():
        await pod.start()
        tasks.append(asyncio.create_task(pod.run_movement_loop()))
    return tasks

async def _cancel_tasks(tasks: list[asyncio.Task]):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

async def _wait_for_delivery(
    sut: SystemUnderTest,
    passenger_id: str,
    timeout_seconds: float = 60.0,
    poll_interval: float = 0.1,
):
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
        SeededPassenger("pax_cross", "a1", "b2"),
        SeededPassenger("pax_local", "a1", "a2"),
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
            PodSpec("p1", "passenger", "1"),
            PodSpec("p2", "passenger", "10"),
            PodSpec("p3", "passenger", "17"),
            PodSpec("c1", "cargo", "11"),
        ],
    )

PRODUCTION = _load_production_network()

def _build_metroplex_network():
    import math

    stations = []
    pods = []

    hub_positions = [
        (0, 0), (80, 0), (40, 70), (-40, 70), (-80, 0)
    ]
    hub_ids = [str(i) for i in range(1, 6)]
    for idx, (x, y) in enumerate(hub_positions):
        neighbors = [s for s in hub_ids if s != hub_ids[idx]]
        stations.append(StationNode(hub_ids[idx], x, y, neighbors=neighbors))

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

        if i == 0:
            neighbors.extend(["1", "2"])
        elif i == 3:
            neighbors.extend(["2", "3"])
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

    east_base = [(500, 0), (500, 120), (500, -120)]
    east_ids = ["13", "14", "15"]
    for sid, (x, y) in zip(east_ids, east_base):
        neighbors = []
        if sid == "13":
            neighbors = ["14", "15", "3", "4"]
        else:
            neighbors = ["13"] + [str(16 + j) for j in range(4)]
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

    for i in range(4):
        angle = (i / 4) * 2 * math.pi
        x = 620 + 100 * math.cos(angle)
        y = 60 + 100 * math.sin(angle)
        sid = str(16 + i)
        neighbors = ["14"] if i == 0 else ["15"]
        stations.append(StationNode(sid, x, y, neighbors=neighbors))

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

    south_center = (0, -300)
    south_spokes = [
        (0, -450),
        (120, -350),
        (120, -250),
        (-120, -250),
        (-120, -350),
    ]

    stations.append(StationNode("27", south_center[0], south_center[1],
                                neighbors=["28", "29", "30", "31", "1", "5"]))
    for i, (x, y) in enumerate(south_spokes):
        sid = str(28 + i)
        stations.append(StationNode(sid, x, y, neighbors=["27"]))

    pods.extend([
        PodSpec("p1", "passenger", "1"),
        PodSpec("p2", "passenger", "2"),
        PodSpec("p3", "passenger", "5"),
        PodSpec("p4", "passenger", "3"),
    ])

    pods.extend([
        PodSpec("p5", "passenger", "9"),
        PodSpec("p6", "passenger", "13"),
        PodSpec("p7", "passenger", "22"),
        PodSpec("p8", "passenger", "27"),
        PodSpec("p9", "passenger", "6"),
        PodSpec("p10", "passenger", "16"),
        PodSpec("p11", "passenger", "20"),
        PodSpec("p12", "passenger", "28"),
        PodSpec("p13", "passenger", "12"),
        PodSpec("p14", "passenger", "25"),
    ])

    pods.extend([
        PodSpec("c1", "cargo", "4"),
        PodSpec("c2", "cargo", "14"),
        PodSpec("c3", "cargo", "23"),
        PodSpec("c4", "cargo", "19"),
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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            p1 = sut.pod("p1")
            p2 = sut.pod("p2")
            p1_passengers_before = len(p1.passengers)
            p2_passengers_before = len(p2.passengers)

            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            await _seed_and_announce_passenger(
                sut, "test_pax_inject", origin, destination
            )
            delivery_event = await _wait_for_delivery(
                sut, "test_pax_inject", timeout_seconds=60.0
            )

            assert delivery_event["station_id"] == destination

            assert delivery_event["pod_id"] == "p1"

            assert not any(
                p.get("passenger_id") == "test_pax_inject"
                for p in p1.passengers
            )

            assert not any(
                p.get("passenger_id") == "test_pax_inject"
                for p in p2.passengers
            )

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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

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

            await asyncio.sleep(1.0)

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

            assert p1.current_route is None or p1.current_route.stations == [destination], (
                f"Expected no active route or idle-route at {destination}, "
                f"got {p1.current_route}"
            )

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_docked_pod_picksup_all_payloads_to_full_capacity_before_calculating_route(self, aexis_sys: SystemUnderTest):

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            p1 = sut.pod("p1")
            p2 = sut.pod("p2")
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            passenger_ids = [f"flood_pax_{i}" for i in range(5)]

            for pid in passenger_ids:
                key = f"aexis:station:{origin}:passengers"
                data = json.dumps({
                    "passenger_id": pid,
                    "destination": destination,
                    "priority": 3,
                    "arrival_time": datetime.now(UTC).isoformat(),
                })
                await sut.redis.hset(key, pid, data)

            await _seed_and_announce_passenger(
                sut, passenger_ids[0], origin, destination
            )

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

            p1_pickup_events = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("pod_id") == "p1"
                and ev.get("message", {}).get("passenger_id", "").startswith("flood_pax_")
            ]
            assert len(p1_pickup_events) == 5, (
                f"Expected p1 to pick up 5 flood passengers, got {len(p1_pickup_events)}"
            )

            p2_pickup_events = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("pod_id") == "p2"
                and ev.get("message", {}).get("passenger_id", "").startswith("flood_pax_")
            ]
            assert len(p2_pickup_events) == 0, (
                f"Expected p2 to claim none of the flood passengers, "
                f"got {len(p2_pickup_events)}"
            )

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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            origin_p1 = "1"
            origin_p2 = "2"

            station_ids = list(sut.stations.keys())
            convergence = "3" if "3" in sut.stations else station_ids[2]
            final_dest = origin_p1

            await _seed_and_announce_passenger(
                sut, "converge_pax_p1", origin_p1, convergence
            )

            await _seed_and_announce_passenger(
                sut, "converge_pax_p2", origin_p2, convergence
            )

            key = f"aexis:station:{convergence}:passengers"
            data = json.dumps({
                "passenger_id": "contested_pax",
                "destination": final_dest,
                "priority": 3,
                "arrival_time": datetime.now(UTC).isoformat(),
            })
            await sut.redis.hset(key, "contested_pax", data)

            await _wait_for_delivery(sut, "converge_pax_p1", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "converge_pax_p2", timeout_seconds=60.0)

            await _wait_for_delivery(sut, "contested_pax", timeout_seconds=60.0)

            contested_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("passenger_id") == "contested_pax"
            ]
            assert len(contested_pickups) == 1, (
                f"Expected exactly 1 pickup of contested_pax, got {len(contested_pickups)}"
            )

            contested_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "contested_pax"
            ]
            assert len(contested_deliveries) == 1, (
                f"Expected exactly 1 delivery of contested_pax, got {len(contested_deliveries)}"
            )

            assert contested_deliveries[0]["message"]["station_id"] == final_dest

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_demand_injected_at_intermediate_station_while_pod_passes_through(self, aexis_sys: SystemUnderTest):

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

            intermediate = "2" if "2" in sut.stations else next(
                sid for sid in sut.stations
                if sid != origin and sid != destination
            )

            await _seed_and_announce_passenger(
                sut, "primary_pax", origin, destination
            )

            await asyncio.sleep(0.5)

            await _seed_and_announce_passenger(
                sut, "intermediate_pax", intermediate, origin
            )

            await _wait_for_delivery(sut, "primary_pax", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "intermediate_pax", timeout_seconds=60.0)

            primary_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "primary_pax"
            ]
            assert len(primary_deliveries) == 1
            assert primary_deliveries[0]["message"]["station_id"] == destination

            secondary_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "intermediate_pax"
            ]
            assert len(secondary_deliveries) == 1
            assert secondary_deliveries[0]["message"]["station_id"] == origin

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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            station_ids = sorted(sut.stations.keys())
            chain = station_ids[:4]

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

            await _seed_and_announce_passenger(
                sut, chain_pax[0], chain[0], chain[1]
            )

            for pid in chain_pax:
                await _wait_for_delivery(sut, pid, timeout_seconds=90.0)

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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            p1 = sut.pod("p1")
            p2 = sut.pod("p2")
            origin = "1"
            p2_station = "2"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations
                if sid != origin and sid != p2_station
            )

            await _seed_and_announce_passenger(
                sut, "decoy_pax", origin, destination
            )

            await asyncio.sleep(0.3)

            await _seed_and_announce_passenger(
                sut, "contested_local", p2_station, destination
            )

            await _wait_for_delivery(sut, "contested_local", timeout_seconds=60.0)

            contested_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("passenger_id") == "contested_local"
            ]
            assert len(contested_pickups) == 1, (
                f"Expected exactly 1 pickup of contested_local, got {len(contested_pickups)}"
            )

            claiming_pod = contested_pickups[0]["message"]["pod_id"]
            assert claiming_pod in ("p1", "p2"), (
                f"Unexpected pod {claiming_pod} claimed contested_local"
            )

            contested_deliveries = [
                ev for ev in sut.events_of_type("PassengerDelivered")
                if ev.get("message", {}).get("passenger_id") == "contested_local"
            ]
            assert len(contested_deliveries) == 1
            assert contested_deliveries[0]["message"]["station_id"] == destination

            await _wait_for_delivery(sut, "decoy_pax", timeout_seconds=60.0)

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_payload_injected_at_station_pod_is_departing_from(self, aexis_sys: SystemUnderTest):

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            origin = "1"
            dest_a = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )
            dest_b = "3" if "3" in sut.stations else next(
                sid for sid in sut.stations
                if sid != origin and sid != dest_a
            )

            await _seed_and_announce_passenger(
                sut, "departure_pax_a", origin, dest_a
            )

            p1 = sut.pod("p1")
            deadline = asyncio.get_event_loop().time() + 30.0
            while asyncio.get_event_loop().time() < deadline:
                if p1.status in (PodStatus.EN_ROUTE, PodStatus.LOADING):
                    break
                await asyncio.sleep(0.05)

            await _seed_and_announce_passenger(
                sut, "departure_pax_b", origin, dest_b
            )

            await _wait_for_delivery(sut, "departure_pax_a", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "departure_pax_b", timeout_seconds=60.0)

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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

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

            spokes = hub_node.neighbors[:3]

            if len(spokes) < 3:
                extras = [
                    sid for sid in sut.stations
                    if sid != hub_id and sid not in spokes
                ]
                spokes.extend(extras[:3 - len(spokes)])

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

            p1_start = next(
                spec.start_station for spec in sut.config.pods
                if spec.pod_id == "p1"
            )
            p2_start = next(
                spec.start_station for spec in sut.config.pods
                if spec.pod_id == "p2"
            )

            if p1_start != hub_id:
                await _seed_and_announce_passenger(
                    sut, "hub_decoy_p1", p1_start, hub_id
                )
            if p2_start != hub_id:
                await _seed_and_announce_passenger(
                    sut, "hub_decoy_p2", p2_start, hub_id
                )

            for pid in hub_pax_ids:
                await _wait_for_delivery(sut, pid, timeout_seconds=90.0)

            for pid in hub_pax_ids:
                pickups = [
                    ev for ev in sut.events_of_type("PassengerPickedUp")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(pickups) == 1, (
                    f"Expected exactly 1 pickup for {pid}, got {len(pickups)}"
                )

            for i, pid in enumerate(hub_pax_ids):
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1, (
                    f"Expected exactly 1 delivery for {pid}, got {len(deliveries)}"
                )
                assert deliveries[0]["message"]["station_id"] == spokes[i]

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

        sut = aexis_sys

        if not sut.cargo_pods:
            pytest.skip("No cargo pods in this scenario")

        tasks = await _run_system(sut)
        try:

            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

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

            await _seed_and_announce_passenger(
                sut, pax_ids[0], origin, destination
            )
            await _seed_and_announce_cargo(
                sut, cargo_ids[0], origin, destination, 50.0
            )

            for pid in pax_ids:
                await _wait_for_delivery(sut, pid, timeout_seconds=90.0)
            for cid in cargo_ids:
                await _wait_for_cargo_delivery(sut, cid, timeout_seconds=90.0)

            for pid in pax_ids:
                deliveries = [
                    ev for ev in sut.events_of_type("PassengerDelivered")
                    if ev.get("message", {}).get("passenger_id") == pid
                ]
                assert len(deliveries) == 1
                assert deliveries[0]["message"]["station_id"] == destination

                delivering_pod_id = deliveries[0]["message"]["pod_id"]
                delivering_pod = sut.pods[delivering_pod_id]
                assert isinstance(delivering_pod, PassengerPod), (
                    f"Passenger {pid} delivered by non-passenger pod {delivering_pod_id}"
                )

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

            for pod in sut.passenger_pods:
                assert not hasattr(pod, "cargo") or len(pod.cargo) == 0
            for pod in sut.cargo_pods:
                assert not hasattr(pod, "passengers") or len(pod.passengers) == 0

        finally:
            for pod in sut.pods.values():
                await pod.stop()
            await _cancel_tasks(tasks)

    async def test_round_trip_demand_does_not_create_infinite_routing_loop(self, aexis_sys: SystemUnderTest):

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            station_a = "1"
            station_b = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != station_a
            )

            key_a = f"aexis:station:{station_a}:passengers"
            data_a = json.dumps({
                "passenger_id": "roundtrip_pax_a",
                "destination": station_b,
                "priority": 3,
                "arrival_time": datetime.now(UTC).isoformat(),
            })
            await sut.redis.hset(key_a, "roundtrip_pax_a", data_a)

            key_b = f"aexis:station:{station_b}:passengers"
            data_b = json.dumps({
                "passenger_id": "roundtrip_pax_b",
                "destination": station_a,
                "priority": 3,
                "arrival_time": datetime.now(UTC).isoformat(),
            })
            await sut.redis.hset(key_b, "roundtrip_pax_b", data_b)

            await _seed_and_announce_passenger(
                sut, "roundtrip_pax_a", station_a, station_b
            )

            await _wait_for_delivery(sut, "roundtrip_pax_a", timeout_seconds=60.0)
            await _wait_for_delivery(sut, "roundtrip_pax_b", timeout_seconds=60.0)

            await asyncio.sleep(1.5)

            p1 = sut.pod("p1")

            assert p1.status == PodStatus.IDLE, (
                f"Expected p1 IDLE after round-trip, got {p1.status}"
            )

            stranded = [
                p for p in p1.passengers
                if p.get("passenger_id", "").startswith("roundtrip_pax_")
            ]
            assert len(stranded) == 0, (
                f"p1 still carrying {stranded} after round-trip delivery"
            )

            assert p1.current_route is None or len(p1.current_route.stations) <= 1, (
                f"p1 has phantom route {p1.current_route} after all demand exhausted"
            )

            assert p1.location_descriptor.location_type == "station", (
                f"p1 is on edge {p1.location_descriptor.edge_id} after settling"
            )

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

        sut = aexis_sys
        tasks = await _run_system(sut)
        try:

            p1 = sut.pod("p1")
            m = p1.capacity
            n = m - k
            origin = "1"
            destination = "4" if "4" in sut.stations else next(
                sid for sid in sut.stations if sid != origin
            )

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

            await _seed_and_announce_passenger(
                sut, pax_ids[0], origin, destination
            )

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

            p1_pickups = [
                ev for ev in sut.events_of_type("PassengerPickedUp")
                if ev.get("message", {}).get("pod_id") == "p1"
                and ev.get("message", {}).get("passenger_id", "").startswith("cap_pax_")
            ]
            assert len(p1_pickups) <= m, (
                f"p1 picked up {len(p1_pickups)} passengers, exceeding capacity {m}"
            )

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

            for ev in all_deliveries:
                assert ev.get("message", {}).get("station_id") == destination, (
                    f"Passenger {ev['message']['passenger_id']} delivered at "
                    f"{ev['message']['station_id']}, expected {destination}"
                )

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

@pytest.fixture
async def gungir_sys():
    sut = await _build_system(SIMPLE)
    yield sut
    await _teardown_system(sut)

class TestPodCoordinationBugs:

    async def test_pods_settle_to_idle_after_all_deliveries_complete(self, gungir_sys: SystemUnderTest):

        sut = gungir_sys
        tasks = await _run_system(sut)
        try:

            await _seed_and_announce_passenger(sut, "phantom_pax", "1", "4")

            delivered = await _wait_for_delivery(sut, "phantom_pax", timeout_seconds=60.0)
            assert delivered, "Passenger was never delivered"

            decisions_at_delivery = len(sut.events_of_type("PodDecision"))

            await asyncio.sleep(15.0)

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

            assert p1.current_route is None, (
                f"p1 still has route {p1.current_route.stations} after settling"
            )

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

        sut = gungir_sys
        tasks = await _run_system(sut)
        try:

            for i in range(3):
                await _seed_and_announce_passenger(
                    sut, f"flipflop_pax_{i}", "1", "4"
                )

            deadline = asyncio.get_event_loop().time() + 30.0
            while asyncio.get_event_loop().time() < deadline:
                p1 = sut.pod("p1")
                if p1.status == PodStatus.EN_ROUTE and p1.current_route:
                    break
                await asyncio.sleep(0.1)

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

        sut = gungir_sys
        tasks = await _run_system(sut)
        try:

            await _seed_and_announce_passenger(sut, "remote_pax", "4", "2")

            await asyncio.sleep(2.0)

            delay_logs = sut.logs_containing("delaying claim for remote_pax")
            assert len(delay_logs) >= 1, (
                f"Expected at least 1 delay log, got {len(delay_logs)}: {delay_logs}"
            )

            p1_delay = None
            p2_delay = None
            for log_line in delay_logs:
                if "p1" in log_line:

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

            claimer_id = claiming_pods.pop()
            non_claimer_id = "p2" if claimer_id == "p1" else "p1"
            non_claimer = sut.pod(non_claimer_id)

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
