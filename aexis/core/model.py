from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

class PodStatus(Enum):
    IDLE = "idle"
    LOADING = "loading"
    EN_ROUTE = "en_route"
    UNLOADING = "unloading"
    MAINTENANCE = "maintenance"

class StationStatus(Enum):
    OPERATIONAL = "operational"
    CONGESTED = "congested"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"

class Priority(Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5

@dataclass
class Event:
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    source: str = ""
    data: dict[str, Any] = field(default_factory=dict)

@dataclass
class PassengerArrival(Event):
    event_type: str = "PassengerArrival"
    passenger_id: str = ""
    station_id: str = ""
    destination: str = ""
    priority: int = Priority.NORMAL.value
    group_size: int = 1
    special_needs: list[str] = field(default_factory=list)
    wait_time_limit: int = 30

@dataclass
class PassengerPickedUp(Event):
    event_type: str = "PassengerPickedUp"
    passenger_id: str = ""
    pod_id: str = ""
    station_id: str = ""
    pickup_time: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))

@dataclass
class PassengerDelivered(Event):
    event_type: str = "PassengerDelivered"
    passenger_id: str = ""
    pod_id: str = ""
    station_id: str = ""
    delivery_time: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    total_travel_time: int = 0
    satisfaction_score: float = 0.0

@dataclass
class CargoRequest(Event):
    event_type: str = "CargoRequest"
    request_id: str = ""
    origin: str = ""
    destination: str = ""
    weight: float = 0.0
    volume: float = 0.0
    priority: int = Priority.NORMAL.value
    hazardous: bool = False
    temperature_controlled: bool = False
    deadline: datetime | None = None

@dataclass
class CargoLoaded(Event):
    event_type: str = "CargoLoaded"
    request_id: str = ""
    pod_id: str = ""
    station_id: str = ""
    load_time: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))

@dataclass
class CargoDelivered(Event):
    event_type: str = "CargoDelivered"
    request_id: str = ""
    pod_id: str = ""
    station_id: str = ""
    delivery_time: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    condition: str = "good"
    on_time: bool = True

@dataclass
class PodStatusUpdate(Event):
    event_type: str = "PodStatusUpdate"
    pod_id: str = ""
    location: str = ""
    status: PodStatus = PodStatus.IDLE
    capacity_used: int = 0
    capacity_total: int = 0
    weight_used: float = 0.0
    weight_total: float = 0.0

    current_route: Optional["Route"] = None

@dataclass
class PodDecision(Event):
    event_type: str = "PodDecision"
    pod_id: str = ""
    decision_type: str = ""
    decision: dict[str, Any] = field(default_factory=dict)
    reasoning: str = ""
    confidence: float = 0.0
    fallback_used: bool = False

@dataclass
class CongestionAlert(Event):
    event_type: str = "CongestionAlert"
    station_id: str = ""
    congestion_level: float = 0.0
    queue_length: int = 0
    average_wait_time: float = 0.0
    affected_routes: list[str] = field(default_factory=list)
    estimated_clear_time: datetime | None = None
    severity: str = "low"

@dataclass
class SystemSnapshot(Event):
    event_type: str = "SystemSnapshot"
    snapshot_id: str = field(
        default_factory=lambda: f"snap_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    system_state: dict[str, Any] = field(default_factory=dict)

@dataclass
class Command(Event):
    command_id: str = field(default_factory=lambda: str(uuid4()))
    command_type: str = ""
    target: str = ""
    target_type: str = "system"
    parameters: dict[str, Any] = field(default_factory=dict)

@dataclass
class AssignRoute(Command):
    command_type: str = "AssignRoute"
    target_type: str = "pod"
    target_pod: str = ""
    route: list[str] = field(default_factory=list)
    priority: int = Priority.NORMAL.value
    deadline: datetime | None = None

@dataclass
class UpdateCapacity(Command):
    command_type: str = "UpdateCapacity"
    target_type: str = "station"
    target_station: str = ""
    max_pods: int = 0
    processing_rate: float = 0.0

@dataclass
class Passenger:
    passenger_id: str
    origin: str
    destination: str
    priority: Priority
    group_size: int = 1
    special_needs: list[str] = field(default_factory=list)
    arrival_time: datetime = field(default_factory=datetime.utcnow)
    wait_time_limit: int = 30
    pickup_time: datetime | None = None
    delivery_time: datetime | None = None
    assigned_pod: str | None = None

@dataclass
class Cargo:
    request_id: str
    origin: str
    destination: str
    weight: float
    volume: float
    priority: Priority
    hazardous: bool = False
    temperature_controlled: bool = False
    deadline: datetime | None = None
    arrival_time: datetime = field(default_factory=datetime.utcnow)
    load_time: datetime | None = None
    delivery_time: datetime | None = None
    assigned_pod: str | None = None

@dataclass
class Route:
    route_id: str
    stations: list[str]
    estimated_duration: int = 0
    distance: float = 0.0
    traffic_level: float = 0.0
    congestion_factor: float = 1.0

@dataclass
class DecisionContext:
    pod_id: str
    current_location: str
    current_route: Optional["Route"]
    capacity_available: int
    weight_available: float

    available_requests: list[dict[str, Any]]
    network_state: dict[str, Any]
    system_metrics: dict[str, Any]

    pod_type: str = "unknown"
    pod_constraints: dict[str, Any] = None
    specialization: str = "general"

    passengers: list[dict[str, Any]] = None
    cargo: list[dict[str, Any]] = None

@dataclass
class Decision:
    decision_type: str
    accepted_requests: list[str]
    rejected_requests: list[str]
    route: list[str]
    estimated_duration: int
    confidence: float
    reasoning: str
    fallback_used: bool = False

@dataclass
class DecisionOutcome:
    decision_id: str
    actual_duration: int
    efficiency_score: float
    passenger_satisfaction: float
    cargo_on_time_rate: float
    lessons_learned: list[str] = field(default_factory=list)

@dataclass
class Coordinate:
    x: float
    y: float

    def distance_to(self, other: "Coordinate") -> float:
        import math
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def interpolate(self, other: "Coordinate", t: float) -> "Coordinate":
        t = max(0.0, min(1.0, t))
        return Coordinate(
            x=self.x + (other.x - self.x) * t,
            y=self.y + (other.y - self.y) * t
        )

@dataclass
class EdgeSegment:
    segment_id: str
    start_node: str
    end_node: str
    start_coord: Coordinate
    end_coord: Coordinate
    length: float = 0.0

    def __post_init__(self):
        if self.length == 0.0:
            self.length = self.start_coord.distance_to(self.end_coord)

    def get_point_at_distance(self, distance: float) -> Coordinate:
        if self.length == 0.0:
            return self.start_coord
        t = min(1.0, max(0.0, distance / self.length))
        return self.start_coord.interpolate(self.end_coord, t)

@dataclass
class LocationDescriptor:
    location_type: str
    node_id: str = ""
    edge_id: str = ""
    coordinate: Coordinate = field(default_factory=lambda: Coordinate(0, 0))
    distance_on_edge: float = 0.0

    def __hash__(self):
        if self.location_type == "station":
            return hash(("station", self.node_id))
        return hash(("edge", self.edge_id, self.distance_on_edge))

@dataclass
class PodPositionUpdate(Event):
    event_type: str = "PodPositionUpdate"
    pod_id: str = ""
    location: LocationDescriptor = field(
        default_factory=lambda: LocationDescriptor("station", "unknown"))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    status: str = "idle"
    speed: float = 0.0
    current_route: Optional[list[str]] = None

@dataclass
class PodArrival(Event):
    event_type: str = "PodArrival"
    pod_id: str = ""
    station_id: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
