import json
import math
import os
import random
from dataclasses import dataclass, field
from typing import Any

import networkx as nx
from .model import Coordinate, EdgeSegment

@dataclass
class NetworkAdjacency:
    node_id: str
    weight: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {"node_id": self.node_id, "weight": self.weight}

@dataclass
class NetworkNode:
    id: str
    label: str
    coordinate: dict[str, float]
    adj: list[NetworkAdjacency] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "coordinate": self.coordinate,
            "adj": [a.to_dict() for a in self.adj],
        }

@dataclass
class Network:
    nodes: list[NetworkNode] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"nodes": [n.to_dict() for n in self.nodes]}

def load_network_data(path: str) -> dict[str, Any] | None:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Network file not found: {path}")
        return None
    except Exception as e:
        print(f"Error loading network: {e}")
        return None

class NetworkContext:

    _instance = None

    def __init__(self, network_data: dict | None = None):
        self.network_graph = nx.Graph()
        self.station_positions = {}
        self.edges: dict[str, EdgeSegment] = {}

        self.stations = {}

        if not network_data:

            try:

                path = os.getenv("AEXIS_NETWORK_DATA", "")
                if os.path.exists(path):
                    network_data = load_network_data(path)
            except Exception as e:
                print(f"Missing network data file or failed to load: {e}")

        if network_data:
            self._initialize_from_data(network_data)
        else:

            print("Warning: NetworkContext initialized with empty network.")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def set_instance(cls, instance):
        cls._instance = instance

    def _initialize_from_data(self, data: dict):
        if "nodes" not in data:
            return

        for node in data["nodes"]:

            station_id = str(node['id'])

            coord = node.get("coordinate", {"x": 0, "y": 0})
            pos = (coord["x"], coord["y"])
            self.station_positions[station_id] = pos
            self.network_graph.add_node(station_id, pos=pos)

            for adj in node.get("adj", []):
                target_id = str(adj['node_id'])
                weight = adj.get("weight", 1.0)
                self.network_graph.add_edge(
                    station_id, target_id, weight=weight)

        self._build_edge_segments()

    def _build_edge_segments(self):
        for u, v in self.network_graph.edges():
            pos_u = self.station_positions.get(u, (0, 0))
            pos_v = self.station_positions.get(v, (0, 0))

            coord_u = Coordinate(pos_u[0], pos_u[1])
            coord_v = Coordinate(pos_v[0], pos_v[1])

            edge_id_forward = f"{u}->{v}"
            edge_id_backward = f"{v}->{u}"

            seg_forward = EdgeSegment(
                segment_id=edge_id_forward,
                start_node=u,
                end_node=v,
                start_coord=coord_u,
                end_coord=coord_v
            )
            seg_backward = EdgeSegment(
                segment_id=edge_id_backward,
                start_node=v,
                end_node=u,
                start_coord=coord_v,
                end_coord=coord_u
            )

            self.edges[edge_id_forward] = seg_forward
            self.edges[edge_id_backward] = seg_backward

    def spawn_pod_at_random_edge(self) -> tuple[str, Coordinate, float]:
        if not self.edges:

            station_id = list(self.station_positions.keys())[
                0] if self.station_positions else "1"
            pos = self.station_positions.get(station_id, (0, 0))
            return station_id, Coordinate(pos[0], pos[1]), 0.0

        edge_id = random.choice(list(self.edges.keys()))
        edge = self.edges[edge_id]

        distance_on_edge = random.uniform(0.1 * edge.length, 0.9 * edge.length)
        coord = edge.get_point_at_distance(distance_on_edge)

        return edge_id, coord, distance_on_edge

    def get_random_station(self) -> str:
        if not self.station_positions:
            return "1"
        return random.choice(list(self.station_positions.keys()))

    def get_nearest_station(self, coordinate: Coordinate) -> str:
        if not self.station_positions:
            return "1"

        nearest_station = None
        nearest_distance = float('inf')

        for station_id, pos in self.station_positions.items():
            station_coord = Coordinate(pos[0], pos[1])
            distance = coordinate.distance_to(station_coord)
            if distance < nearest_distance:
                nearest_distance = distance
                nearest_station = station_id

        return nearest_station or "1"

    def _initialize_default(self):
        pass

    def calculate_distance(self, station1: str, station2: str) -> float:
        pos1 = self.station_positions.get(station1, (0, 0))
        pos2 = self.station_positions.get(station2, (0, 0))
        return math.sqrt((pos1[0] - pos2[0]) ** 2 + (pos1[1] - pos2[1]) ** 2)

    def get_route_distance(self, route: list[str]) -> float:
        total_distance = 0.0
        for i in range(len(route) - 1):
            try:

                if self.network_graph.has_edge(route[i], route[i + 1]):
                    total_distance += self.network_graph[route[i]][route[i + 1]][
                        "weight"
                    ]
                else:

                    total_distance += self.calculate_distance(
                        route[i], route[i + 1])
            except Exception:
                total_distance += self.calculate_distance(
                    route[i], route[i + 1])
        return total_distance
