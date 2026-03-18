
import json
import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

class KnowledgeBaseManager:

    def __init__(self):
        region = os.environ.get("DO_SPACES_REGION", "nyc3")
        spaces_key = os.environ.get("DO_SPACES_KEY", "")
        spaces_secret = os.environ.get("DO_SPACES_SECRET", "")
        self._bucket_name = os.environ.get("DO_SPACES_BUCKET", "")

        if not spaces_key or not spaces_secret:
            logger.warning(
                "DO_SPACES_KEY / DO_SPACES_SECRET not set — "
                "KnowledgeBaseManager will not be able to upload data"
            )
            self._client = None
            return

        self._client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=f"https://{region}.digitaloceanspaces.com",
            aws_access_key_id=spaces_key,
            aws_secret_access_key=spaces_secret,
        )

    @property
    def is_configured(self) -> bool:
        return self._client is not None and bool(self._bucket_name)

    def upload_network_topology(
        self,
        network_data: dict,
        filename: str = "aexis_network_topology.json",
    ) -> bool:
        if not self.is_configured:
            logger.error(
                "Cannot upload network topology: Spaces credentials or bucket not configured"
            )
            return False

        topology_doc = self._build_topology_document(network_data)

        try:
            self._client.put_object(
                Bucket=self._bucket_name,
                Key=filename,
                Body=json.dumps(topology_doc, indent=2),
                ContentType="application/json",
            )
            logger.info(
                "Uploaded network topology (%d stations, %d edges) to "
                "Spaces bucket '%s' as '%s'",
                len(topology_doc.get("stations", [])),
                len(topology_doc.get("edges", [])),
                self._bucket_name,
                filename,
            )
            return True

        except (BotoCoreError, ClientError) as exc:
            logger.error("Failed to upload network topology to Spaces: %s", exc)
            return False

    def upload_station_metadata(
        self,
        station_data: list[dict],
        filename: str = "aexis_station_metadata.json",
    ) -> bool:
        if not self.is_configured:
            logger.error("Cannot upload station metadata: not configured")
            return False

        try:
            self._client.put_object(
                Bucket=self._bucket_name,
                Key=filename,
                Body=json.dumps(station_data, indent=2),
                ContentType="application/json",
            )
            logger.info(
                "Uploaded station metadata (%d stations) to Spaces '%s'",
                len(station_data), self._bucket_name,
            )
            return True

        except (BotoCoreError, ClientError) as exc:
            logger.error("Failed to upload station metadata to Spaces: %s", exc)
            return False

    @staticmethod
    def _build_topology_document(network_data: dict) -> dict:
        stations = []
        edges = []

        nodes = network_data.get("nodes", [])
        for node in nodes:
            station_id = node.get("id", "")
            station_entry = {
                "station_id": station_id,
                "name": node.get("name", station_id),
                "position": node.get("position", {}),
                "capacity": node.get("capacity", {}),
                "description": (
                    f"Station {station_id} is located at position "
                    f"({node.get('position', {}).get('x', 0)}, "
                    f"{node.get('position', {}).get('y', 0)}). "
                    f"It has {node.get('capacity', {}).get('loading_bays', 'unknown')} "
                    f"loading bays."
                ),
            }
            stations.append(station_entry)

        raw_edges = network_data.get("edges", [])
        for edge in raw_edges:
            edge_entry = {
                "from": edge.get("from", ""),
                "to": edge.get("to", ""),
                "weight": edge.get("weight", 1.0),
                "description": (
                    f"Edge from station {edge.get('from', '?')} to "
                    f"station {edge.get('to', '?')} with distance/weight "
                    f"{edge.get('weight', 1.0)}."
                ),
            }
            edges.append(edge_entry)

        return {
            "document_type": "aexis_network_topology",
            "version": "1.0",
            "total_stations": len(stations),
            "total_edges": len(edges),
            "stations": stations,
            "edges": edges,
            "summary": (
                f"AEXIS transit network with {len(stations)} stations and "
                f"{len(edges)} connections. Pods navigate between stations "
                f"to pick up and deliver passengers and cargo."
            ),
        }
