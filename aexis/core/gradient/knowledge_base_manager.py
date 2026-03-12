import os
import logging
import json
import boto3
from typing import List, Optional
from .errors import ErrorCode, create_error

logger = logging.getLogger(__name__)

class KnowledgeBaseManager:
    """Manages DigitalOcean Gradient Knowledge Bases and Spaces sync"""

    def __init__(self, gradient_client):
        self.gradient_client = gradient_client
        self.spaces_client = boto3.client(
            "s3",
            region_name=os.environ.get("DO_SPACES_REGION", "nyc3"),
            endpoint_url=f"https://{os.environ.get('DO_SPACES_REGION', 'nyc3')}.digitaloceanspaces.com",
            aws_access_key_id=os.environ.get("DO_SPACES_KEY"),
            aws_secret_access_key=os.environ.get("DO_SPACES_SECRET"),
        )
        self.bucket_name = os.environ.get("DO_SPACES_BUCKET")

    def sync_network_data(self, network_data: dict, filename: str = "network_topology.json"):
        """Sync local network topology to DigitalOcean Spaces"""
        try:
            if not self.bucket_name:
                raise create_error(
                    ErrorCode.CONFIG_MISSING_ENV_VAR,
                    component="KnowledgeBaseManager",
                    context={"var_name": "DO_SPACES_BUCKET"}
                )

            logger.info(f"Syncing {filename} to Spaces bucket: {self.bucket_name}")
            self.spaces_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=json.dumps(network_data, indent=2),
                ContentType="application/json"
            )
            logger.info(f"Successfully synced {filename} to Spaces")
            
        except Exception as e:
            logger.error(f"Failed to sync network data to Spaces: {e}")
            raise

    def trigger_kb_index(self, kb_id: str):
        """Trigger an indexing job for a Knowledge Base attached to the Spaces bucket"""
        try:
            # Note: The Gradient SDK pattern involves workspace-level KB management
            # kb = self.gradient_client.get_knowledge_base(kb_id=kb_id)
            # await kb.trigger_indexing()
            # For now, we simulate or use the appropriate SDK method
            logger.info(f"Triggering re-indexing for Knowledge Base: {kb_id}")
            # Placeholder for actual indexing trigger via Gradient ADK/SDK
            pass
        except Exception as e:
            logger.error(f"Failed to trigger KB indexing: {e}")
            raise

    def query_kb(self, kb_id: str, query: str) -> List[dict]:
        """Query the Knowledge Base for relevant routing context"""
        try:
            # kb = self.gradient_client.get_knowledge_base(kb_id=kb_id)
            # results = await kb.query(query=query)
            # return results
            logger.info(f"Querying KB {kb_id}: {query}")
            return []
        except Exception as e:
            logger.error(f"KB query failed: {e}")
            return []
