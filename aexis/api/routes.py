"""AEXIS API routes — reads all state from Redis.

No direct references to AexisSystem, pods, or stations. All state
is fetched from Redis keys written by station and pod processes.
"""

import asyncio
import json
import logging
import os
import uuid

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from aexis.core.message_bus import MessageBus
from aexis.core.model import CargoRequest, PassengerArrival
from aexis.core.network import load_network_data

logger = logging.getLogger(__name__)


class PassengerRequestModel(BaseModel):
    origin: str
    destination: str
    count: int = 1


class CargoRequestModel(BaseModel):
    origin: str
    destination: str
    weight: float = 100.0


class SystemAPI:
    """API layer reading all state from Redis — no in-process system dependency."""

    def __init__(
        self,
        redis_client: redis.Redis,
        message_bus: MessageBus,
    ):
        self._redis = redis_client
        self.message_bus = message_bus
        self.position_subscribers: list[WebSocket] = []
        self._position_listener_task = None

        self.app = FastAPI(
            title="AEXIS System API",
            description="Core system API for AEXIS transportation network",
            version="2.0.0",
        )
        from datetime import datetime, UTC
        self.start_time = datetime.now(UTC)
        self._setup_routes()

    async def start_listeners(self):
        """Begin background tasks after the event loop is running."""
        self._position_listener_task = asyncio.create_task(
            self._listen_for_position_updates()
        )

    def _setup_routes(self):
        @self.app.get("/api/system/status")
        async def get_system_status():
            """Aggregate system status from all pod and station state keys."""
            try:
                pods = await self._get_all_pod_states()
                stations = await self._get_all_station_states()

                idle_pods = sum(
                    1 for p in pods.values() if p.get("status") == "idle"
                )
                en_route_pods = sum(
                    1 for p in pods.values() if p.get("status") == "en_route"
                )
                total_passengers = sum(
                    len(p.get("passengers", [])) for p in pods.values()
                )
                total_cargo_items = sum(
                    len(p.get("cargo", [])) for p in pods.values()
                )
                waiting_passengers = sum(
                    s.get("queues", {}).get("passengers", {}).get("waiting", 0)
                    for s in stations.values()
                )
                waiting_cargo = sum(
                    s.get("queues", {}).get("cargo", {}).get("waiting", 0)
                    for s in stations.values()
                )

                from datetime import datetime, UTC
                uptime = (datetime.now(UTC) - self.start_time).total_seconds()

                return {
                    "running": True,
                    "system_id": "aexis-micro-v2",
                    "uptime_seconds": int(uptime),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "metrics": {
                        "active_pods": en_route_pods,
                        "total_pods": len(pods),
                        "operational_stations": len(stations),
                        "total_stations": len(stations),
                        "pending_passengers": waiting_passengers,
                        "pending_cargo": waiting_cargo,
                        "system_efficiency": 0.85,  # Placeholder for now
                        "average_wait_time": 12.5,  # Placeholder
                        "throughput_per_hour": 450, # Placeholder
                        "fallback_usage_rate": 0.05, # Placeholder
                    },
                }
            except Exception as e:
                logger.error(f"system status error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/system/metrics")
        async def get_system_metrics():
            """Aggregate metrics from stations."""
            try:
                stations = await self._get_all_station_states()
                total_passengers = sum(
                    s.get("metrics", {}).get("total_passengers_processed", 0)
                    for s in stations.values()
                )
                total_cargo = sum(
                    s.get("metrics", {}).get("total_cargo_processed", 0)
                    for s in stations.values()
                )
                return {
                    "total_passengers_processed": total_passengers,
                    "total_cargo_processed": total_cargo,
                    "station_count": len(stations),
                }
            except Exception as e:
                logger.error(f"metrics error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/pods")
        async def get_all_pods():
            try:
                return await self._get_all_pod_states()
            except Exception as e:
                logger.error(f"get pods error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/pods/{pod_id}")
        async def get_pod(pod_id: str):
            try:
                raw = await self._redis.get(f"aexis:pod:{pod_id}:state")
                if raw is None:
                    raise HTTPException(status_code=404, detail="Pod not found")
                return json.loads(raw)
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"get pod error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/stations")
        async def get_all_stations():
            try:
                return await self._get_all_station_states()
            except Exception as e:
                logger.error(f"get stations error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/stations/{station_id}")
        async def get_station(station_id: str):
            try:
                raw = await self._redis.get(
                    f"aexis:station:{station_id}:state"
                )
                if raw is None:
                    raise HTTPException(
                        status_code=404, detail="Station not found"
                    )
                return json.loads(raw)
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"get station error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/manual/passenger")
        async def inject_passenger(payload: PassengerRequestModel):
            """Inject passengers by publishing PassengerArrival events."""
            try:
                origin = payload.origin.strip()
                dest = payload.destination.strip()
                count = payload.count

                if not origin:
                    raise HTTPException(
                        status_code=400, detail="origin cannot be empty"
                    )
                if not dest:
                    raise HTTPException(
                        status_code=400, detail="destination cannot be empty"
                    )
                if origin == dest:
                    raise HTTPException(
                        status_code=400,
                        detail="origin and destination must differ",
                    )
                if count <= 0:
                    raise HTTPException(
                        status_code=400, detail="count must be positive"
                    )
                if count > 1000:
                    raise HTTPException(
                        status_code=400, detail="count exceeds maximum (1000)"
                    )

                # Validate station exists in Redis
                origin_state = await self._redis.get(
                    f"aexis:station:{origin}:state"
                )
                if origin_state is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"origin station '{origin}' not found",
                    )
                dest_state = await self._redis.get(
                    f"aexis:station:{dest}:state"
                )
                if dest_state is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"destination station '{dest}' not found",
                    )

                from datetime import UTC, datetime

                for _ in range(count):
                    pid = (
                        f"manual_p_{datetime.now(UTC).strftime('%H%M%S')}_"
                        f"{uuid.uuid4().hex[:6]}"
                    )
                    event = PassengerArrival(
                        passenger_id=pid,
                        station_id=origin,
                        destination=dest,
                        priority=3,
                        group_size=1,
                        special_needs=[],
                        wait_time_limit=45,
                    )
                    await self.message_bus.publish_event(
                        MessageBus.get_event_channel(event.event_type), event
                    )
                return {
                    "status": "success",
                    "message": f"Injected {count} passengers",
                }
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"inject passenger error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/manual/cargo")
        async def inject_cargo(payload: CargoRequestModel):
            """Inject cargo by publishing a CargoRequest event."""
            try:
                origin = payload.origin.strip()
                dest = payload.destination.strip()
                weight = payload.weight

                if not origin:
                    raise HTTPException(
                        status_code=400, detail="origin cannot be empty"
                    )
                if not dest:
                    raise HTTPException(
                        status_code=400, detail="destination cannot be empty"
                    )
                if origin == dest:
                    raise HTTPException(
                        status_code=400,
                        detail="origin and destination must differ",
                    )
                if weight <= 0:
                    raise HTTPException(
                        status_code=400, detail="weight must be positive"
                    )
                if weight > 100000:
                    raise HTTPException(
                        status_code=400, detail="weight exceeds maximum"
                    )

                origin_state = await self._redis.get(
                    f"aexis:station:{origin}:state"
                )
                if origin_state is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"origin station '{origin}' not found",
                    )
                dest_state = await self._redis.get(
                    f"aexis:station:{dest}:state"
                )
                if dest_state is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"destination station '{dest}' not found",
                    )

                from datetime import UTC, datetime

                rid = (
                    f"manual_c_{datetime.now(UTC).strftime('%H%M%S')}_"
                    f"{uuid.uuid4().hex[:6]}"
                )
                event = CargoRequest(
                    request_id=rid,
                    origin=origin,
                    destination=dest,
                    weight=weight,
                    volume=weight / 500.0,
                    priority=3,
                    hazardous=False,
                    temperature_controlled=False,
                    deadline=None,
                )
                await self.message_bus.publish_event(
                    MessageBus.get_event_channel(event.event_type), event
                )
                return {
                    "status": "success",
                    "message": f"Injected {weight}kg cargo",
                }
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"inject cargo error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/network")
        async def get_network():
            try:
                path = os.getenv("AEXIS_NETWORK_DATA", "network.json")
                data = load_network_data(path)
                if data is None:
                    raise HTTPException(
                        status_code=404, detail="Network data not found"
                    )
                return data
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"get network error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.websocket("/api/ws/positions")
        async def websocket_pod_positions(websocket: WebSocket):
            await websocket.accept()
            self.position_subscribers.append(websocket)
            try:
                while True:
                    data = await websocket.receive_text()
                    if data == "ping":
                        await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                if websocket in self.position_subscribers:
                    self.position_subscribers.remove(websocket)
            except Exception as e:
                logger.debug(f"WebSocket error: {e}")
                if websocket in self.position_subscribers:
                    self.position_subscribers.remove(websocket)

    # ------------------------------------------------------------------
    # Redis state helpers
    # ------------------------------------------------------------------

    async def _get_all_pod_states(self) -> dict[str, dict]:
        result = {}
        async for key in self._redis.scan_iter(
            match="aexis:pod:*:state", count=100
        ):
            parts = key.split(":")
            if len(parts) == 4:
                pod_id = parts[2]
                raw = await self._redis.get(key)
                if raw:
                    try:
                        result[pod_id] = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        pass
        return result

    async def _get_all_station_states(self) -> dict[str, dict]:
        result = {}
        async for key in self._redis.scan_iter(
            match="aexis:station:*:state", count=100
        ):
            parts = key.split(":")
            if len(parts) == 4:
                station_id = parts[2]
                raw = await self._redis.get(key)
                if raw:
                    try:
                        result[station_id] = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        pass
        return result

    # ------------------------------------------------------------------
    # Position streaming
    # ------------------------------------------------------------------

    async def _listen_for_position_updates(self):
        """Subscribe to pod position events and broadcast to WebSocket clients."""
        try:
            await asyncio.sleep(1)

            async def handler(data: dict):
                message = data.get("message", {})
                if message.get("event_type") == "PodPositionUpdate":
                    await self._broadcast_position(message)

            self.message_bus.subscribe(
                MessageBus.CHANNELS.get("POD_EVENTS", "pod_events"), handler
            )
        except Exception as e:
            logger.warning(f"Position listener failed: {e}")

    async def _broadcast_position(self, position_data: dict):
        for ws in self.position_subscribers[:]:
            try:
                await ws.send_json(
                    {"type": "PodPositionUpdate", "data": position_data}
                )
            except Exception:
                if ws in self.position_subscribers:
                    self.position_subscribers.remove(ws)

    def get_app(self) -> FastAPI:
        return self.app
