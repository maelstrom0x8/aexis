import logging
import os
import json
import asyncio

from aexis.core.system import load_network_data
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from ..core.errors import handle_exception
from ..core.system import AexisSystem
from pydantic import BaseModel


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
    """API layer that proxies requests to core system"""

    def __init__(self, system: AexisSystem):
        self.system = system
        self.app = FastAPI(
            title="AEXIS System API",
            description="Core system API for AEXIS transportation network",
            version="1.0.0",
        )
        self.position_subscribers = []  # WebSocket connections for position streaming
        self._position_listener_task = None  # Task for listening to position updates
        self._setup_routes()
        asyncio.create_task(self._start_position_listener())

    def _setup_routes(self):
        """Setup API routes"""

        @self.app.get("/api/system/status")
        async def get_system_status():
            """Get overall system status"""
            try:
                return self.system.get_system_state()
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.get("/api/system/metrics")
        async def get_system_metrics():
            """Get system metrics"""
            try:
                return self.system.metrics
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.get("/api/pods")
        async def get_all_pods():
            """Get all pod states"""
            try:
                return {
                    pod_id: pod.get_state() for pod_id, pod in self.system.pods.items()
                }
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.get("/api/pods/{pod_id}")
        async def get_pod(pod_id: str):
            """Get specific pod state"""
            try:
                pod_state = self.system.get_pod_state(pod_id)
                if not pod_state:
                    raise HTTPException(
                        status_code=404, detail="Pod not found")
                return pod_state
            except HTTPException:
                raise
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.get("/api/stations")
        async def get_all_stations():
            """Get all station states"""
            try:
                return {
                    station_id: station.get_state()
                    for station_id, station in self.system.stations.items()
                }
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.get("/api/stations/{station_id}")
        async def get_station(station_id: str):
            """Get specific station state"""
            try:
                station_state = self.system.get_station_state(station_id)
                if not station_state:
                    raise HTTPException(
                        status_code=404, detail="Station not found")
                return station_state
            except HTTPException:
                raise
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.post("/api/manual/passenger")
        async def inject_passenger(payload: PassengerRequestModel):
            try:
                # Input validation with specific error messages
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
                        detail="origin and destination must be different stations",
                    )

                if count <= 0:
                    raise HTTPException(
                        status_code=400, detail="count must be positive"
                    )
                if count > 1000:
                    raise HTTPException(
                        status_code=400,
                        detail="count exceeds maximum (1000 passengers)",
                    )

                # Validate stations exist
                if origin not in self.system.stations:
                    raise HTTPException(
                        status_code=404, detail=f"origin station '{origin}' not found"
                    )
                if dest not in self.system.stations:
                    raise HTTPException(
                        status_code=404,
                        detail=f"destination station '{dest}' not found",
                    )

                await self.system.inject_passenger_request(origin, dest, count)
                return {"status": "success", "message": f"Injected {count} passengers"}
            except HTTPException:
                raise
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.post("/api/manual/cargo")
        async def inject_cargo(payload: CargoRequestModel):
            try:
                # Input validation with specific error messages
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
                        detail="origin and destination must be different stations",
                    )

                # Weight is already validated by Pydantic model
                if weight <= 0:
                    raise HTTPException(
                        status_code=400, detail="weight must be positive"
                    )
                if weight > 100000:
                    raise HTTPException(
                        status_code=400, detail="weight exceeds maximum (100000 kg)"
                    )

                # Validate stations exist
                if origin not in self.system.stations:
                    raise HTTPException(
                        status_code=404, detail=f"origin station '{origin}' not found"
                    )
                if dest not in self.system.stations:
                    raise HTTPException(
                        status_code=404,
                        detail=f"destination station '{dest}' not found",
                    )

                logger.info(
                    f"Injecting cargo: origin={origin}, dest={dest}, weight={weight}kg"
                )

                await self.system.inject_cargo_request(origin, dest, weight)
                return {"status": "success", "message": f"Injected {weight}kg cargo"}
            except HTTPException:
                raise
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.get("/api/network")
        async def get_network():
            """Get network topology data"""
            try:
                data = self.get_network_data()
                if data is None:
                    raise HTTPException(
                        status_code=404, detail="Network data not found"
                    )
                return data
            except HTTPException:
                raise
            except Exception as e:
                error_details = handle_exception(e, "SystemAPI")
                raise HTTPException(
                    status_code=500, detail=error_details.message)

        @self.app.websocket("/api/ws/positions")
        async def websocket_pod_positions(websocket: WebSocket):
            """WebSocket endpoint for streaming pod position updates in real-time

            Clients connect to this endpoint to receive PodPositionUpdate events
            as pods move along network edges.
            """
            await websocket.accept()
            self.position_subscribers.append(websocket)

            try:
                # Keep connection open, receive any client messages (optional)
                while True:
                    data = await websocket.receive_text()
                    # Could handle client commands here if needed
                    if data == "ping":
                        await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                self.position_subscribers.remove(websocket)
                logger.debug("Client disconnected from pod positions stream")
            except Exception as e:
                logger.error(f"WebSocket position stream error: {e}")
                if websocket in self.position_subscribers:
                    self.position_subscribers.remove(websocket)

    async def _start_position_listener(self):
        """Listen to PodPositionUpdate events from message bus and broadcast to WebSocket clients"""
        try:
            await asyncio.sleep(1)
            message_bus = self.system.message_bus
            if not message_bus:
                logger.warning(
                    "Message bus not available for position listener")
                return

            async def position_update_handler(data: dict):
                """Handle PodPositionUpdate events from message bus"""
                try:
                    message = data.get("message", {})
                    event_type = message.get("event_type", "")
                    if event_type == "PodPositionUpdate":
                        await self.broadcast_pod_position(message)
                except Exception as e:
                    logger.debug(f"Error processing position update: {e}")
            message_bus.subscribe("pod_events", position_update_handler)
        except Exception as e:
            logger.warning(f"Position listener error: {e}")

    async def broadcast_pod_position(self, position_data: dict):
        """Broadcast pod position update to all connected WebSocket clients

        Called when PodPositionUpdate events are published
        """
        for websocket in self.position_subscribers[:]:  # Copy list to avoid modification during iteration
            try:
                pod_id = position_data.get("pod_id", "")
                pod_type = "passenger"
                if pod_id in self.system.pods:
                    pod_type = self.system.pods[pod_id].pod_type.value

                await websocket.send_json({
                    "type": "PodPositionUpdate",
                    "data": {
                        **position_data,
                        "pod_type": pod_type
                    }
                })
            except Exception as e:
                logger.debug(f"Failed to send position to client: {e}")
                if websocket in self.position_subscribers:
                    self.position_subscribers.remove(websocket)

    def get_network_data(self) -> dict | None:
        path = os.getenv("AEXIS_NETWORK_DATA", "network.json")
        return load_network_data(path)

    def get_app(self) -> FastAPI:
        """Get FastAPI application instance"""
        return self.app
