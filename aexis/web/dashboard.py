import asyncio
import json
import logging
import os

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)


class NoCacheStaticFiles(StaticFiles):
    def file_response(self, *args, **kwargs) -> FileResponse:
        response = super().file_response(*args, **kwargs)
        # Force no caching for all static files (especially JS/CSS)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response


class WebDashboard:
    """FastAPI web dashboard - serves SPA and proxies API requests"""

    def __init__(self, api_base_url: str = "http://localhost:8001"):
        self.api_base_url = api_base_url
        self.app = FastAPI(
            title="AEXIS Dashboard",
            description="Autonomous Event-Driven Transportation Intelligence System",
            version="1.0.0",
        )
        self.websocket_connections: list[WebSocket] = []

        # Setup CORS and middleware
        self._setup_middleware()

        # Setup routes
        self._setup_routes()

        # Setup static files
        self._setup_static_files()

    def _setup_middleware(self):
        """Setup CORS and other middleware"""
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def _setup_routes(self):
        """Setup API routes"""

        @self.app.get("/")
        async def index():
            """Serve main dashboard SPA"""
            # Serve the index.html file
            return FileResponse(
                os.path.join(os.path.dirname(__file__), "static", "index.html")
            )

        @self.app.get("/api/system/status")
        async def get_system_status():
            """Proxy system status from API"""
            return await self._proxy_request("GET", "/api/system/status")

        @self.app.get("/api/system/metrics")
        async def get_system_metrics():
            """Proxy system metrics from API"""
            return await self._proxy_request("GET", "/api/system/metrics")

        @self.app.get("/api/network")
        async def get_network():
            """Proxy network topology from API"""
            return await self._proxy_request("GET", "/api/network")

        @self.app.get("/api/pods")
        async def get_all_pods():
            """Proxy all pod states from API"""
            return await self._proxy_request("GET", "/api/pods")

        @self.app.get("/api/pods/{pod_id}")
        async def get_pod(pod_id: str):
            """Proxy specific pod state from API"""
            return await self._proxy_request("GET", f"/api/pods/{pod_id}")

        @self.app.get("/api/stations")
        async def get_all_stations():
            """Proxy all station states from API"""
            return await self._proxy_request("GET", "/api/stations")

        @self.app.get("/api/stations/{station_id}")
        async def get_station(station_id: str):
            """Proxy specific station state from API"""
            return await self._proxy_request("GET", f"/api/stations/{station_id}")

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time updates"""
            await self._handle_websocket(websocket)

        @self.app.websocket("/ws/positions")
        async def websocket_positions_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time pod position updates"""
            await self._handle_positions_websocket(websocket)

        # Generic Proxy for Manual Injection
        @self.app.post("/api/manual/{path:path}")
        async def proxy_post(path: str, request: dict):
            return await self._proxy_request(
                "POST", f"/api/manual/{path}", json_data=request
            )

    async def _proxy_request(
        self, method: str, path: str, json_data: dict | None = None
    ):
        """Generic proxy handler with resilience"""
        try:
            async with httpx.AsyncClient() as client:
                if json_data:
                    response = await client.request(
                        method, f"{self.api_base_url}{path}", json=json_data
                    )
                else:
                    response = await client.request(
                        method, f"{self.api_base_url}{path}"
                    )
                if response.status_code == 404:
                    raise HTTPException(
                        status_code=404, detail="Resource not found")
                return response.json()
        except httpx.ConnectError:
            # Resilience: Return offline status or empty data instead of crashing
            logger.warning(f"Backend API offline: {path}")
            raise HTTPException(status_code=503, detail="System Offline")
        except Exception as e:
            logger.error(f"Proxy error {path}: {e}")
            raise HTTPException(status_code=500, detail="Proxy Error")

    def _setup_static_files(self):
        """Setup static file serving"""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        if not os.path.exists(static_dir):
            os.makedirs(static_dir)
        self.app.mount(
            "/static", NoCacheStaticFiles(directory=static_dir), name="static"
        )

    async def _handle_websocket(self, websocket: WebSocket):
        """Handle WebSocket connections for real-time updates"""
        await websocket.accept()
        self.websocket_connections.append(websocket)
        try:
            # Poll for initial state
            try:
                state = await self._proxy_request("GET", "/api/system/status")
                await websocket.send_text(
                    json.dumps({"type": "system_state", "data": state})
                )
            except:
                pass  # Ignore initial fetch failure

            while True:
                try:
                    # Keep alive / read messages
                    message = await websocket.receive_text()
                    print(f"Got status message {message}")
                    # We could handle client messages here
                except WebSocketDisconnect:
                    break

        except Exception as e:
            logger.debug(f"WebSocket connection error: {e}", exc_info=True)
        finally:
            if websocket in self.websocket_connections:
                self.websocket_connections.remove(websocket)

    async def _handle_positions_websocket(self, websocket: WebSocket):
        """Handle WebSocket connections for real-time pod position updates

        Proxies connections from the dashboard WebSocket to the API's position stream
        """
        await websocket.accept()

        try:
            # Connect to API position stream using websockets library
            import websockets
            
            # Determine API WS URL (replace http/https with ws/wss)
            # Ensure we don't double-slash if base_url ends with /
            api_base = self.api_base_url.rstrip("/")
            api_ws_base = api_base.replace("http://", "ws://").replace("https://", "wss://")
            ws_url = f"{api_ws_base}/api/ws/positions"
            
            logger.info(f"Connecting to upstream position stream: {ws_url}")

            async with websockets.connect(ws_url) as upstream_ws:
                # Create tasks for bidirectional forwarding
                async def forward_upstream_to_client():
                    try:
                        async for message in upstream_ws:
                            await websocket.send_text(message)
                    except Exception as e:
                        logger.debug(f"Error forwarding upstream -> client: {e}")

                async def forward_client_to_upstream():
                    try:
                        while True:
                            message = await websocket.receive_text()
                            await upstream_ws.send(message)
                    except WebSocketDisconnect:
                        pass # Client disconnected
                    except Exception as e:
                        logger.debug(f"Error forwarding client -> upstream: {e}")

                # Run until either disconnects
                # We prioritize upstream_to_client for position updates
                await asyncio.gather(
                    forward_upstream_to_client(),
                    forward_client_to_upstream(),
                    return_exceptions=True
                )

        except Exception as e:
            logger.debug(f"Position stream connection error: {e}")
        finally:
            try:
                await websocket.close()
            except:
                pass

    # Note: In a real production split, we'd need a way to receive events
    # from the Core System (e.g. via Redis PubSub) to broadcast to WebSockets.
    # For now, the client polls via the 'get_state' or we can implement a
    # background task here to poll the API and broadcast.
    # For this refactor, I will add a background poller to simulate push.

    async def start_background_poller(self):
        """Poll API and broadcast updates"""
        while True:
            try:
                if self.websocket_connections:
                    state = await self._proxy_request("GET", "/api/system/status")
                    await self.broadcast({"type": "system_state", "data": state})
            except:
                pass  # meaningful logging is handled in proxy_request

            await asyncio.sleep(2)  # 2s polling interval

    async def broadcast(self, message: dict):
        """Broadcast message to all clients"""
        serialized = json.dumps(message)
        for ws in self.websocket_connections:
            try:
                await ws.send_text(serialized)
            except:
                pass

    async def start_redis_listener(self):
        """Subscribe to Redis channels and forward events to WebSocket clients"""
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        redis_password = os.getenv("REDIS_PASSWORD")

        self._redis_running = True
        redis_client = None
        pubsub = None

        try:
            redis_client = redis.from_url(
                redis_url, password=redis_password, decode_responses=True
            )
            await redis_client.ping()
            logger.info("Dashboard connected to Redis for event forwarding")

            pubsub = redis_client.pubsub()

            # Subscribe to event channels
            channels = [
                "aexis:events:passenger",
                "aexis:events:cargo",
                "aexis:events:pods",
                "aexis:events:system",
            ]

            for channel in channels:
                await pubsub.subscribe(channel)
                logger.info(f"Dashboard subscribed to {channel}")

            # Listen and forward
            while self._redis_running:
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=1.0),
                        timeout=2.0,
                    )
                    if message and message["type"] == "message":
                        try:
                            data = json.loads(message["data"])
                            await self.broadcast(
                                {
                                    "type": "event",
                                    "channel": message["channel"],
                                    "data": data.get("message", data),
                                }
                            )
                        except json.JSONDecodeError:
                            pass
                except TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"Redis listener error: {e}")
        finally:
            # Cleanup
            if pubsub:
                try:
                    await pubsub.unsubscribe()
                    await pubsub.aclose()
                except:
                    pass
            if redis_client:
                try:
                    await redis_client.aclose()
                except:
                    pass

    def get_app(self) -> FastAPI:
        """Get FastAPI application instance"""

        # Start background tasks on startup
        @self.app.on_event("startup")
        async def startup_event():
            asyncio.create_task(self.start_background_poller())
            asyncio.create_task(self.start_redis_listener())

        @self.app.on_event("shutdown")
        async def shutdown_event():
            self._redis_running = False

        return self.app
