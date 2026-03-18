"""Pod process entry point.

Usage:
    python -m aexis.pod.main --pod-id pod_001 --type passenger --redis-url redis://localhost:6379

Each pod instance runs as a standalone process:
  1. Connects to Redis
  2. Creates a StationClient for remote station queue access
  3. Spawns at a random network edge (or specified station)
  4. Runs its own movement loop + subscribes to events
  5. Accepts dev commands on aexis:cmd:pod:{id}
"""

import argparse
import asyncio
import json
import logging
import os
import signal

import redis.asyncio as redis

from aexis.core.logging_config import setup_logging
from aexis.core.message_bus import MessageBus
from aexis.core.network import NetworkContext, load_network_data
from aexis.core.ai_provider import AIProviderFactory
from aexis.core.routing import AIRouter, OfflineRouter, RoutingProvider
from aexis.core.station_client import StationClient
from aexis.pod import CargoPod, PassengerPod

logger = logging.getLogger("aexis.pod")


async def _handle_commands(
    redis_client: redis.Redis,
    pod,
):
    """Listen for dev/ops commands on a dedicated Redis channel.

    Supported commands:
      {"cmd": "status"}
      {"cmd": "assign_route", "stations": ["station_001", "station_003"]}
      {"cmd": "set_speed", "speed": 30.0}

    Responses published to the channel specified in "reply_to".
    """
    channel = f"aexis:cmd:pod:{pod.pod_id}"
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    logger.info(f"Listening for commands on {channel}")

    try:
        while True:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if not msg or msg["type"] != "message":
                await asyncio.sleep(0.05)
                continue

            try:
                payload = json.loads(msg["data"])
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Malformed command on {channel}")
                continue

            cmd = payload.get("cmd")
            reply_to = payload.get("reply_to")
            response: dict = {"pod_id": pod.pod_id}

            if cmd == "status":
                response["state"] = pod.get_state()

            elif cmd == "assign_route":
                stations = payload.get("stations", [])
                if not stations or len(stations) < 2:
                    response["error"] = "stations must be a list of ≥2 station IDs"
                else:
                    from aexis.core.model import Route
                    from datetime import datetime

                    route = Route(
                        route_id=f"manual_{datetime.now().timestamp()}",
                        stations=stations,
                        estimated_duration=len(stations) * 5,
                    )
                    pod.current_route = route
                    success = await pod._hydrate_route(stations)
                    if success:
                        from aexis.core.model import PodStatus

                        pod.status = PodStatus.EN_ROUTE
                        response["route_assigned"] = stations
                    else:
                        response["error"] = "route hydration failed"

            elif cmd == "set_speed":
                new_speed = float(payload.get("speed", pod.speed))
                if new_speed <= 0 or new_speed > 200:
                    response["error"] = "speed must be between 0 and 200 m/s"
                else:
                    pod.speed = new_speed
                    response["speed"] = new_speed

            else:
                response["error"] = f"unknown command: {cmd}"

            if reply_to:
                await redis_client.publish(reply_to, json.dumps(response))

    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


async def run_pod(args: argparse.Namespace):
    """Main async entry point for a pod process."""
    redis_url = args.redis_url
    redis_password = args.redis_password or os.getenv("REDIS_PASSWORD")
    pod_id = args.pod_id
    pod_type = args.type

    # Setup structured logging
    setup_logging("pod", pod_id)

    # Connect to Redis
    redis_client = redis.from_url(
        redis_url,
        password=redis_password,
        decode_responses=True,
        socket_connect_timeout=10,
        retry_on_timeout=True,
    )
    try:
        await redis_client.ping()
    except Exception as e:
        logger.error(f"Cannot connect to Redis at {redis_url}: {e}")
        return

    # Message bus
    message_bus = MessageBus(redis_url=redis_url, password=redis_password)
    if not await message_bus.connect():
        logger.error("Failed to connect MessageBus")
        return

    # Network topology
    network_path = args.network_path or os.getenv(
        "AEXIS_NETWORK_DATA", "aexis/network.json"
    )
    network_data = load_network_data(network_path)
    if network_data:
        nc = NetworkContext(network_data)
        NetworkContext.set_instance(nc)
    else:
        logger.error("No network data — pod cannot operate without topology")
        return

    # Station client for remote queue access
    station_client = StationClient(redis_client)

    # Routing provider — AI primary if credentials are set, offline fallback always
    routing_provider = RoutingProvider()

    agent_endpoint = os.getenv("GRADIENT_AGENT_ENDPOINT", "")
    agent_access_key = os.getenv("GRADIENT_AGENT_ACCESS_KEY", "")

    if agent_endpoint and agent_access_key:
        try:
            ai_provider = AIProviderFactory.create_provider(
                "gradient",
                agent_endpoint=agent_endpoint,
                agent_access_key=agent_access_key,
            )
            routing_provider.add_router(AIRouter(pod_id, ai_provider=ai_provider))
            logger.info("AI routing enabled via Gradient Agent for pod %s", pod_id)
        except Exception as exc:
            logger.warning(
                "Failed to initialise Gradient AI provider for pod %s: %s. "
                "Falling back to offline routing.",
                pod_id, exc,
            )

    routing_provider.add_router(OfflineRouter())

    # Create pod
    if pod_type == "cargo":
        pod = CargoPod(
            message_bus, redis_client, pod_id, station_client, routing_provider
        )
    else:
        pod = PassengerPod(
            message_bus, redis_client, pod_id, station_client, routing_provider
        )

    # Spawn position
    nc = NetworkContext.get_instance()
    if args.station:
        # Spawn at specified station
        from aexis.core.model import LocationDescriptor, Coordinate

        pos = nc.station_positions.get(args.station, (0, 0))
        pod.location_descriptor = LocationDescriptor(
            location_type="station",
            node_id=args.station,
            coordinate=Coordinate(pos[0], pos[1]),
        )
    else:
        # Default: spawn at random station (docked)
        from aexis.core.model import LocationDescriptor, Coordinate

        station_id = nc.get_random_station()
        pos = nc.station_positions.get(station_id, (0, 0))
        pod.location_descriptor = LocationDescriptor(
            location_type="station",
            node_id=station_id,
            coordinate=Coordinate(pos[0], pos[1]),
        )

    if args.speed:
        pod.speed = args.speed

    await pod.start()

    # Launch tasks
    movement_task = asyncio.create_task(pod.run_movement_loop())
    cmd_task = asyncio.create_task(_handle_commands(redis_client, pod))
    bus_task = asyncio.create_task(message_bus.start_listening())

    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info(
        f"Pod {pod_id} ({pod_type}) running at {pod.location} "
        f"(speed: {pod.speed} m/s)"
    )

    await shutdown_event.wait()

    # Cleanup
    movement_task.cancel()
    cmd_task.cancel()
    bus_task.cancel()
    await pod.stop()
    await message_bus.stop_listening()
    await message_bus.disconnect()
    await redis_client.aclose()
    logger.info(f"Pod {pod_id} shut down")


def main():
    parser = argparse.ArgumentParser(description="AEXIS Pod Process")
    parser.add_argument(
        "--pod-id", required=True, help="Pod identifier (e.g. pod_001)"
    )
    parser.add_argument(
        "--type",
        choices=["passenger", "cargo"],
        default="passenger",
        help="Pod type",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379"),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--redis-password",
        default=None,
        help="Redis password (overrides REDIS_PASSWORD env var)",
    )
    parser.add_argument(
        "--network-path",
        default=None,
        help="Path to network.json",
    )
    parser.add_argument(
        "--station",
        default=None,
        help="Spawn at this station instead of random edge",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=None,
        help="Override default pod speed (m/s)",
    )
    args = parser.parse_args()
    asyncio.run(run_pod(args))


if __name__ == "__main__":
    main()
