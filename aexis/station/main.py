"""Station process entry point.

Usage:
    python -m aexis.station.main --station-id station_001 --redis-url redis://localhost:6379

Each station instance runs as a standalone process:
  1. Connects to Redis
  2. Subscribes to passenger/cargo/pod event channels
  3. Manages its queue state in Redis hashes
  4. Accepts dev commands on aexis:cmd:station:{id}
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import uuid

import redis.asyncio as redis

from aexis.core.logging_config import setup_logging
from aexis.core.message_bus import MessageBus
from aexis.core.network import NetworkContext, load_network_data
from aexis.station import Station, PassengerGenerator, CargoGenerator

logger = logging.getLogger("aexis.station")


async def _handle_commands(
    redis_client: redis.Redis,
    station: Station,
    message_bus: MessageBus,
    passenger_gen: PassengerGenerator | None,
    cargo_gen: CargoGenerator | None,
):
    """Listen for dev/ops commands on a dedicated Redis channel.

    Supported commands:
      {"cmd": "status"}
      {"cmd": "inject_passenger", "destination": "station_003", ...}
      {"cmd": "inject_cargo", "destination": "station_005", "weight": 100}

    Responses are published to the channel specified in the "reply_to" field.
    """
    channel = f"aexis:cmd:station:{station.station_id}"
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
            response: dict = {"station_id": station.station_id}

            if cmd == "status":
                response["state"] = station.get_state()

            elif cmd == "inject_passenger":
                if not passenger_gen:
                    response["error"] = "passenger generator not available"
                else:
                    dest = payload.get("destination", "")
                    if not dest:
                        response["error"] = "destination required"
                    else:
                        pid = f"manual_p_{uuid.uuid4().hex[:8]}"
                        event = passenger_gen.create_manual_event(
                            pid, station.station_id, dest
                        )
                        await message_bus.publish_event(
                            MessageBus.get_event_channel(event.event_type), event
                        )
                        response["injected"] = {
                            "passenger_id": pid,
                            "destination": dest,
                        }

            elif cmd == "inject_cargo":
                if not cargo_gen:
                    response["error"] = "cargo generator not available"
                else:
                    dest = payload.get("destination", "")
                    weight = float(payload.get("weight", 100.0))
                    if not dest:
                        response["error"] = "destination required"
                    else:
                        rid = f"manual_c_{uuid.uuid4().hex[:8]}"
                        event = cargo_gen.create_manual_event(
                            rid, station.station_id, dest, weight
                        )
                        await message_bus.publish_event(
                            MessageBus.get_event_channel(event.event_type), event
                        )
                        response["injected"] = {
                            "request_id": rid,
                            "destination": dest,
                            "weight": weight,
                        }
            else:
                response["error"] = f"unknown command: {cmd}"

            if reply_to:
                await redis_client.publish(reply_to, json.dumps(response))

    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


async def run_station(args: argparse.Namespace):
    """Main async entry point for a station process."""
    redis_url = args.redis_url
    redis_password = args.redis_password or os.getenv("REDIS_PASSWORD")
    station_id = args.station_id

    # Setup structured logging
    setup_logging("station", station_id)

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

    # Connect message bus
    message_bus = MessageBus(redis_url=redis_url, password=redis_password)
    if not await message_bus.connect():
        logger.error("Failed to connect MessageBus")
        return

    # Load network topology
    network_path = args.network_path or os.getenv(
        "AEXIS_NETWORK_DATA", "aexis/network.json"
    )
    network_data = load_network_data(network_path)
    if network_data:
        network_ctx = NetworkContext(network_data)
        NetworkContext.set_instance(network_ctx)
    else:
        logger.warning("No network data loaded — station running without topology")

    # Determine connected stations from network graph
    connected: list[str] = []
    if network_data:
        nc = NetworkContext.get_instance()
        if nc.network_graph.has_node(station_id):
            connected = [
                n
                for n in nc.network_graph.neighbors(station_id)
                if n.startswith("station_")
            ]

    # Create station
    station = Station(message_bus, redis_client, station_id)
    station.connected_stations = connected
    await station.start()

    # Resolve all station IDs for generators
    all_station_ids: list[str] = []
    if network_data:
        nc = NetworkContext.get_instance()
        all_station_ids = sorted(nc.station_positions.keys())

    # Generators (only if this is the "first" station alphabetically, or
    # controlled by --generators flag to avoid duplicates)
    passenger_gen = None
    cargo_gen = None
    gen_tasks = []
    if args.generators:
        passenger_gen = PassengerGenerator(message_bus, all_station_ids)
        cargo_gen = CargoGenerator(message_bus, all_station_ids)
        gen_tasks.append(asyncio.create_task(passenger_gen.start()))
        gen_tasks.append(asyncio.create_task(cargo_gen.start()))
        logger.info("Payload generators enabled on this station process")

    # Command listener
    cmd_task = asyncio.create_task(
        _handle_commands(
            redis_client, station, message_bus, passenger_gen, cargo_gen
        )
    )

    # Bus listener
    bus_task = asyncio.create_task(message_bus.start_listening())

    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info(
        f"Station {station_id} running (connected to {len(connected)} neighbors)"
    )

    # Wait for shutdown
    await shutdown_event.wait()

    # Cleanup
    cmd_task.cancel()
    bus_task.cancel()
    for t in gen_tasks:
        t.cancel()

    if passenger_gen:
        await passenger_gen.stop()
    if cargo_gen:
        await cargo_gen.stop()

    await station.stop()
    await message_bus.stop_listening()
    await message_bus.disconnect()
    await redis_client.aclose()
    logger.info(f"Station {station_id} shut down")


def main():
    parser = argparse.ArgumentParser(description="AEXIS Station Process")
    parser.add_argument(
        "--station-id",
        required=True,
        help="Station identifier (e.g. station_001)",
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
        help="Path to network.json (overrides AEXIS_NETWORK_DATA env var)",
    )
    parser.add_argument(
        "--generators",
        action="store_true",
        help="Enable passenger/cargo generators on this station process",
    )
    args = parser.parse_args()
    asyncio.run(run_station(args))


if __name__ == "__main__":
    main()
