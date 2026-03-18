
import json
import logging
from typing import Any

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

_CLAIM_LUA = """
local queue_key = KEYS[1]
local claims_key = KEYS[2]
local item_id = ARGV[1]
local pod_id = ARGV[2]

-- Item must exist in the queue
if redis.call('HEXISTS', queue_key, item_id) == 0 then
    return 0
end

-- HSETNX returns 1 if the field was set (not previously claimed)
return redis.call('HSETNX', claims_key, item_id, pod_id)
"""

def _passengers_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:passengers"

def _cargo_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:cargo"

def _passenger_claims_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:claims:passengers"

def _cargo_claims_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:claims:cargo"

def _state_key(station_id: str) -> str:
    return f"aexis:station:{station_id}:state"

class StationClient:

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("StationClient requires a connected Redis client")
        self._redis = redis_client
        self._claim_script = self._redis.register_script(_CLAIM_LUA)

    async def claim_passenger(
        self, station_id: str, passenger_id: str, pod_id: str
    ) -> bool:
        if not station_id or not passenger_id or not pod_id:
            logger.warning(
                "claim_passenger called with empty args: "
                f"station={station_id}, passenger={passenger_id}, pod={pod_id}"
            )
            return False

        try:
            result = await self._claim_script(
                keys=[_passengers_key(station_id), _passenger_claims_key(station_id)],
                args=[passenger_id, pod_id],
            )
            claimed = bool(result)
            if claimed:
                logger.info(
                    f"StationClient: pod {pod_id} claimed passenger "
                    f"{passenger_id} at {station_id}"
                )
            return claimed
        except Exception as e:
            logger.error(
                f"StationClient: claim_passenger failed for {passenger_id} "
                f"at {station_id}: {e}"
            )
            return False

    async def claim_cargo(
        self, station_id: str, request_id: str, pod_id: str
    ) -> bool:
        if not station_id or not request_id or not pod_id:
            logger.warning(
                "claim_cargo called with empty args: "
                f"station={station_id}, request={request_id}, pod={pod_id}"
            )
            return False

        try:
            result = await self._claim_script(
                keys=[_cargo_key(station_id), _cargo_claims_key(station_id)],
                args=[request_id, pod_id],
            )
            claimed = bool(result)
            if claimed:
                logger.info(
                    f"StationClient: pod {pod_id} claimed cargo "
                    f"{request_id} at {station_id}"
                )
            return claimed
        except Exception as e:
            logger.error(
                f"StationClient: claim_cargo failed for {request_id} "
                f"at {station_id}: {e}"
            )
            return False

    async def get_pending_passengers(
        self, station_id: str, destination: str | None = None
    ) -> list[dict[str, Any]]:
        try:
            all_passengers = await self._redis.hgetall(
                _passengers_key(station_id)
            )
            claimed = await self._redis.hgetall(
                _passenger_claims_key(station_id)
            )

            pending = []
            for pid, raw in all_passengers.items():
                if pid in claimed:
                    continue
                try:
                    passenger = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        f"StationClient: corrupt passenger data for {pid} "
                        f"at {station_id}"
                    )
                    continue
                if destination and passenger.get("destination") != destination:
                    continue
                pending.append(passenger)
            return pending
        except Exception as e:
            logger.error(
                f"StationClient: get_pending_passengers failed "
                f"for {station_id}: {e}"
            )
            return []

    async def get_pending_cargo(
        self, station_id: str, destination: str | None = None
    ) -> list[dict[str, Any]]:
        try:
            all_cargo = await self._redis.hgetall(_cargo_key(station_id))
            claimed = await self._redis.hgetall(_cargo_claims_key(station_id))

            pending = []
            for rid, raw in all_cargo.items():
                if rid in claimed:
                    continue
                try:
                    cargo = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        f"StationClient: corrupt cargo data for {rid} "
                        f"at {station_id}"
                    )
                    continue
                if destination and cargo.get("destination") != destination:
                    continue
                pending.append(cargo)
            return pending
        except Exception as e:
            logger.error(
                f"StationClient: get_pending_cargo failed "
                f"for {station_id}: {e}"
            )
            return []

    async def get_claimed_passengers(
        self, station_id: str, pod_id: str
    ) -> list[dict[str, Any]]:
        try:
            all_passengers = await self._redis.hgetall(
                _passengers_key(station_id)
            )
            claims = await self._redis.hgetall(
                _passenger_claims_key(station_id)
            )

            result = []
            for pid, claimant in claims.items():
                if claimant != pod_id:
                    continue
                raw = all_passengers.get(pid)
                if raw is None:
                    continue
                try:
                    result.append(json.loads(raw))
                except (json.JSONDecodeError, TypeError):
                    continue
            return result
        except Exception as e:
            logger.error(
                f"StationClient: get_claimed_passengers failed "
                f"for {station_id}, pod {pod_id}: {e}"
            )
            return []

    async def get_claimed_cargo(
        self, station_id: str, pod_id: str
    ) -> list[dict[str, Any]]:
        try:
            all_cargo = await self._redis.hgetall(_cargo_key(station_id))
            claims = await self._redis.hgetall(_cargo_claims_key(station_id))

            result = []
            for rid, claimant in claims.items():
                if claimant != pod_id:
                    continue
                raw = all_cargo.get(rid)
                if raw is None:
                    continue
                try:
                    result.append(json.loads(raw))
                except (json.JSONDecodeError, TypeError):
                    continue
            return result
        except Exception as e:
            logger.error(
                f"StationClient: get_claimed_cargo failed "
                f"for {station_id}, pod {pod_id}: {e}"
            )
            return []

    async def get_station_state(self, station_id: str) -> dict[str, Any] | None:
        try:
            raw = await self._redis.get(_state_key(station_id))
            if raw is None:
                return None
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(
                f"StationClient: corrupt state data for {station_id}: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"StationClient: get_station_state failed for {station_id}: {e}"
            )
            return None

    async def get_all_station_ids(self) -> list[str]:
        try:
            station_ids = []
            async for key in self._redis.scan_iter(
                match="aexis:station:*:state", count=100
            ):

                parts = key.split(":")
                if len(parts) == 4:
                    station_ids.append(parts[2])
            return sorted(station_ids)
        except Exception as e:
            logger.error(f"StationClient: get_all_station_ids failed: {e}")
            return []
