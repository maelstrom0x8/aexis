"""Unit tests for StationClient claim/query operations.

Tests atomicity of claims under concurrent access, pending/claimed queries,
and edge cases like double-claims and claims on nonexistent items.
"""

import asyncio
import json

import pytest

from aexis.tests.conftest import async_seed_cargo, async_seed_passenger


class TestClaimPassenger:
    """Atomic claim semantics for passenger queue items."""

    async def test_claim_succeeds_when_passenger_exists(
        self, station_client, redis_client
    ):
        await async_seed_passenger(redis_client, "1", "p_001", "2")
        result = await station_client.claim_passenger("1", "p_001", "1")
        assert result is True

    async def test_double_claim_fails(self, station_client, redis_client):
        await async_seed_passenger(redis_client, "1", "p_001", "2")
        first = await station_client.claim_passenger("1", "p_001", "1")
        second = await station_client.claim_passenger("1", "p_001", "pod_002")
        assert first is True
        assert second is False

    async def test_claim_nonexistent_passenger_fails(self, station_client):
        result = await station_client.claim_passenger(
            "1", "p_nonexistent", "1"
        )
        assert result is False

    async def test_claim_with_empty_args_fails(self, station_client):
        assert await station_client.claim_passenger("", "p_001", "1") is False
        assert await station_client.claim_passenger("1", "", "1") is False
        assert await station_client.claim_passenger("1", "p_001", "") is False

    async def test_concurrent_claims_single_winner(
        self, station_client, redis_client
    ):
        """Multiple pods racing to claim the same passenger — only one wins."""
        await async_seed_passenger(redis_client, "1", "p_race", "2")

        results = await asyncio.gather(
            station_client.claim_passenger("1", "p_race", "1"),
            station_client.claim_passenger("1", "p_race", "pod_002"),
            station_client.claim_passenger("1", "p_race", "pod_003"),
            station_client.claim_passenger("1", "p_race", "pod_004"),
            station_client.claim_passenger("1", "p_race", "pod_005"),
        )
        assert sum(results) == 1, f"Expected exactly 1 winner, got {sum(results)}"


class TestClaimCargo:
    """Atomic claim semantics for cargo queue items."""

    async def test_claim_succeeds(self, station_client, redis_client):
        await async_seed_cargo(
            redis_client, "1", "c_001", "3", 50.0
        )
        result = await station_client.claim_cargo("1", "c_001", "1")
        assert result is True

    async def test_double_claim_fails(self, station_client, redis_client):
        await async_seed_cargo(
            redis_client, "1", "c_001", "3", 50.0
        )
        first = await station_client.claim_cargo("1", "c_001", "1")
        second = await station_client.claim_cargo("1", "c_001", "pod_002")
        assert first is True
        assert second is False

    async def test_claim_nonexistent_cargo_fails(self, station_client):
        result = await station_client.claim_cargo(
            "1", "c_nonexistent", "1"
        )
        assert result is False


class TestPendingQueries:
    """Querying unclaimed items from station queues."""

    async def test_pending_passengers_excludes_claimed(
        self, station_client, redis_client
    ):
        await async_seed_passenger(redis_client, "1", "p_001", "2")
        await async_seed_passenger(redis_client, "1", "p_002", "3")
        await async_seed_passenger(redis_client, "1", "p_003", "2")

        # Claim one
        await station_client.claim_passenger("1", "p_002", "1")

        pending = await station_client.get_pending_passengers("1")
        pending_ids = {p["passenger_id"] for p in pending}
        assert pending_ids == {"p_001", "p_003"}

    async def test_pending_cargo_excludes_claimed(
        self, station_client, redis_client
    ):
        await async_seed_cargo(
            redis_client, "1", "c_001", "3", 25.0
        )
        await async_seed_cargo(
            redis_client, "1", "c_002", "4", 50.0
        )

        await station_client.claim_cargo("1", "c_001", "1")

        pending = await station_client.get_pending_cargo("1")
        assert len(pending) == 1
        assert pending[0]["request_id"] == "c_002"

    async def test_pending_passengers_empty_station(self, station_client):
        pending = await station_client.get_pending_passengers("station_empty")
        assert pending == []

    async def test_pending_passengers_destination_filter(
        self, station_client, redis_client
    ):
        await async_seed_passenger(redis_client, "1", "p_001", "2")
        await async_seed_passenger(redis_client, "1", "p_002", "3")

        filtered = await station_client.get_pending_passengers(
            "1", destination="2"
        )
        assert len(filtered) == 1
        assert filtered[0]["passenger_id"] == "p_001"


class TestClaimedQueries:
    """Querying items claimed by a specific pod."""

    async def test_get_claimed_passengers(self, station_client, redis_client):
        await async_seed_passenger(redis_client, "1", "p_001", "2")
        await async_seed_passenger(redis_client, "1", "p_002", "3")

        await station_client.claim_passenger("1", "p_001", "1")
        await station_client.claim_passenger("1", "p_002", "pod_002")

        claimed = await station_client.get_claimed_passengers("1", "1")
        assert len(claimed) == 1
        assert claimed[0]["passenger_id"] == "p_001"

    async def test_get_claimed_cargo_by_pod(self, station_client, redis_client):
        await async_seed_cargo(
            redis_client, "1", "c_001", "3", 25.0
        )
        await async_seed_cargo(
            redis_client, "1", "c_002", "3", 50.0
        )
        await async_seed_cargo(
            redis_client, "1", "c_003", "4", 75.0
        )

        await station_client.claim_cargo("1", "c_001", "1")
        await station_client.claim_cargo("1", "c_003", "1")
        await station_client.claim_cargo("1", "c_002", "pod_002")

        claimed = await station_client.get_claimed_cargo("1", "1")
        claim_ids = {c["request_id"] for c in claimed}
        assert claim_ids == {"c_001", "c_003"}

    async def test_no_claimed_items_returns_empty(self, station_client):
        claimed = await station_client.get_claimed_passengers(
            "1", "pod_nonexistent"
        )
        assert claimed == []


class TestStationState:
    """Station state snapshot reads."""

    async def test_get_state_when_published(self, station_client, redis_client):
        state = {"station_id": "1", "status": "operational"}
        await redis_client.set(
            "aexis:station:1:state", json.dumps(state)
        )
        result = await station_client.get_station_state("1")
        assert result == state

    async def test_get_state_returns_none_when_missing(self, station_client):
        result = await station_client.get_station_state("station_missing")
        assert result is None

    async def test_get_all_station_ids(self, station_client, redis_client):
        for i in range(1, 4):
            sid = str(i)
            await redis_client.set(
                f"aexis:station:{sid}:state",
                json.dumps({"station_id": sid}),
            )
        ids = await station_client.get_all_station_ids()
        assert ids == ["1", "2", "3"]
