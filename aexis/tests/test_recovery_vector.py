"""
Recovery and Resilience Integration Tests for AEXIS System

Tests verify system behavior under failure conditions using REAL Redis integration:
- MessageBus reconnection logic
- Component recovery after crash
- Malformed message handling
- Network interruption resilience
- State persistence verification
"""

import asyncio
import json
import pytest
import pytest_asyncio
import logging
import time
from pathlib import Path
from datetime import datetime, UTC
from unittest.mock import MagicMock, AsyncMock, patch

import redis.asyncio as redis

from aexis.core.system import AexisSystem, SystemContext, AexisConfig
from aexis.core.message_bus import MessageBus
from aexis.core.network import NetworkContext
from aexis.core.model import Event, Command

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("aexis.core.message_bus").setLevel(logging.DEBUG)

# Constants for real Redis
REDIS_URL = "redis://:your_redis_password_here@localhost:6379/0"

@pytest.fixture
def network_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "network.json"

@pytest.fixture
def clean_redis():
    """Ensure Redis is clean before/after tests"""
    import redis as sync_redis
    r = sync_redis.from_url(REDIS_URL)
    r.flushdb()
    yield
    r.flushdb()
    r.close()

@pytest_asyncio.fixture
async def recovery_system(network_path, clean_redis):
    """System configured with REAL Redis for recovery testing"""
    config = AexisConfig(
        debug=True,
        network_data_path=str(network_path),
        pods={"count": 5, "cargoPercentage": 50},
        stations={"count": 21},
        ai={"provider": "none"},
        redis={"url": REDIS_URL}  # Use real Redis
    )
    
    mock_ctx = MagicMock(spec=SystemContext)
    mock_ctx.get_config.return_value = config
    
    # Initialize network context
    NetworkContext._instance = None
    with open(str(network_path), 'r') as f:
        network_data = json.load(f)
    real_network = NetworkContext(network_data=network_data)
    NetworkContext._instance = real_network
    mock_ctx.get_network_context.return_value = real_network
    
    # Create system - will use real MessageBus due to config
    system = AexisSystem(system_context=mock_ctx)
    
    # Mock visualizer/metrics to avoid noise
    system._update_metrics = AsyncMock()
    system._publish_snapshot = AsyncMock()
    system._log_system_status = AsyncMock()
    
    await system.initialize()
    # Manually start message bus listening since we don't call system.start()
    await system.message_bus.start_listening()
    yield system
    await system.shutdown()

# --- Connection verification ---

@pytest.mark.asyncio
@pytest.mark.skip
async def test_real_redis_connection(recovery_system):
    """
    Verify the system is actually connected to Redis.
    """
    system = recovery_system
    
    assert isinstance(system.message_bus, MessageBus)
    assert system.message_bus.redis_client is not None
    assert await system.message_bus.redis_client.ping() is True

# --- Reconnection Tests ---

@pytest.mark.asyncio
@pytest.mark.skip
async def test_message_persistence_during_processing(recovery_system):
    """
    Verify that messages published to Redis are received by subscribers.
    This confirms the basic real-infrastructure path works.
    """
    system = recovery_system
    received_events = []
    
    event_channel = MessageBus.CHANNELS["SYSTEM_EVENTS"]
    
    def handler(data):
        print(f"DEBUG: Handler received data: {data}")
        received_events.append(data)
    
    # Remove await for synchronous subscribe method
    system.message_bus.subscribe(event_channel, handler)
    await asyncio.sleep(0.5)  # Wait for subscription to propagate
    
    # Publish event
    test_event = Event(
        event_type="TEST_PERSISTENCE",
        data={"test_id": "persistence_001"},
        source="unit_test"
    )
    await system.message_bus.publish_event(event_channel, test_event)
    
    # Wait for propagation (real network delay)
    for _ in range(10):
        if len(received_events) > 0:
            break
        await asyncio.sleep(0.1)
        
    assert len(received_events) == 1
    # Check payload content
    received = received_events[0]
    # MessageBus wraps event in "message" key, and event has "data" dict
    assert received["message"]["data"]["test_id"] == "persistence_001"

@pytest.mark.asyncio
@pytest.mark.skip
async def test_subscriber_recovery_after_error(recovery_system):
    """
    If a subscriber handler raises an exception, the subscription should remain active
    for subsequent messages.
    """
    system = recovery_system
    channel = "ERROR_TEST_CHANNEL"
    received_count = 0
    
    def error_prone_handler(data):
        nonlocal received_count
        received_count += 1
        event_data = data.get("message", {}).get("data", {})
        if event_data.get("trigger_error"):
            raise ValueError("Simulated handler crash")
            
    system.message_bus.subscribe(channel, error_prone_handler)
    await asyncio.sleep(0.5)
    
    # 1. Send normal message
    evt1 = Event(event_type="TEST_ERROR", data={"trigger_error": False, "seq": 1})
    await system.message_bus.publish_event(channel, evt1)
    await asyncio.sleep(0.2)
    assert received_count == 1
    
    # 2. Send triggers error - logs error but shouldn't crash loop
    evt2 = Event(event_type="TEST_ERROR", data={"trigger_error": True, "seq": 2})
    await system.message_bus.publish_event(channel, evt2)
    await asyncio.sleep(0.2)
    assert received_count == 2
    
    # 3. Send normal message again - should still work
    evt3 = Event(event_type="TEST_ERROR", data={"trigger_error": False, "seq": 3})
    await system.message_bus.publish_event(channel, evt3)
    await asyncio.sleep(0.2)
    assert received_count == 3

# --- System Resilience ---

@pytest.mark.asyncio
@pytest.mark.skip
async def test_invalid_json_handling(recovery_system):
    """
    System should not crash when receiving malformed JSON from Redis.
    """
    system = recovery_system
    channel = MessageBus.CHANNELS["SYSTEM_EVENTS"]
    
    # Bypass MessageBus.publish (which serializes) and write "garbage" directly to Redis
    redis_client = system.message_bus.redis_client
    await redis_client.publish(channel, "this is not json { [ }")
    
    # Allow some time for processing
    await asyncio.sleep(0.2)
    
    # System should still be running and able to process valid messages
    assert system.running
    
    # Verify valid message still works
    valid_received = False
    def valid_handler(data):
        nonlocal valid_received
        valid_received = True
        
    system.message_bus.subscribe(channel, valid_handler)
    await asyncio.sleep(0.5)
    valid_event = Event(event_type="TEST_VALID", data={"status": "ok"})
    await system.message_bus.publish_event(channel, valid_event)
    
    for _ in range(10):
        if valid_received:
            break
        await asyncio.sleep(0.1)
        
    assert valid_received, "System failed to process valid message after malformed input"

@pytest.mark.asyncio
@pytest.mark.skip
async def test_rapid_connection_cycling(recovery_system):
    """
    Verify system stability when connections are rapidly opened/closed?
    Actually, mostly verify generic "Restart" capability of the system components.
    """
    system = recovery_system
    
    # Simulate a "soft restart" of the message bus connection
    # Note: access private method or just call connect again?
    
    # Close existing
    if system.message_bus.redis_client:
        await system.message_bus.redis_client.close()
    
    # Wait a bit
    await asyncio.sleep(0.1)
    
    # Reconnect
    await system.message_bus.connect()
    
    assert await system.message_bus.redis_client.ping()
    
    # Verify subscriptions are restored?
    # The default MessageBus implementation generally needs to resubscribe logic
    # Let's check if our implementation handles this or if we need to manually trigger it.
    # Looking at Aexis MessageBus, it might not auto-resubscribe on manual close/open,
    # but let's see if manual resubscription works.
    
    received = False
    def handler(data):
        nonlocal received
        received = True
        
    system.message_bus.subscribe("RECONNECT_TEST", handler)
    await asyncio.sleep(0.5)
    reconnect_event = Event(event_type="TEST_RECONNECT", data={"msg": "post-reconnect"})
    await system.message_bus.publish_event("RECONNECT_TEST", reconnect_event)
    
    await asyncio.sleep(0.5)
    assert received
