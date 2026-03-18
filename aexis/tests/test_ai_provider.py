"""Unit tests for the Gradient AI provider layer.

All tests use mocked HTTP — no real Gradient Agent calls.
"""

import json
import os

import httpx
import pytest

from aexis.core.ai_provider import (
    AIProviderFactory,
    GradientAIProvider,
    MockAIProvider,
)
from aexis.core.errors import GradientException
from aexis.core.model import DecisionContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_context(**overrides) -> DecisionContext:
    """Build a minimal DecisionContext for testing."""
    defaults = dict(
        pod_id="pod_test_01",
        current_location="1",
        current_route=None,
        capacity_available=10,
        weight_available=0.0,
        available_requests=[
            {
                "type": "passenger",
                "origin": "2",
                "destination": "3",
                "passenger_id": "p_001",
            },
        ],
        network_state={"avg_congestion": 0.2},
        system_metrics={"total_pods": 4},
        pod_type="passenger",
        pod_constraints={},
        specialization="passenger_transport",
        passengers=[],
        cargo=[],
    )
    defaults.update(overrides)
    return DecisionContext(**defaults)


def _agent_response_body(
    content: str,
    include_retrieval: bool = False,
    include_guardrails: bool = False,
) -> dict:
    """Build a fake agent response body."""
    body = {
        "id": "chatcmpl-test",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 150,
            "completion_tokens": 80,
            "total_tokens": 230,
        },
    }
    if include_retrieval:
        body["retrieval"] = {
            "sources": [
                {"document": "aexis_network_topology.json", "score": 0.92},
            ]
        }
    if include_guardrails:
        body["guardrails"] = {
            "actions": [
                {"type": "content_moderation", "result": "passed"},
            ]
        }
    return body


_VALID_DECISION_JSON = json.dumps({
    "accepted_requests": ["p_001"],
    "rejected_requests": [],
    "route": ["1", "2", "3"],
    "estimated_duration": 12,
    "confidence": 0.85,
    "reasoning": "Route to pickup at station 2, deliver at station 3",
})


@pytest.fixture
def provider():
    """Create a GradientAIProvider with a dummy endpoint."""
    return GradientAIProvider(
        agent_endpoint="https://agent.example.com",
        agent_access_key="test-key-123",
    )


# ---------------------------------------------------------------------------
# GradientAIProvider — prompt construction
# ---------------------------------------------------------------------------

class TestPromptConstruction:
    """The prompt sent to the agent must contain key context fields."""

    def test_prompt_contains_pod_id(self, provider):
        ctx = _make_context(pod_id="pod_alpha")
        prompt = provider._build_prompt(ctx)
        assert "pod_alpha" in prompt

    def test_prompt_contains_location(self, provider):
        ctx = _make_context(current_location="station_7")
        prompt = provider._build_prompt(ctx)
        assert "station_7" in prompt

    def test_prompt_contains_available_requests(self, provider):
        ctx = _make_context()
        prompt = provider._build_prompt(ctx)
        assert "p_001" in prompt

    def test_prompt_contains_passengers_onboard(self, provider):
        ctx = _make_context(
            passengers=[{"passenger_id": "p_onboard", "destination": "5"}]
        )
        prompt = provider._build_prompt(ctx)
        assert "p_onboard" in prompt


# ---------------------------------------------------------------------------
# GradientAIProvider — response parsing
# ---------------------------------------------------------------------------

class TestResponseParsing:
    """Agent response parsing handles clean JSON, markdown fences, and noise."""

    def test_clean_json_response(self, provider):
        ctx = _make_context()
        body = _agent_response_body(_VALID_DECISION_JSON)
        decision = provider._parse_response(body, ctx)

        assert decision.decision_type == "route_selection"
        assert decision.route == ["1", "2", "3"]
        assert decision.confidence == 0.85
        assert decision.accepted_requests == ["p_001"]
        assert decision.fallback_used is False

    def test_markdown_fenced_json(self, provider):
        ctx = _make_context()
        fenced = f"```json\n{_VALID_DECISION_JSON}\n```"
        body = _agent_response_body(fenced)
        decision = provider._parse_response(body, ctx)

        assert decision.route == ["1", "2", "3"]

    def test_json_with_surrounding_text(self, provider):
        ctx = _make_context()
        noisy = f"Here is my analysis:\n{_VALID_DECISION_JSON}\nDone."
        body = _agent_response_body(noisy)
        decision = provider._parse_response(body, ctx)

        assert decision.route == ["1", "2", "3"]

    def test_empty_choices_raises(self, provider):
        ctx = _make_context()
        body = {"choices": []}
        with pytest.raises(GradientException):
            provider._parse_response(body, ctx)

    def test_empty_content_raises(self, provider):
        ctx = _make_context()
        body = _agent_response_body("")
        with pytest.raises(GradientException):
            provider._parse_response(body, ctx)

    def test_invalid_json_raises(self, provider):
        ctx = _make_context()
        body = _agent_response_body("this is not json at all {broken")
        with pytest.raises(GradientException):
            provider._parse_response(body, ctx)

    def test_missing_required_field_raises(self, provider):
        ctx = _make_context()
        incomplete = json.dumps({
            "accepted_requests": [],
            # missing "rejected_requests", "route", etc.
        })
        body = _agent_response_body(incomplete)
        with pytest.raises(GradientException):
            provider._parse_response(body, ctx)

    def test_empty_route_falls_back_to_current_location(self, provider):
        ctx = _make_context(current_location="station_5")
        decision_json = json.dumps({
            "accepted_requests": [],
            "rejected_requests": [],
            "route": [],
            "estimated_duration": 0,
            "confidence": 0.5,
            "reasoning": "No routes available",
        })
        body = _agent_response_body(decision_json)
        decision = provider._parse_response(body, ctx)
        # Empty route should be filled with current location
        assert decision.route == ["station_5"]

    def test_confidence_clamped_to_0_1(self, provider):
        ctx = _make_context()
        decision_json = json.dumps({
            "accepted_requests": [],
            "rejected_requests": [],
            "route": ["1"],
            "estimated_duration": 5,
            "confidence": 1.5,
            "reasoning": "Overconfident",
        })
        body = _agent_response_body(decision_json)
        decision = provider._parse_response(body, ctx)
        assert decision.confidence == 1.0

    def test_retrieval_info_logged(self, provider, caplog):
        ctx = _make_context()
        body = _agent_response_body(
            _VALID_DECISION_JSON, include_retrieval=True
        )
        import logging
        with caplog.at_level(logging.INFO, logger="aexis.core.ai_provider"):
            provider._log_platform_metadata(body, ctx.pod_id)
        assert "RAG retrieved 1 source(s)" in caplog.text


# ---------------------------------------------------------------------------
# GradientAIProvider — retry logic
# ---------------------------------------------------------------------------

class TestRetryLogic:
    """Agent HTTP call retries on transient failures."""

    async def test_retries_on_timeout(self, provider, monkeypatch):
        """Should retry on httpx.TimeoutException and eventually raise."""
        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise httpx.TimeoutException("Connection timed out")

        monkeypatch.setattr(provider._client, "post", mock_post)

        ctx = _make_context()
        with pytest.raises(GradientException):
            await provider.make_decision(ctx)

        assert call_count == 3  # 3 retries

    async def test_retries_on_connect_error(self, provider, monkeypatch):
        """Should retry on httpx.ConnectError."""
        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise httpx.ConnectError("Connection refused")

        monkeypatch.setattr(provider._client, "post", mock_post)

        ctx = _make_context()
        with pytest.raises(GradientException):
            await provider.make_decision(ctx)

        assert call_count == 3

    async def test_succeeds_after_transient_failure(self, provider, monkeypatch):
        """Should succeed if a retry works."""
        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("Transient failure")
            return httpx.Response(
                200,
                json=_agent_response_body(_VALID_DECISION_JSON),
                request=httpx.Request("POST", "https://agent.example.com"),
            )

        monkeypatch.setattr(provider._client, "post", mock_post)

        ctx = _make_context()
        decision = await provider.make_decision(ctx)

        assert call_count == 2
        assert decision.route == ["1", "2", "3"]

    async def test_rate_limit_raises_immediately(self, provider, monkeypatch):
        """429 should raise without retrying."""
        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                429,
                text="Rate limited",
                request=httpx.Request("POST", "https://agent.example.com"),
            )

        monkeypatch.setattr(provider._client, "post", mock_post)

        ctx = _make_context()
        with pytest.raises(GradientException):
            await provider.make_decision(ctx)

        assert call_count == 1


# ---------------------------------------------------------------------------
# MockAIProvider
# ---------------------------------------------------------------------------

class TestMockAIProvider:

    async def test_returns_deterministic_decision(self):
        mock = MockAIProvider(response_delay=0.0)
        ctx = _make_context()
        decision = await mock.make_decision(ctx)

        assert decision.decision_type == "route_selection"
        assert decision.route == [ctx.current_location]
        assert decision.confidence == 0.8

    async def test_increments_call_count(self):
        mock = MockAIProvider(response_delay=0.0)
        ctx = _make_context()
        await mock.make_decision(ctx)
        await mock.make_decision(ctx)
        assert mock.call_count == 2

    def test_is_always_available(self):
        mock = MockAIProvider()
        assert mock.is_available() is True


# ---------------------------------------------------------------------------
# AIProviderFactory
# ---------------------------------------------------------------------------

class TestAIProviderFactory:

    def test_create_mock_provider(self):
        provider = AIProviderFactory.create_provider("mock")
        assert isinstance(provider, MockAIProvider)

    def test_create_gradient_provider_from_env(self, monkeypatch):
        monkeypatch.setenv("GRADIENT_AGENT_ENDPOINT", "https://agent.test.com")
        monkeypatch.setenv("GRADIENT_AGENT_ACCESS_KEY", "test-key")

        provider = AIProviderFactory.create_provider("gradient")
        assert isinstance(provider, GradientAIProvider)

    def test_auto_creates_mock_without_env(self, monkeypatch):
        monkeypatch.delenv("GRADIENT_AGENT_ENDPOINT", raising=False)
        monkeypatch.delenv("GRADIENT_AGENT_ACCESS_KEY", raising=False)

        provider = AIProviderFactory.create_provider("auto")
        assert isinstance(provider, MockAIProvider)

    def test_auto_creates_gradient_with_env(self, monkeypatch):
        monkeypatch.setenv("GRADIENT_AGENT_ENDPOINT", "https://agent.test.com")
        monkeypatch.setenv("GRADIENT_AGENT_ACCESS_KEY", "test-key")

        provider = AIProviderFactory.create_provider("auto")
        assert isinstance(provider, GradientAIProvider)

    def test_invalid_type_raises(self):
        with pytest.raises(Exception):
            AIProviderFactory.create_provider("openai")


# ---------------------------------------------------------------------------
# GuardrailConfig
# ---------------------------------------------------------------------------

class TestGuardrailConfig:

    def test_valid_decision_passes(self):
        from aexis.core.gradient.guardrail_config import GuardrailConfig

        data = {
            "accepted_requests": ["p_001"],
            "rejected_requests": [],
            "route": ["1", "2"],
            "confidence": 0.9,
        }
        assert GuardrailConfig.validate_decision_schema(data) is True

    def test_missing_field_fails(self):
        from aexis.core.gradient.guardrail_config import GuardrailConfig

        data = {"accepted_requests": [], "route": ["1"]}
        assert GuardrailConfig.validate_decision_schema(data) is False

    def test_empty_route_fails(self):
        from aexis.core.gradient.guardrail_config import GuardrailConfig

        data = {
            "accepted_requests": [],
            "rejected_requests": [],
            "route": [],
            "confidence": 0.5,
        }
        assert GuardrailConfig.validate_decision_schema(data) is False

    def test_confidence_out_of_range_fails(self):
        from aexis.core.gradient.guardrail_config import GuardrailConfig

        data = {
            "accepted_requests": [],
            "rejected_requests": [],
            "route": ["1"],
            "confidence": 1.5,
        }
        assert GuardrailConfig.validate_decision_schema(data) is False

    def test_sanitize_clamps_confidence(self):
        from aexis.core.gradient.guardrail_config import GuardrailConfig

        data = {"confidence": 2.0, "route": ["1"]}
        sanitized = GuardrailConfig.sanitize_decision(data)
        assert sanitized["confidence"] == 1.0

    def test_sanitize_strips_invalid_stations(self):
        from aexis.core.gradient.guardrail_config import GuardrailConfig

        data = {
            "accepted_requests": [],
            "rejected_requests": [],
            "route": ["1", "invalid_station", "2"],
            "confidence": 0.5,
            "estimated_duration": 10,
        }
        valid_ids = {"1", "2", "3", "4"}
        sanitized = GuardrailConfig.sanitize_decision(data, valid_station_ids=valid_ids)
        assert sanitized["route"] == ["1", "2"]
