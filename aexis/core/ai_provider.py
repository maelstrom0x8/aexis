
import asyncio
import json
import logging
import os
import random
from abc import ABC, abstractmethod

import httpx

from .errors import ErrorCode, GradientException, create_error
from .model import Decision, DecisionContext

logger = logging.getLogger(__name__)

_DECISION_SCHEMA_FIELDS = {
    "accepted_requests": list,
    "rejected_requests": list,
    "route": list,
    "estimated_duration": (int, float),
    "confidence": (int, float),
    "reasoning": str,
}

class AIProvider(ABC):

    @abstractmethod
    async def make_decision(self, context: DecisionContext) -> Decision:
        pass

    @abstractmethod
    def is_available(self) -> bool:
        pass

    @abstractmethod
    async def close(self):
        pass

class GradientAIProvider(AIProvider):

    _MAX_RETRIES = 3
    _BASE_DELAY_SECONDS = 1.0
    _REQUEST_TIMEOUT_SECONDS = 30.0

    def __init__(
        self,
        agent_endpoint: str,
        agent_access_key: str,
    ):
        if not agent_endpoint:
            raise create_error(
                ErrorCode.CONFIG_MISSING_ENV_VAR,
                component="GradientAIProvider",
                context={"var_name": "GRADIENT_AGENT_ENDPOINT"},
            )
        if not agent_access_key:
            raise create_error(
                ErrorCode.CONFIG_MISSING_ENV_VAR,
                component="GradientAIProvider",
                context={"var_name": "GRADIENT_AGENT_ACCESS_KEY"},
            )

        self._base_url = agent_endpoint.rstrip("/")
        if not self._base_url.endswith("/api/v1"):
            self._base_url = f"{self._base_url}/api/v1"

        self._access_key = agent_access_key
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {self._access_key}",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(self._REQUEST_TIMEOUT_SECONDS, connect=10.0),
        )
        self.call_count = 0

    async def make_decision(self, context: DecisionContext) -> Decision:
        if not self.is_available():
            raise create_error(
                ErrorCode.AI_PROVIDER_LIMIT_REACHED,
                component="GradientAIProvider",
                context={"pod_id": context.pod_id},
            )

        prompt = self._build_prompt(context)
        raw_response = await self._call_agent(prompt, context.pod_id)
        decision = self._parse_response(raw_response, context)

        self.call_count += 1
        return decision

    def is_available(self) -> bool:
        return self._client is not None

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _call_agent(self, prompt: str, pod_id: str) -> dict:
        last_error: Exception | None = None

        for attempt in range(self._MAX_RETRIES):
            try:
                response = await self._client.post(
                    "/chat/completions",
                    json={

                        "model": "n/a",
                        "messages": [
                            {"role": "user", "content": prompt},
                        ],
                        "stream": False,

                        "include_retrieval_info": True,
                        "include_guardrails_info": True,
                    },
                )

                if response.status_code == 429:
                    raise create_error(
                        ErrorCode.GRADIENT_RATE_LIMIT_EXCEEDED,
                        component="GradientAIProvider",
                        context={"pod_id": pod_id},
                    )

                if response.status_code >= 500:
                    raise create_error(
                        ErrorCode.GRADIENT_SDK_ERROR,
                        component="GradientAIProvider",
                        context={
                            "reason": f"HTTP {response.status_code}: {response.text[:200]}",
                        },
                    )

                response.raise_for_status()
                body = response.json()

                self._log_platform_metadata(body, pod_id)

                return body

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = exc
                if attempt < self._MAX_RETRIES - 1:
                    delay = self._BASE_DELAY_SECONDS * (2 ** attempt) + random.uniform(0, 0.3)
                    logger.warning(
                        "Gradient Agent call attempt %d/%d failed for pod %s: %s. "
                        "Retrying in %.2fs",
                        attempt + 1, self._MAX_RETRIES, pod_id, exc, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise create_error(
                    ErrorCode.GRADIENT_TIMEOUT,
                    component="GradientAIProvider",
                    context={"timeout": self._REQUEST_TIMEOUT_SECONDS, "pod_id": pod_id},
                ) from exc

            except GradientException:
                raise

            except Exception as exc:
                last_error = exc
                if attempt < self._MAX_RETRIES - 1:
                    delay = self._BASE_DELAY_SECONDS * (2 ** attempt) + random.uniform(0, 0.3)
                    logger.warning(
                        "Gradient Agent call attempt %d/%d failed for pod %s: %s. "
                        "Retrying in %.2fs",
                        attempt + 1, self._MAX_RETRIES, pod_id, exc, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise create_error(
                    ErrorCode.GRADIENT_SDK_ERROR,
                    component="GradientAIProvider",
                    context={"reason": str(exc), "pod_id": pod_id},
                ) from exc

        raise create_error(
            ErrorCode.GRADIENT_SDK_ERROR,
            component="GradientAIProvider",
            context={"reason": str(last_error), "pod_id": pod_id},
        )

    def _log_platform_metadata(self, body: dict, pod_id: str) -> None:
        retrieval = body.get("retrieval")
        if retrieval:
            sources = retrieval.get("sources", [])
            logger.info(
                "Gradient RAG retrieved %d source(s) for pod %s",
                len(sources), pod_id,
            )

        guardrails = body.get("guardrails")
        if guardrails:
            actions = guardrails.get("actions", [])
            if actions:
                logger.warning(
                    "Gradient Guardrails triggered %d action(s) for pod %s: %s",
                    len(actions), pod_id, actions,
                )

    def _build_prompt(self, context: DecisionContext) -> str:
        ctx_data = {
            "pod_id": context.pod_id,
            "pod_type": context.pod_type,
            "current_location": context.current_location,
            "capacity_available": context.capacity_available,
            "weight_available": context.weight_available,
            "passengers_onboard": context.passengers if context.passengers else [],
            "cargo_onboard": context.cargo if context.cargo else [],
            "available_requests": context.available_requests,
            "network_state": context.network_state,
            "system_metrics": context.system_metrics,
        }

        return (
            "Make a routing decision for this pod. "
            "Output ONLY a raw JSON object matching the decision schema.\n\n"
            f"```json\n{json.dumps(ctx_data, indent=2, default=str)}\n```"
        )

    def _parse_response(self, body: dict, context: DecisionContext) -> Decision:
        choices = body.get("choices", [])
        if not choices:
            raise create_error(
                ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED,
                component="GradientAIProvider",
                context={"reason": "Empty choices array", "pod_id": context.pod_id},
            )

        text = choices[0].get("message", {}).get("content", "").strip()
        if not text:
            raise create_error(
                ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED,
                component="GradientAIProvider",
                context={"reason": "Empty message content", "pod_id": context.pod_id},
            )

        usage = body.get("usage")
        if usage:
            logger.debug(
                "Gradient tokens — prompt: %d, completion: %d, total: %d (pod %s)",
                usage.get("prompt_tokens", 0),
                usage.get("completion_tokens", 0),
                usage.get("total_tokens", 0),
                context.pod_id,
            )

        data = self._extract_json(text, context.pod_id)
        self._validate_decision_fields(data, context.pod_id)

        route = data.get("route", [])
        if not route:
            route = [context.current_location]

        confidence = data.get("confidence", 0.0)
        confidence = max(0.0, min(1.0, float(confidence)))

        return Decision(
            decision_type="route_selection",
            accepted_requests=data.get("accepted_requests", []),
            rejected_requests=data.get("rejected_requests", []),
            route=route,
            estimated_duration=int(data.get("estimated_duration", 0)),
            confidence=confidence,
            reasoning=data.get("reasoning", "Gradient Agent decision"),
            fallback_used=False,
        )

    def _extract_json(self, text: str, pod_id: str) -> dict:

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        cleaned = text
        if "```" in cleaned:

            cleaned = cleaned.replace("```json", "").replace("```", "").strip()
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError:
                pass

        first_brace = text.find("{")
        last_brace = text.rfind("}")
        if first_brace != -1 and last_brace > first_brace:
            try:
                return json.loads(text[first_brace : last_brace + 1])
            except json.JSONDecodeError:
                pass

        raise create_error(
            ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED,
            component="GradientAIProvider",
            context={
                "reason": "No valid JSON found in agent response",
                "text_snippet": text[:200],
                "pod_id": pod_id,
            },
        )

    def _validate_decision_fields(self, data: dict, pod_id: str) -> None:
        for field, expected_type in _DECISION_SCHEMA_FIELDS.items():
            if field not in data:
                raise create_error(
                    ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED,
                    component="GradientAIProvider",
                    context={
                        "reason": f"Missing required field: {field}",
                        "pod_id": pod_id,
                    },
                )
            if not isinstance(data[field], expected_type):
                raise create_error(
                    ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED,
                    component="GradientAIProvider",
                    context={
                        "reason": f"Field '{field}' has wrong type: "
                                  f"expected {expected_type}, got {type(data[field])}",
                        "pod_id": pod_id,
                    },
                )

class MockAIProvider(AIProvider):

    def __init__(self, response_delay: float = 0.1):
        self.response_delay = response_delay
        self.call_count = 0

    async def make_decision(self, context: DecisionContext) -> Decision:
        await asyncio.sleep(self.response_delay)
        self.call_count += 1

        accepted = []
        if context.available_requests:
            first_id = context.available_requests[0].get("id")
            if first_id:
                accepted = [first_id]

        return Decision(
            decision_type="route_selection",
            accepted_requests=accepted,
            rejected_requests=[],
            route=[context.current_location],
            estimated_duration=10,
            confidence=0.8,
            reasoning=f"Mock decision #{self.call_count}",
            fallback_used=False,
        )

    def is_available(self) -> bool:
        return True

    async def close(self):
        pass

class AIProviderFactory:

    @staticmethod
    def create_provider(provider_type: str = "auto", **kwargs) -> AIProvider:
        if provider_type == "auto":
            endpoint = os.environ.get("GRADIENT_AGENT_ENDPOINT", "")
            access_key = os.environ.get("GRADIENT_AGENT_ACCESS_KEY", "")
            if endpoint and access_key:
                provider_type = "gradient"
            else:
                provider_type = "mock"

        if provider_type == "gradient":
            endpoint = kwargs.get(
                "agent_endpoint",
                os.environ.get("GRADIENT_AGENT_ENDPOINT", ""),
            )
            access_key = kwargs.get(
                "agent_access_key",
                os.environ.get("GRADIENT_AGENT_ACCESS_KEY", ""),
            )
            return GradientAIProvider(
                agent_endpoint=endpoint,
                agent_access_key=access_key,
            )

        if provider_type == "mock":
            return MockAIProvider(
                response_delay=kwargs.get("response_delay", 0.1),
            )

        raise create_error(
            ErrorCode.CONFIG_INVALID_VALUE,
            component="AIProviderFactory",
            context={
                "var_name": "provider_type",
                "value": provider_type,
            },
        )

    @staticmethod
    def get_available_providers() -> list[str]:
        return ["gradient", "mock", "auto"]
