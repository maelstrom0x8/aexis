import logging
from abc import ABC, abstractmethod

from .errors import ErrorCode, create_error
from .model import Decision, DecisionContext

logger = logging.getLogger(__name__)


class AIProvider(ABC):
    """Abstract base class for AI decision providers"""

    @abstractmethod
    async def make_decision(self, context: DecisionContext) -> Decision:
        """Make routing decision using AI"""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if AI provider is available"""
        pass

    @abstractmethod
    async def close(self):
        """Cleanup resources used by the provider"""
        pass


class GradientAIProvider(AIProvider):
    """DigitalOcean Gradient™ AI provider implementation"""

    def __init__(self, client, workspace_id: str, model_slug: str = "llama-3-8b-instruct"):
        self.client = client
        self.workspace_id = workspace_id
        self.model_slug = model_slug
        self.call_count = 0

    async def make_decision(self, context: DecisionContext) -> Decision:
        """Make decision using DigitalOcean Gradient™"""
        try:
            if not self.is_available():
                raise create_error(
                    ErrorCode.AI_PROVIDER_NOT_AVAILABLE,
                    component="GradientAIProvider",
                    context={"pod_id": context.pod_id},
                )

            prompt = self._build_prompt(context)
            
            # Using Gradient Serverless Inference
            # In a full-stack implementation, this would call a Gradient Agent
            # but for a direct swap, we use the inference API.
            response = await self._call_gradient(prompt)
            decision = self._parse_response(response, context)

            self.call_count += 1
            return decision

        except Exception as e:
            logger.error(f"Gradient AI decision failed: {e}", exc_info=True)
            raise

    def is_available(self) -> bool:
        """Check if Gradient is available"""
        return self.client is not None and self.workspace_id is not None

    def get_provider_name(self) -> str:
        return f"DigitalOcean Gradient ({self.model_slug})"

    async def _call_gradient(self, prompt: str):
        """Call Gradient API with exponential backoff retry logic"""
        import asyncio
        import random
        
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                model = self.client.get_base_model(base_model_slug=self.model_slug)
                full_prompt = f"{self._get_system_instruction()}\n\n{prompt}"
                
                response = await asyncio.to_thread(
                    model.complete,
                    query=full_prompt,
                    max_generated_token_count=1024,
                    temperature=0.2
                )
                return response
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Gradient API call failed after {max_retries} attempts: {e}")
                    raise
                
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                logger.warning(f"Gradient API attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)

    async def close(self):
        """Gradient client cleanup (if applicable)"""
        # The current Gradient client is stateless/sync-wrapper 
        # but we provide the hook for future SDK updates
        pass

    def _build_prompt(self, context: DecisionContext) -> str:
        """Build comprehensive prompt for decision making"""
        import json
        
        ctx_data = {
            "pod_id": context.pod_id,
            "current_location": context.current_location,
            "capacity": {"available": context.capacity_available},
            "weight": {"available": context.weight_available},
            "passengers": context.passengers if context.passengers else [],
            "cargo": context.cargo if context.cargo else [],
            "requests": context.available_requests,
            "network_status": context.network_state,
        }
        
        return f"""
Analyze the transportation scenario and output ONLY valid JSON matching the schema.

Context:
{json.dumps(ctx_data, indent=2, default=str)}

Decision Task:
1. Evaluate requests vs constraints.
2. Optimize route for efficiency.
3. Output JSON with fields: accepted_requests, rejected_requests, route, estimated_duration, confidence, reasoning.
"""

    def _get_system_instruction(self) -> str:
        """Get system instruction and formatting requirements"""
        return """
You are the AEXIS Central Routing Intelligence on DigitalOcean Gradient. 
Optimize pod routing for efficiency and safety.
CRITICAL ROUTING RULES:
1. If you have passengers or cargo onboard, you MUST include their destinations in your route.
2. Prioritize dropping off onboard payload before fulfilling new requests.
3. The first stops in your route should be the destinations of the payload currently onboard.
Output MUST be a single raw JSON object. No markdown. No reasoning outside JSON.
{
    "accepted_requests": [],
    "rejected_requests": [],
    "route": ["station_id", ...],
    "estimated_duration": <int>,
    "confidence": <float>,
    "reasoning": "<string>"
}
"""

    def _parse_response(self, response, context: DecisionContext) -> Decision:
        """Parse Gradient response into Decision object"""
        import json
        
        text = response.generated_output.strip()
        try:
            # Clean up potential markdown formatting
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
            
            data = json.loads(text)
            
            return Decision(
                decision_type="route_selection",
                accepted_requests=data.get("accepted_requests", []),
                rejected_requests=data.get("rejected_requests", []),
                route=data.get("route", [context.current_location]),
                estimated_duration=data.get("estimated_duration", 0),
                confidence=float(data.get("confidence", 0.0)),
                reasoning=data.get("reasoning", "Gradient decision"),
                fallback_used=False,
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Gradient response: {e}\nRaw: {text}")
            raise create_error(
                ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED,
                component="GradientAIProvider",
                context={"error": str(e), "text_snippet": text[:100]}
            )


class MockAIProvider(AIProvider):
    """Mock AI provider for testing and development"""

    def __init__(self, response_delay: float = 0.1):
        self.response_delay = response_delay
        self.call_count = 0

    async def make_decision(self, context: DecisionContext) -> Decision:
        """Make mock decision"""
        import asyncio

        await asyncio.sleep(self.response_delay)

        self.call_count += 1

        # Simple mock logic
        if context.available_requests:
            accepted = (
                [context.available_requests[0].get("id")]
                if context.available_requests
                else []
            )
        else:
            accepted = []

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

    def get_provider_name(self) -> str:
        return "Mock AI"

    async def close(self):
        pass


class AIProviderFactory:
    """Factory for creating AI providers"""

    @staticmethod
    def create_provider(provider_type: str, **kwargs) -> AIProvider:
        """Create AI provider instance"""
        if provider_type.lower() == "gradient":
            import os
            from gradientai import Gradient
            
            token = os.environ.get("GRADIENT_ACCESS_TOKEN")
            workspace_id = os.environ.get("GRADIENT_WORKSPACE_ID")
            
            if not token or not workspace_id:
                raise create_error(
                    ErrorCode.CONFIG_INVALID_VALUE,
                    component="AIProviderFactory",
                    context={"missing_env": ["GRADIENT_ACCESS_TOKEN", "GRADIENT_WORKSPACE_ID"]},
                )
                
            client = Gradient(access_token=token, workspace_id=workspace_id)
            return GradientAIProvider(client, workspace_id)

        elif provider_type.lower() == "mock":
            return MockAIProvider(kwargs.get("response_delay", 0.1))

        else:
            raise create_error(
                ErrorCode.CONFIG_INVALID_VALUE,
                component="AIProviderFactory",
                context={
                    "provider_type": provider_type,
                    "supported": ["gradient", "mock"],
                },
            )

    @staticmethod
    def get_available_providers() -> list[str]:
        """Get list of available provider types"""
        return ["gradient", "mock"]
