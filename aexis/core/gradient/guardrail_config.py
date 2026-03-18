"""Client-side guardrail validation and sanitization.

The platform-side Gradient Guardrails handle content moderation, jailbreak
detection, and sensitive data filtering.  This module provides a *second layer*
of structural validation on the parsed decision JSON to catch schema issues
the LLM may produce that slip through the platform guardrails.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GuardrailConfig:
    """Configures and validates DigitalOcean Gradient Guardrails.

    Platform-side guardrails are attached to the agent in the DO control panel.
    This class handles client-side structural validation that complements
    the platform's content-level guardrails.
    """

    REQUIRED_DECISION_FIELDS = {
        "accepted_requests": list,
        "rejected_requests": list,
        "route": list,
        "confidence": (int, float),
    }

    @staticmethod
    def get_routing_guardrail_spec() -> dict[str, Any]:
        """Platform guardrail specification for the routing agent.

        This spec documents what is configured on the Gradient platform.
        It is not sent programmatically — it exists for reference and auditing.
        """
        return {
            "name": "aexis-routing-safety",
            "guardrails": [
                {
                    "type": "content_moderation",
                    "description": "Block toxic or harmful routing instructions",
                    "enabled": True,
                },
                {
                    "type": "jailbreak_detection",
                    "description": "Prevent prompt injection attacks",
                    "enabled": True,
                },
                {
                    "type": "sensitive_data",
                    "description": "Redact PII from passenger metadata",
                    "enabled": True,
                },
            ],
        }

    @staticmethod
    def validate_decision_schema(decision_data: dict[str, Any]) -> bool:
        """Validate that a decision dict has the required structure.

        Returns True if valid, False otherwise.  Logs specific validation
        failures for debugging.
        """
        for field, expected_type in GuardrailConfig.REQUIRED_DECISION_FIELDS.items():
            if field not in decision_data:
                logger.warning("Decision schema validation failed: missing field '%s'", field)
                return False
            if not isinstance(decision_data[field], expected_type):
                logger.warning(
                    "Decision schema validation failed: field '%s' expected %s, got %s",
                    field,
                    expected_type,
                    type(decision_data[field]),
                )
                return False

        # Route must be a non-empty list of strings
        route = decision_data.get("route", [])
        if not route:
            logger.warning("Decision schema validation failed: route is empty")
            return False
        if not all(isinstance(s, str) for s in route):
            logger.warning("Decision schema validation failed: route contains non-string elements")
            return False

        # Confidence must be 0–1
        confidence = decision_data.get("confidence", -1)
        if not (0.0 <= float(confidence) <= 1.0):
            logger.warning(
                "Decision schema validation failed: confidence %.3f out of [0, 1]",
                confidence,
            )
            return False

        return True

    @staticmethod
    def sanitize_decision(
        decision_data: dict[str, Any],
        valid_station_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        """Sanitize a decision dict by clamping/fixing invalid values.

        Args:
            decision_data: Raw parsed decision from the agent.
            valid_station_ids: If provided, strip station IDs not in this set.

        Returns:
            A cleaned copy of the decision data.
        """
        sanitized = dict(decision_data)

        # Clamp confidence to [0, 1]
        raw_confidence = sanitized.get("confidence", 0.5)
        sanitized["confidence"] = max(0.0, min(1.0, float(raw_confidence)))

        # Ensure lists are lists (not None)
        for list_field in ("accepted_requests", "rejected_requests", "route"):
            if not isinstance(sanitized.get(list_field), list):
                sanitized[list_field] = []

        # Ensure estimated_duration is a non-negative int
        raw_duration = sanitized.get("estimated_duration", 0)
        try:
            sanitized["estimated_duration"] = max(0, int(raw_duration))
        except (ValueError, TypeError):
            sanitized["estimated_duration"] = 0

        # Strip station IDs not present in the network
        if valid_station_ids and sanitized.get("route"):
            original_route = sanitized["route"]
            filtered_route = [s for s in original_route if s in valid_station_ids]
            if len(filtered_route) < len(original_route):
                removed = set(original_route) - set(filtered_route)
                logger.warning(
                    "Guardrail sanitization removed invalid station IDs from route: %s",
                    removed,
                )
            sanitized["route"] = filtered_route

        return sanitized

    @staticmethod
    def log_guardrail_actions(guardrail_info: dict[str, Any] | None) -> None:
        """Log guardrail actions returned in agent response metadata.

        The Gradient platform returns guardrail info when
        ``include_guardrails_info=True`` is set in the request.
        """
        if not guardrail_info:
            return

        actions = guardrail_info.get("actions", [])
        if not actions:
            return

        for action in actions:
            action_type = action.get("type", "unknown")
            action_result = action.get("result", "unknown")
            logger.info(
                "Platform guardrail action: type=%s result=%s",
                action_type,
                action_result,
            )
