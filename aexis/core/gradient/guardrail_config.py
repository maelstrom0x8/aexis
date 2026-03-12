import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class GuardrailConfig:
    """Configures and validates DigitalOcean Gradient Guardrails"""

    @staticmethod
    def get_routing_guardrail_spec() -> Dict[str, Any]:
        """Get the specification for the routing safety guardrail"""
        return {
            "name": "aexis-routing-safety",
            "type": "content_filtering",
            "config": {
                "input_filters": [
                    {"type": "pii", "action": "block"},
                    {"type": "toxic", "action": "block"}
                ],
                "output_filters": [
                    {"type": "malformed_json", "action": "fix"},
                    {"type": "safety_violation", "action": "block"}
                ],
                "predefined_response": "Safety constraint violated: Routing decision blocked by AEXIS Guardrails."
            }
        }

    @staticmethod
    def validate_decision_schema(decision_data: Dict[str, Any]) -> bool:
        """Client-side validation to complement Gradient Guardrails"""
        required_fields = [
            "accepted_requests", 
            "rejected_requests", 
            "route", 
            "confidence"
        ]
        return all(field in decision_data for field in required_fields)
