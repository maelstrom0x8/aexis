from dataclasses import dataclass
from enum import Enum
from typing import Any


class ErrorCode(Enum):
    """System-wide error codes for consistent error handling"""

    # System Errors (1000-1099)
    SYSTEM_INITIALIZATION_FAILED = 1000
    SYSTEM_SHUTDOWN_FAILED = 1001
    SYSTEM_NOT_RUNNING = 1002
    INVALID_CONFIGURATION = 1003

    # Redis Errors (1100-1199)
    REDIS_CONNECTION_FAILED = 1100
    REDIS_PUBLISH_FAILED = 1101
    REDIS_SUBSCRIBE_FAILED = 1102
    REDIS_AUTHENTICATION_FAILED = 1103
    REDIS_TIMEOUT = 1104
    REDIS_CHANNEL_NOT_FOUND = 1105

    # AI Provider Errors (1150-1199) - New category for general AI provider issues
    AI_PROVIDER_LIMIT_REACHED = 1150
    AI_PROVIDER_TIMEOUT = 1151

    # Gradient API Errors (1200-1299)
    GRADIENT_API_KEY_MISSING = 1200
    GRADIENT_WORKSPACE_ID_MISSING = 1201
    GRADIENT_RATE_LIMIT_EXCEEDED = 1202
    GRADIENT_QUOTA_EXCEEDED = 1203
    GRADIENT_MODEL_NOT_AVAILABLE = 1204
    GRADIENT_RESPONSE_PARSING_FAILED = 1205
    GRADIENT_TIMEOUT = 1206
    GRADIENT_SDK_ERROR = 1207

    # Pod Errors (1300-1399)
    POD_NOT_FOUND = 1300
    POD_INITIALIZATION_FAILED = 1301
    POD_DECISION_FAILED = 1302
    POD_INVALID_STATE = 1303
    POD_CAPACITY_EXCEEDED = 1304
    POD_ROUTE_INVALID = 1305
    POD_BATTERY_LOW = 1306

    # Station Errors (1400-1499)
    STATION_NOT_FOUND = 1400
    STATION_INITIALIZATION_FAILED = 1401
    STATION_CAPACITY_EXCEEDED = 1402
    STATION_CONGESTION_HIGH = 1403
    STATION_OFFLINE = 1404
    STATION_INVALID_REQUEST = 1405

    # Event Errors (1500-1599)
    EVENT_VALIDATION_FAILED = 1500
    EVENT_PROCESSING_FAILED = 1501
    EVENT_PUBLISHING_FAILED = 1502
    EVENT_TYPE_NOT_SUPPORTED = 1503
    EVENT_DATA_INVALID = 1504
    EVENT_TIMEOUT = 1505

    # Routing Errors (1600-1699)
    ROUTING_PATH_NOT_FOUND = 1600
    ROUTING_INVALID_DESTINATION = 1601
    ROUTING_NETWORK_DISCONNECTED = 1602
    ROUTING_ALGORITHM_FAILED = 1603
    ROUTING_CAPACITY_CONSTRAINT = 1604

    # Configuration Errors (1700-1799)
    CONFIG_MISSING_ENV_VAR = 1700
    CONFIG_INVALID_VALUE = 1701
    CONFIG_FILE_NOT_FOUND = 1702
    CONFIG_PARSE_ERROR = 1703

    # Network Errors (1800-1899)
    NETWORK_CONNECTION_FAILED = 1800
    NETWORK_TIMEOUT = 1801
    NETWORK_UNREACHABLE = 1802
    NETWORK_PROTOCOL_ERROR = 1803

    # Data Validation Errors (1900-1999)
    VALIDATION_MISSING_FIELD = 1900
    VALIDATION_INVALID_TYPE = 1901
    VALIDATION_OUT_OF_RANGE = 1902
    VALIDATION_INVALID_FORMAT = 1903
    VALIDATION_DUPLICATE_ID = 1904


@dataclass
class ErrorDetails:
    """Detailed error information"""

    code: ErrorCode
    message: str
    component: str
    context: dict[str, Any]
    timestamp: str
    severity: str = "error"  # error, warning, info
    retry_possible: bool = True
    suggested_action: str | None = None


class AexisException(Exception):
    """Base exception for AEXIS system"""

    def __init__(
        self,
        error_code: ErrorCode,
        message: str,
        component: str = "unknown",
        context: dict[str, Any] | None = None,
        **kwargs,
    ):
        self.error_code = error_code
        self.message = message
        self.component = component
        self.context = context or {}
        self.details = ErrorDetails(
            code=error_code,
            message=message,
            component=component,
            context=self.context,
            timestamp=kwargs.get("timestamp", ""),
            severity=kwargs.get("severity", "error"),
            retry_possible=kwargs.get("retry_possible", True),
            suggested_action=kwargs.get("suggested_action"),
        )
        super().__init__(self.message)


class SystemException(AexisException):
    """System-level exceptions"""

    pass


class RedisException(AexisException):
    """Redis-related exceptions"""

    pass


class GradientException(AexisException):
    """DigitalOcean Gradient™ API-related exceptions"""

    pass


class PodException(AexisException):
    """Pod-related exceptions"""

    pass


class StationException(AexisException):
    """Station-related exceptions"""

    pass


class EventException(AexisException):
    """Event-related exceptions"""

    pass


class RoutingException(AexisException):
    """Routing-related exceptions"""

    pass


class ValidationException(AexisException):
    """Data validation exceptions"""

    pass


class ConfigurationException(AexisException):
    """Configuration-related exceptions"""

    pass


# Error message templates
ERROR_MESSAGES = {
    ErrorCode.SYSTEM_INITIALIZATION_FAILED: "System failed to initialize: {reason}",
    ErrorCode.SYSTEM_SHUTDOWN_FAILED: "System failed to shutdown gracefully: {reason}",
    ErrorCode.SYSTEM_NOT_RUNNING: "System is not currently running",
    ErrorCode.INVALID_CONFIGURATION: "Invalid configuration: {reason}",
    ErrorCode.REDIS_CONNECTION_FAILED: "Failed to connect to Redis: {reason}",
    ErrorCode.REDIS_PUBLISH_FAILED: "Failed to publish message: {reason}",
    ErrorCode.REDIS_SUBSCRIBE_FAILED: "Failed to subscribe: {reason}",
    ErrorCode.REDIS_AUTHENTICATION_FAILED: "Redis authentication failed: {reason}",
    ErrorCode.REDIS_TIMEOUT: "Redis operation timed out: {reason}",
    ErrorCode.REDIS_CHANNEL_NOT_FOUND: "Redis channel not found: {reason}",
    ErrorCode.GRADIENT_API_KEY_MISSING: "Gradient access token not configured",
    ErrorCode.GRADIENT_WORKSPACE_ID_MISSING: "Gradient workspace ID not configured",
    ErrorCode.GRADIENT_RATE_LIMIT_EXCEEDED: "Gradient API rate limit exceeded",
    ErrorCode.GRADIENT_QUOTA_EXCEEDED: "Gradient API quota exceeded",
    ErrorCode.GRADIENT_MODEL_NOT_AVAILABLE: "Gradient model {model} is not available",
    ErrorCode.GRADIENT_RESPONSE_PARSING_FAILED: "Failed to parse Gradient API response: {reason}",
    ErrorCode.GRADIENT_TIMEOUT: "Gradient API request timed out after {timeout}s",
    ErrorCode.GRADIENT_SDK_ERROR: "Gradient SDK error: {reason}",
    ErrorCode.POD_NOT_FOUND: "Pod {pod_id} not found",
    ErrorCode.POD_INITIALIZATION_FAILED: "Pod {pod_id} failed to initialize: {reason}",
    ErrorCode.POD_DECISION_FAILED: "Pod {pod_id} decision making failed: {reason}",
    ErrorCode.POD_INVALID_STATE: "Pod {pod_id} is in invalid state {state}",
    ErrorCode.POD_CAPACITY_EXCEEDED: "Pod {pod_id} capacity exceeded: {current}/{max}",
    ErrorCode.POD_ROUTE_INVALID: "Pod {pod_id} route {route} is invalid: {reason}",
    ErrorCode.POD_BATTERY_LOW: "Pod {pod_id} battery level {level} is below threshold {threshold}",
    ErrorCode.STATION_NOT_FOUND: "Station {station_id} not found",
    ErrorCode.STATION_INITIALIZATION_FAILED: "Station {station_id} failed to initialize: {reason}",
    ErrorCode.STATION_CAPACITY_EXCEEDED: "Station {station_id} capacity exceeded: {current}/{max}",
    ErrorCode.STATION_CONGESTION_HIGH: "Station {station_id} congestion level {level} is critical",
    ErrorCode.STATION_OFFLINE: "Station {station_id} is offline",
    ErrorCode.STATION_INVALID_REQUEST: "Station {station_id} received invalid request: {reason}",
    ErrorCode.EVENT_VALIDATION_FAILED: "Event validation failed: {reason}",
    ErrorCode.EVENT_PROCESSING_FAILED: "Event processing failed for {event_type}: {reason}",
    ErrorCode.EVENT_PUBLISHING_FAILED: "Event publishing failed for {event_type}: {reason}",
    ErrorCode.EVENT_TYPE_NOT_SUPPORTED: "Event type {event_type} is not supported",
    ErrorCode.EVENT_DATA_INVALID: "Event data is invalid: {error}",
    ErrorCode.EVENT_TIMEOUT: "Event processing timed out after {timeout}s",
    ErrorCode.ROUTING_PATH_NOT_FOUND: "No path found from {origin} to {destination}",
    ErrorCode.ROUTING_INVALID_DESTINATION: "Invalid destination station: {destination}",
    ErrorCode.ROUTING_NETWORK_DISCONNECTED: "Routing network is disconnected",
    ErrorCode.ROUTING_ALGORITHM_FAILED: "Routing algorithm failed: {reason}",
    ErrorCode.ROUTING_CAPACITY_CONSTRAINT: "Route capacity constraints cannot be satisfied",
    ErrorCode.CONFIG_MISSING_ENV_VAR: "Missing required environment variable: {var_name}",
    ErrorCode.CONFIG_INVALID_VALUE: "Invalid value for {var_name}: {value}",
    ErrorCode.CONFIG_FILE_NOT_FOUND: "Configuration file not found: {file_path}",
    ErrorCode.CONFIG_PARSE_ERROR: "Configuration parse error: {reason}",
    ErrorCode.NETWORK_CONNECTION_FAILED: "Network connection failed to {host}:{port}: {reason}",
    ErrorCode.NETWORK_TIMEOUT: "Network operation timed out after {timeout}s",
    ErrorCode.NETWORK_UNREACHABLE: "Network host {host} is unreachable",
    ErrorCode.NETWORK_PROTOCOL_ERROR: "Network protocol error: {reason}",
    ErrorCode.VALIDATION_MISSING_FIELD: "Missing required field: {field_name}",
    ErrorCode.VALIDATION_INVALID_TYPE: "Invalid type: {reason}",
    ErrorCode.VALIDATION_OUT_OF_RANGE: "Value out of range: {reason}",
    ErrorCode.VALIDATION_INVALID_FORMAT: "Invalid format: {reason}",
    ErrorCode.VALIDATION_DUPLICATE_ID: "Duplicate ID: {reason}",
}


def create_error(
    error_code: ErrorCode,
    component: str = "unknown",
    context: dict[str, Any] | None = None,
    **kwargs,
) -> AexisException:
    """Create a typed exception with proper error details"""

    # Get error message template
    template = ERROR_MESSAGES.get(error_code, "Unknown error: {error_code}")

    # Format message with context
    try:
        message = template.format(**(context or {}), **kwargs)
    except KeyError as e:
        message = f"Error formatting failed for {error_code}: {e}. Original: {template}"

    # Determine exception class
    exception_map = {
        1000: SystemException,
        1100: RedisException,
        1200: GradientException,
        1300: PodException,
        1400: StationException,
        1500: EventException,
        1600: RoutingException,
        1700: ConfigurationException,
        1900: ValidationException,
    }

    # Get exception class by error code range
    exception_class = SystemException  # Default
    for code_range, exc_class in exception_map.items():
        if error_code.value >= code_range and error_code.value < code_range + 100:
            exception_class = exc_class
            break

    # Create and return exception
    return exception_class(
        error_code=error_code,
        message=message,
        component=component,
        context=context or {},
        **kwargs,
    )


def handle_exception(exc: Exception, component: str = "unknown") -> ErrorDetails:
    """Convert any exception to standardized ErrorDetails"""

    if isinstance(exc, AexisException):
        return exc.details

    # Handle standard exceptions
    if isinstance(exc, ConnectionError):
        return create_error(
            ErrorCode.NETWORK_CONNECTION_FAILED,
            component=component,
            context={"original_error": str(exc)},
        ).details

    if isinstance(exc, TimeoutError):
        return create_error(
            ErrorCode.NETWORK_TIMEOUT,
            component=component,
            context={"original_error": str(exc)},
        ).details

    if isinstance(exc, ValueError):
        return create_error(
            ErrorCode.VALIDATION_INVALID_FORMAT,
            component=component,
            context={"original_error": str(exc)},
        ).details

    if isinstance(exc, KeyError):
        return create_error(
            ErrorCode.VALIDATION_MISSING_FIELD,
            component=component,
            context={"original_error": str(exc)},
        ).details

    # Generic exception
    return ErrorDetails(
        code=ErrorCode.SYSTEM_INITIALIZATION_FAILED,
        message=f"Unexpected error in {component}: {str(exc)}",
        component=component,
        context={"original_error": str(exc), "exception_type": type(exc).__name__},
        timestamp="",
        severity="error",
        retry_possible=True,
        suggested_action="Check system logs and restart if necessary",
    )
