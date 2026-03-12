# Web Module

This module provides the web dashboard and API server for AEXIS.

## Architecture

### Loose Coupling Design
The web module is intentionally **decoupled** from the core AEXIS system:

```
Browser → FastAPI Dashboard → API Layer → Core System
```

- **Browser** - Web interface with real-time updates
- **Dashboard** - UI server that proxies to API layer
- **API Layer** - SystemAPI that interfaces with core system
- **Core System** - Handles transportation logic, AI decisions, event processing

### Benefits
- **Independent Deployment** - Each layer can run separately
- **Technology Flexibility** - Web stack can be changed without affecting core
- **Scalability** - Multiple web instances can connect to same API
- **Testing** - Each layer can be tested independently
- **Security** - API layer provides controlled access to core system

## Components

### `api.py`
SystemAPI layer that:
- Provides REST endpoints for core system access
- Handles error propagation and validation
- Serves as the single point of access to core system
- Can be deployed independently

### `dashboard.py`
FastAPI dashboard that:
- Provides web UI and WebSocket endpoints
- Proxies all requests through API layer
- Handles real-time updates via WebSocket
- Never accesses core system directly

### `server.py`
Web server orchestration:
- Combines API and dashboard into single server
- Manages mounting and routing
- Supports standalone deployment
- Graceful shutdown handling

## API Endpoints

### System Endpoints (API Layer)
- `GET /api/system/status` - Complete system state
- `GET /api/system/metrics` - System metrics only

### Resource Endpoints (API Layer)
- `GET /api/pods` - All pod states
- `GET /api/pods/{pod_id}` - Specific pod state
- `GET /api/stations` - All station states
- `GET /api/stations/{station_id}` - Specific station state

### Dashboard Endpoints
- `GET /` - Main dashboard HTML
- `GET /api/*` - Proxied to API layer
- `WS /ws` - Real-time event streaming

## Usage

### Integrated Mode
```python
# Web server runs as part of main system
from aexis.web.server import WebServer

web_server = WebServer()
await web_server.initialize(system)
await web_server.start()
```

### Separate API and Dashboard
```bash
# Terminal 1: Core system + API
python -m aexis.main

# Terminal 2: Dashboard (connects to API)
API_BASE_URL=http://localhost:8001 python -m aexis.web.dashboard
```

### Standalone Dashboard
```bash
# Dashboard with mock API
API_BASE_URL=http://localhost:8001 python -m aexis.web.dashboard
```

## Deployment Options

### Single Process
```bash
# Core system + API + dashboard together
ENABLE_WEB_SERVER=true python -m aexis.main
```

### Separate Processes
```bash
# Terminal 1: Core system + API
python -m aexis.main

# Terminal 2: Dashboard
python -m aexis.web.server
```

### Docker Deployment
```dockerfile
# Core system + API container
FROM python:3.12
COPY . /app
WORKDIR /app
RUN pip install -e .
CMD ["python", "-m", "aexis.main"]

# Dashboard container
FROM python:3.12
COPY . /app
WORKDIR /app
RUN pip install -e .
ENV API_BASE_URL=http://api-service:8001
CMD ["python", "-m", "aexis.web.server"]
```

## Configuration

### Environment Variables
```bash
# Web server configuration
UI_HOST=0.0.0.0
UI_PORT=8000
API_PORT=8001
ENABLE_WEB_SERVER=true

# API connection (for standalone dashboard)
API_BASE_URL=http://localhost:8001

# Core system configuration
REDIS_PASSWORD=your_password
AI_PROVIDER=mock
```

## Features

### API Layer
- **Single Access Point** - All system access through controlled endpoints
- **Error Handling** - Proper HTTP status codes and error messages
- **Validation** - Input validation and type safety
- **Independence** - Can be deployed separately from dashboard

### Dashboard Layer
- **Real-time Updates** - WebSocket streaming of system events
- **Proxy Pattern** - All data access through API layer
- **Graceful Degradation** - Handles API unavailability
- **Professional UI** - Modern web interface with charts

### Communication Flow
```
Browser Request → Dashboard → HTTP Client → API Layer → Core System
Browser WebSocket → Dashboard → Event Broadcasting
```

## Security Considerations

### API Layer
- **Controlled Access** - Single point to enforce security policies
- **Input Validation** - FastAPI automatic validation
- **Error Sanitization** - Prevents system details leakage

### Dashboard Layer
- **No Direct Access** - Cannot access core system directly
- **API Authentication** - Can add auth to API layer
- **CORS Configuration** - Configurable for production

### Network Isolation
- **API Internal** - Can be deployed in private network
- **Dashboard External** - Can be exposed to internet
- **Firewall Rules** - Only dashboard needs external access
