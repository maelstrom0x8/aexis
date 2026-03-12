# AEXIS Core

The engine behind the Autonomous Event-Driven Transportation Intelligence System.

## Architecture

AEXIS Core implements a decentralized routing logic where each Pod acts as an autonomous agent.

- **Intelligence**: Powered by `GradientAIProvider` using DigitalOcean Gradient AI.
- **Messaging**: Event-driven coordination via Redis.
- **Modeling**: Discrete event simulation with real-time trajectory updates.

## Key Components

- `aexis.core.pod`: Autonomous agent logic (Passenger & Cargo variants).
- `aexis.core.ai_provider`: Integration with DigitalOcean Gradient SDK.
- `aexis.core.network`: Topology and movement physics.
- `aexis.core.gradient`: Managers for Knowledge Bases and Guardrails.

## Configuration

The core system is configured via environment variables and `aexis.json`:

| Variable | Description |
|----------|-------------|
| `AI_PROVIDER` | Set to `gradient` for production |
| `GRADIENT_ACCESS_TOKEN` | Your DigitalOcean Gradient API token |
| `GRADIENT_WORKSPACE_ID` | Your Gradient Workspace identifier |
| `REDIS_URL` | Connection string for DigitalOcean Managed Redis |

## Development

```bash
uv sync
uv run python main.py
```

## Production Requirements

AEXIS follows strict production standards:
- **Robust Error Handling**: Specific recovery for API and Network failures.
- **Input Validation**: Schema enforcement via Gradient Guardrails.
- **Scalability**: Designed for DigitalOcean App Platform horizontal scaling.
