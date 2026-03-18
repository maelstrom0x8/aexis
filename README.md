# AEXIS - Autonomous Event-Driven Transportation Intelligence System

A decentralized autonomous transportation system powered by **DigitalOcean Gradient™ AI**, built for the DigitalOcean Gradient AI Hackathon.

## Overview

AEXIS models interlinked transit stations where autonomous Pods (agents) service dynamic inflows of passengers and cargo without central control. The system uses DigitalOcean Gradient AI as the "brain" for pod decisions, leveraging managed agents, knowledge bases, and guardrails for intelligent route optimization.

## Key Features

- **Event-Driven Architecture**: Reactive, scalable system built on Redis
- **AI-Powered Decisions**: Gradient AI for intelligent autonomous logistics
- **Real-Time Dashboard**: Live monitoring of pods, stations, and requests
- **Production Resilient**: Robust error handling and managed infrastructure
- **Mixed Transit**: Unified handling for both passengers and cargo

---

## Setup & Installation

### Prerequisites
- Python 3.12+
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) package manager (recommended)
- DigitalOcean Gradient™ Access Token & Workspace ID

### 1. Clone the Repository
```bash
git clone <repository>
cd aexis
```

### 2. Environment Configuration
Copy the example environment file and configure it:
```bash
cp .env.example .env
```
Edit `.env` and fill in your DigitalOcean credentials and preferences. Specifically:
- `REDIS_PASSWORD`: A secure password for the local Redis instance.
- `GRADIENT_ACCESS_TOKEN`: Your Gradient API token.
- `GRADIENT_WORKSPACE_ID`: Your Gradient workspace ID.

### 3. Start Infrastructure
AEXIS requires Redis as its message broker. Start the local Redis instance using Docker:
```bash
docker compose up -d
```
*(Verify Redis is running with `docker compose ps`)*

### 4. Install Dependencies
Install all required Python packages using `uv` (or pip):
```bash
uv sync
```

---

## Running the System

AEXIS consists of multiple microservices (Pods, Stations, API, and Web Dashboard). The easiest way to spin up the entire system is using the provided launch script.

```bash
# Start the system
./scripts/launch.py
```

The script will automatically:
1. Initialize the transit network from `network.json`.
2. Spin up independent station processes.
3. Launch passenger and cargo pod instances.
4. Start the backend API on port `8001`.
5. Start the Web Dashboard on port `8000`.

### Access Points
- **Web Dashboard (Visualizer)**: [http://localhost:8000](http://localhost:8000)
- **API Documentation (Swagger)**: [http://localhost:8001/docs](http://localhost:8001/docs)

*(Press `Ctrl+C` in the terminal to cleanly shut down all microservices).*

---

## System Usage & Interaction

Once the system is running, you can interact with it in several ways:

### 1. The Real-Time Dashboard
Navigate to [http://localhost:8000](http://localhost:8000) in your browser to watch the pods navigate the topology in real-time, view live metrics, and monitor station queues.

### 2. Interactive Command-Line Interface (CLI)
AEXIS includes a powerful interactive CLI to manage and query the live system.

Open a **new terminal** and run:
```bash
# Enter the interactive shell
python -m aexis.cli.console
```

**Common CLI Commands:**
- `status`: Displays current system health, uptime, and core metrics.
- `watch`: Opens a live-updating dashboard in your terminal.
- `pods`: Lists all active pods and their current state.
- `pods <pod_id>`: Shows detailed telemetry for a specific pod.
- `stations`: Lists all stations and their passenger/cargo wait queues.
- `stations <station_id>`: Shows detailed metrics for a specific station.
- `inject_passenger <origin> <destination> [count]`: Spawns passengers.
- `inject_cargo <origin> <destination> [weight]`: Spawns cargo requests.

*Type `help` in the CLI for a full list of commands.*

### 3. Automated Payload Injector
To simulate realistic traffic and test the AI's routing capabilities, you can run the automated payload injector in the background.

In a **new terminal**, run:
```bash
python payload_injector.py --host localhost:8001 --interval 5.0 --ratio 0.7
```
- `--interval`: Average seconds between new requests (default: 5.0).
- `--ratio`: Ratio of passenger vs. cargo requests (default: 0.7 / 70% pax).

This script will continuously spawn new requests across random stations, forcing the pods to dynamically calculate optimal routes using Gradient AI.

---

## Configuration Details

You can modify system behavior by adjusting the `.env` file variables:

- **Network Layout**:
  - `AEXIS_NETWORK_DATA`: Path to the network data file (default: `./aexis/networks/metroplex.json`).
- **Fleet Sizing**: 
  - `POD_COUNT`: Total number of pods to spawn (default: 5).
- **Core Networking**:
  - `API_PORT` (default: 8001) and `UI_PORT` (default: 8000).
- **AI Routing**:
  - `AI_PROVIDER`: Set to `gradient` to use cloud inference, or `mock` for local deterministic routing fallback.

