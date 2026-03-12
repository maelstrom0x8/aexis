# AEXIS - Autonomous Event-Driven Transportation Intelligence System

A decentralized autonomous transportation system powered by **DigitalOcean Gradient™ AI**, built for the DigitalOcean Gradient AI Hackathon.

## Overview

AEXIS models interlinked transit stations where autonomous Pods (agents) service dynamic inflows of passengers and cargo without central control. The system uses DigitalOcean Gradient AI as the "brain" for pod decisions, leveraging managed agents, knowledge bases, and guardrails for intelligent route optimization.

## DigitalOcean Gradient AI Ecosystem

AEXIS is built on a full-stack DigitalOcean architecture, ensuring speed, simplicity, and scale.

### Core Gradient Features
- **[Gradient Agents](https://docs.digitalocean.com/products/gradient/concepts/agents/)**: Autonomous routing agents built with the Gradient ADK.
- **[Gradient Knowledge Bases](https://docs.digitalocean.com/products/gradient/concepts/knowledge-bases/)**: Context-aware decision making using RAG based on network topology in DigitalOcean Spaces.
- **[Gradient Guardrails](https://docs.digitalocean.com/products/gradient/concepts/guardrails/)**: Safety-critical constraints and schema enforcement for all routing decisions.
- **[Serverless Inference](https://docs.digitalocean.com/products/gradient/getting-started/serverless-inference/)**: High-performance Llama 3 models power the intelligence layer.

### Infrastructure Integration
- **Hosting**: [DigitalOcean App Platform](https://www.digitalocean.com/products/app-platform) (AEXIS API & Dashboard)
- **Storage**: [DigitalOcean Spaces](https://www.digitalocean.com/products/spaces) (Data source for Knowledge Bases)
- **Persistence**: [DigitalOcean Managed Redis](https://www.digitalocean.com/products/managed-databases-redis) (Event messaging)

## Key Features

- 🚀 **Event-Driven Architecture**: Reactive, scalable system built on Redis
- 🧠 **AI-Powered Decisions**: Gradient AI for intelligent autonomous logistics
- 📊 **Real-Time Dashboard**: Live monitoring of pods, stations, and requests
- 🔄 **Production Resilient**: Robust error handling and managed infrastructure
- 🌐 **Mixed Transit**: Unified handling for both passengers and cargo

## Quick Start

```bash
# Clone and setup
git clone <repository>
cd aexis
cp .env.example .env

# Start local Redis for development
docker compose up -d

# Install dependencies and sync environment
uv sync

# Run the system
cd aexis
./run_services.sh
```

Access the visualizer at http://localhost:8000

## Simulation

To inject traffic into the live network:
```bash
uv run python payload_injector.py
```
