#!/bin/bash

# AEXIS Service Runner
# Starts Redis, Core API, and Web Dashboard

# Function to kill all child processes on exit
cleanup() {
    echo "Shutting down services..."
    kill $(jobs -p) 2>/dev/null
    wait
    echo "All services stopped."
}

trap cleanup SIGINT SIGTERM

# Check for .env
if [ ! -f .env ]; then
    echo "Error: .env file not found"
    exit 1
fi

# Load helpful environment variables for checking
set -a
source .env
set +a
# Move to python project root
cd aexis

# Activate virtual environment
source .venv/bin/activate

# Add parent directory to python path so we can import 'aexis' package
export PYTHONPATH=$PYTHONPATH:..

echo "Using Python: $(which python)"
pip list | grep uvicorn

echo "Starting AEXIS Services..."

# 1. Start Redis (if not running via docker)
# Check if redis container is running
if ! docker ps | grep -q "aexis-redis"; then
    echo "Starting Redis via Docker..."
    docker-compose up -d aexis-redis
    sleep 2
fi

# 2. Start Core API (Port 8001)
echo "Starting Core API on port 8001..."
python -m aexis.api.main &
API_PID=$!

# Wait for API to warm up
echo "Waiting for API initialization..."
sleep 5

# 3. Start Web Dashboard (Port 8000)
echo "Starting Web Dashboard on port 8000..."
python -m aexis.web.main &
WEB_PID=$!

echo "Services started!"
echo "├── Core API: http://localhost:8001"
echo "└── Dashboard: http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop all services."

# Wait for processes
wait
