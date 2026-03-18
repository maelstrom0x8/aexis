# Network Configurations

This directory contains multiple network topology configurations that can be loaded in development or testing modes.

## Available Networks

### `simple.json` - Simple 4-Station Fully-Meshed Network

- **Stations**: 4 (all connected to each other)
- **Use Case**: Basic integration testing, simple routing verification
- **Characteristics**: Complete graph, equal edge weights
- **Best For**: Quick tests, demonstrating fundamental pod behavior

### `triangle.json` - 3-Station Triangle Topology

- **Stations**: 3 (forming a triangle)
- **Use Case**: Basic routing with multiple path options
- **Characteristics**: Small, symmetric, simple enough for manual verification
- **Best For**: Edge case testing, demonstrating path selection

### `linear_5.json` - 5-Station Linear Chain

- **Stations**: 5 (arranged in a line: 1-2-3-4-5)
- **Use Case**: Testing sequential routing and long-distance delivery
- **Characteristics**: Single path between endpoints, no alternative routes
- **Best For**: Testing deadlock scenarios, verifying long-distance routing

### `star_hub.json` - Star Topology with Central Hub

- **Stations**: 5 (1 hub + 4 spokes)
- **Use Case**: Hub contention testing, central distribution center scenarios
- **Characteristics**: All traffic flows through central hub
- **Best For**: Testing hub bottleneck scenarios, resource contention

### `disconnected.json` - Two Isolated Clusters

- **Stations**: 4 (2 clusters of 2 stations each)
- **Use Case**: Testing unreachable destination handling
- **Characteristics**: Two separate connected components (a1-a2) and (b1-b2)
- **Best For**: Verifying error handling for impossible routes, isolation testing

### `production.json` - Production Network (21 Stations)

- **Stations**: 21
- **Use Case**: Real-world topology testing with realistic weights
- **Characteristics**: Complex mesh with variable edge weights based on Euclidean distances
- **Best For**: Production-like testing, performance profiling, stress testing
- **Source**: Derived from `/aexis/network.json`

### `metroplex.json` - High-Complexity 31-Station Mesh

- **Stations**: 31 (organized in 5 clusters)
- **Clusters**:
  - **Central Hub**: 5 stations (fully meshed core)
  - **North Cluster**: 7 stations (ring topology + hub bridges)
  - **East Cluster**: 7 stations (tree topology + hub bridges)
  - **West Cluster**: 7 stations (ring topology + hub bridges)
  - **South Cluster**: 5 stations (star topology + hub bridges)
- **Pods**: 18 distributed (14 passenger + 4 cargo)
- **Use Case**: Large-scale stress testing, multi-cluster routing
- **Characteristics**: Strategic inter-cluster connections, balanced pod distribution
- **Best For**: Stress testing, verifying system stability under high load, cluster routing

## Usage

### Command Line (Set Environment Variable)

Switch networks by setting the `AEXIS_NETWORK_DATA` environment variable:

```bash
# Load a specific network before running the system
export AEXIS_NETWORK_DATA=/path/to/aexis/networks/triangle.json
python scripts/launch.py

# Or set it inline
AEXIS_NETWORK_DATA=/path/to/aexis/networks/metroplex.json python scripts/launch.py
```

### In Python Code

Use the `NetworkLoader` utility class:

```python
from aexis.networks import NetworkLoader

loader = NetworkLoader()

# List available networks
networks = loader.list_networks()
for name, info in networks.items():
    print(f"{name}: {info['description']} ({info['node_count']} stations)")

# Load a specific network
network_data = loader.load_network("metroplex")

# Set environment variable to activate a network
loader.set_environment_network("star_hub")

# Get path to a network file
path = loader.get_network_path("simple")
```

### In Configuration

Update `.env` to use a specific network:

```bash
# .env
AEXIS_NETWORK_DATA=/home/user/dev/aexis/aexis/networks/metroplex.json
```

## Creating New Networks

Network JSON files follow this structure:

```json
{
  "name": "network_name",
  "description": "Human-readable description",
  "nodes": [
    {
      "id": "station_id",
      "label": "display_label",
      "coordinate": {
        "x": 100,
        "y": 200
      },
      "adj": [
        {
          "node_id": "adjacent_station_id",
          "weight": 123.45
        }
      ]
    }
  ]
}
```

### Field Descriptions

- **id**: Unique station identifier (string or number as string)
- **label**: Display name for UI visualization
- **coordinate**: 2D position for visualization (x, y coordinates)
- **adj**: Array of adjacent stations with edge weights
- **weight**: Edge weight (typically Euclidean distance or estimated travel time)

### Guidelines for New Networks

1. **Keep node IDs sequential or meaningful** - Easier to trace in logs
2. **Make edges bidirectional** - If A connects to B, B should connect to A with same weight
3. **Use realistic weights** - Base on Euclidean distance or expected travel time
4. **Test connectivity** - Verify important pairs are reachable
5. **Document the topology** - Include description and use case in the JSON

## Testing Networks

Run the parametric test suite with different networks:

```bash
# The test suite automatically loads networks from test scenarios
pytest aexis/tests/test_template_parametric.py -v

# Run with a specific network enabled
AEXIS_NETWORK_DATA=/path/to/networks/metroplex.json pytest aexis/tests/
```

## Network Selection Guide

| Use Case                         | Recommended Network               |
| -------------------------------- | --------------------------------- |
| Quick smoke tests                | `simple.json`                     |
| Basic routing verification       | `triangle.json`                   |
| Long-distance routing            | `linear_5.json`                   |
| Hub contention testing           | `star_hub.json`                   |
| Unreachable destination handling | `disconnected.json`               |
| Production-like testing          | `production.json`                 |
| High-load stress testing         | `metroplex.json`                  |
| Development/exploration          | `simple.json` or `metroplex.json` |

## Performance Notes

- **Small networks** (simple, triangle, star_hub): Fast routing, suitable for interactive development
- **Medium networks** (linear_5, production): Balanced for testing and profiling
- **Large networks** (metroplex): Stress testing, performance benchmarking, system stability verification

## Related Files

- Main network configuration: `/aexis/network.json` (production network)
- Network core module: `/aexis/core/network.py` (NetworkContext, routing algorithms)
- Test scenarios: `/aexis/tests/test_template_parametric.py` (parametric test suite)
- Environment configuration: `/.env` (AEXIS_NETWORK_DATA variable)
