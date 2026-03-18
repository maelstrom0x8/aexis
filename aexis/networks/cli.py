#!/usr/bin/env python3
"""CLI utility for managing and switching between network configurations.

Usage:
    python -m aexis.networks.cli list
    python -m aexis.networks.cli info <network_name>
    python -m aexis.networks.cli validate <network_name>
    python -m aexis.networks.cli env <network_name>
"""

import json
import sys
from pathlib import Path

from aexis.networks import NetworkLoader


def cmd_list(loader: NetworkLoader) -> int:
    """List all available networks."""
    networks = loader.list_networks()

    if not networks:
        print("No networks found.")
        return 0

    print("Available Networks:")
    print("-" * 80)

    for name, info in networks.items():
        print(f"\n{name}")
        print(f"  Description: {info['description']}")
        print(f"  Stations:    {info['node_count']}")
        print(f"  Path:        {info['path']}")

    return 0


def cmd_info(loader: NetworkLoader, network_name: str) -> int:
    """Show detailed information about a network."""
    try:
        network = loader.load_network(network_name)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}")
        return 1

    print(f"Network: {network.get('name', network_name)}")
    print(f"Description: {network.get('description', 'N/A')}")
    print(f"\nStations ({len(network.get('nodes', []))})")
    print("-" * 80)

    for node in network.get("nodes", []):
        node_id = node.get("id")
        label = node.get("label")
        coord = node.get("coordinate", {})
        adj_count = len(node.get("adj", []))
        print(f"  {node_id:4} ({label:10}) @ ({coord.get('x', 0):7.1f}, {coord.get('y', 0):7.1f})  →  {adj_count} connections")

    return 0


def cmd_validate(loader: NetworkLoader, network_name: str) -> int:
    """Validate a network configuration."""
    is_valid, message = loader.validate_network(network_name)
    print(message)
    return 0 if is_valid else 1


def cmd_env(loader: NetworkLoader, network_name: str) -> int:
    """Print environment variable export command."""
    try:
        path = loader.get_network_path(network_name)
        print(f"export AEXIS_NETWORK_DATA={path}")
        return 0
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def main() -> int:
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python -m aexis.networks.cli <command> [args]")
        print("\nCommands:")
        print("  list                      List all available networks")
        print("  info <network_name>       Show detailed network information")
        print("  validate <network_name>   Validate a network configuration")
        print("  env <network_name>        Print AEXIS_NETWORK_DATA export command")
        return 0

    try:
        loader = NetworkLoader()
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    command = sys.argv[1]

    if command == "list":
        return cmd_list(loader)
    elif command == "info":
        if len(sys.argv) < 3:
            print("Usage: python -m aexis.networks.cli info <network_name>")
            return 1
        return cmd_info(loader, sys.argv[2])
    elif command == "validate":
        if len(sys.argv) < 3:
            print("Usage: python -m aexis.networks.cli validate <network_name>")
            return 1
        return cmd_validate(loader, sys.argv[2])
    elif command == "env":
        if len(sys.argv) < 3:
            print("Usage: python -m aexis.networks.cli env <network_name>")
            return 1
        return cmd_env(loader, sys.argv[2])
    else:
        print(f"Unknown command: {command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
