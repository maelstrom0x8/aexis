"""Network configuration loader for AEXIS system.

This module provides utilities for loading network configurations from the
networks/ directory and switching between different topologies via environment
variables or programmatically.

Example:
    Load a network by name::

        from aexis.networks import NetworkLoader
        
        # Load production network
        loader = NetworkLoader()
        network_path = loader.get_network_path("production")
        
        # Or use environment variable
        import os
        os.environ["AEXIS_NETWORK_DATA"] = loader.get_network_path("metroplex")
        
        # List all available networks
        available = loader.list_networks()
        print(f"Available networks: {available}")
"""

import json
import os
from pathlib import Path
from typing import Optional


class NetworkLoader:
    """Load and manage network configurations from the networks/ directory."""

    def __init__(self, networks_dir: Optional[Path] = None):
        """Initialize the network loader.

        Args:
            networks_dir: Optional path to networks directory. If not provided,
                         uses the default networks/ directory in the AEXIS package.
        """
        if networks_dir is None:
            # Default to networks/ in the same directory as this module
            self.networks_dir = Path(__file__).parent / "networks"
        else:
            self.networks_dir = Path(networks_dir)

        if not self.networks_dir.exists():
            raise FileNotFoundError(
                f"Networks directory not found: {self.networks_dir}"
            )

    def list_networks(self) -> dict[str, dict]:
        """List all available networks with their metadata.

        Returns:
            Dictionary mapping network names to their metadata (name, description).

        Example:
            {
                "triangle": {"name": "triangle", "description": "Simple 3-station..."},
                "linear_5": {"name": "linear_5", "description": "5-station chain..."},
                ...
            }
        """
        networks = {}
        for json_file in sorted(self.networks_dir.glob("*.json")):
            try:
                with open(json_file) as f:
                    data = json.load(f)
                    network_name = json_file.stem
                    networks[network_name] = {
                        "name": data.get("name", network_name),
                        "description": data.get("description", ""),
                        "node_count": len(data.get("nodes", [])),
                        "path": str(json_file),
                    }
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load {json_file}: {e}")

        return networks

    def get_network_path(self, network_name: str) -> str:
        """Get the full path to a network JSON file.

        Args:
            network_name: Name of the network (without .json extension).

        Returns:
            Absolute path to the network JSON file.

        Raises:
            FileNotFoundError: If the network does not exist.
        """
        network_file = self.networks_dir / f"{network_name}.json"
        if not network_file.exists():
            available = list(self.list_networks().keys())
            raise FileNotFoundError(
                f"Network '{network_name}' not found. Available networks: {available}"
            )
        return str(network_file.absolute())

    def load_network(self, network_name: str) -> dict:
        """Load and parse a network JSON file.

        Args:
            network_name: Name of the network (without .json extension).

        Returns:
            Parsed network configuration dictionary.

        Raises:
            FileNotFoundError: If the network does not exist.
            json.JSONDecodeError: If the JSON is invalid.
        """
        path = self.get_network_path(network_name)
        with open(path) as f:
            return json.load(f)

    def validate_network(self, network_name: str) -> tuple[bool, str]:
        """Validate that a network file exists and is valid JSON.

        Args:
            network_name: Name of the network to validate.

        Returns:
            Tuple of (is_valid, message).
        """
        try:
            self.load_network(network_name)
            return True, f"Network '{network_name}' is valid"
        except FileNotFoundError as e:
            return False, str(e)
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON in '{network_name}': {e}"

    def set_environment_network(self, network_name: str) -> None:
        """Set the AEXIS_NETWORK_DATA environment variable to a specific network.

        Args:
            network_name: Name of the network to activate.

        Raises:
            FileNotFoundError: If the network does not exist.
        """
        path = self.get_network_path(network_name)
        os.environ["AEXIS_NETWORK_DATA"] = path


def get_default_loader() -> NetworkLoader:
    """Get a default NetworkLoader instance."""
    return NetworkLoader()
