
import json
import os
from pathlib import Path
from typing import Optional

class NetworkLoader:

    def __init__(self, networks_dir: Optional[Path] = None):
        if networks_dir is None:

            self.networks_dir = Path(__file__).parent / "networks"
        else:
            self.networks_dir = Path(networks_dir)

        if not self.networks_dir.exists():
            raise FileNotFoundError(
                f"Networks directory not found: {self.networks_dir}"
            )

    def list_networks(self) -> dict[str, dict]:
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
        network_file = self.networks_dir / f"{network_name}.json"
        if not network_file.exists():
            available = list(self.list_networks().keys())
            raise FileNotFoundError(
                f"Network '{network_name}' not found. Available networks: {available}"
            )
        return str(network_file.absolute())

    def load_network(self, network_name: str) -> dict:
        path = self.get_network_path(network_name)
        with open(path) as f:
            return json.load(f)

    def validate_network(self, network_name: str) -> tuple[bool, str]:
        try:
            self.load_network(network_name)
            return True, f"Network '{network_name}' is valid"
        except FileNotFoundError as e:
            return False, str(e)
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON in '{network_name}': {e}"

    def set_environment_network(self, network_name: str) -> None:
        path = self.get_network_path(network_name)
        os.environ["AEXIS_NETWORK_DATA"] = path

def get_default_loader() -> NetworkLoader:
    return NetworkLoader()
