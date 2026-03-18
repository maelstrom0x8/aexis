import cmd
import json
import logging
import os
import sys
from typing import Any

import httpx
from tabulate import tabulate

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

class APIClient:

    def __init__(self, base_url: str = "http://localhost:8001"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=5.0)

    def check_health(self) -> bool:
        try:
            resp = self.client.get(f"{self.base_url}/api/system/status")
            return resp.status_code == 200
        except:
            return False

    def get_system_state(self) -> dict[str, Any]:
        resp = self.client.get(f"{self.base_url}/api/system/status")
        resp.raise_for_status()
        return resp.json()

    def get_all_pods(self) -> dict[str, Any]:
        resp = self.client.get(f"{self.base_url}/api/pods")
        resp.raise_for_status()
        return resp.json()

    def get_pod_state(self, pod_id: str) -> dict[str, Any] | None:
        try:
            resp = self.client.get(f"{self.base_url}/api/pods/{pod_id}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            return None

    def get_all_stations(self) -> dict[str, Any]:
        resp = self.client.get(f"{self.base_url}/api/stations")
        resp.raise_for_status()
        return resp.json()

    def get_station_state(self, station_id: str) -> dict[str, Any] | None:
        try:
            resp = self.client.get(f"{self.base_url}/api/stations/{station_id}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            return None

    def inject_passenger(self, origin: str, dest: str, count: int) -> bool:
        payload = {"origin": origin, "destination": dest, "count": count}
        resp = self.client.post(f"{self.base_url}/api/manual/passenger", json=payload)
        resp.raise_for_status()
        return True

    def inject_cargo(self, origin: str, dest: str, weight: float) -> bool:
        payload = {"origin": origin, "destination": dest, "weight": weight}
        resp = self.client.post(f"{self.base_url}/api/manual/cargo", json=payload)
        resp.raise_for_status()
        return True

class AexisCLI(cmd.Cmd):
    prompt = "aexis> "
    intro = (
        "\n"
        "╔════════════════════════════════════════════════════════════╗\n"
        "║  AEXIS Command-Line Interface                             ║\n"
        "║  Autonomous Event-driven Transportation Intelligence       ║\n"
        "║  Type 'help' for available commands                       ║\n"
        "╚════════════════════════════════════════════════════════════╝\n"
    )

    def __init__(self, client: APIClient):
        super().__init__()
        self.client = client
        self._setup_logging()

    def _setup_logging(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def _check_connection(self) -> bool:
        if not self.client.check_health():
            self._error("Cannot connect to AEXIS API. Is the system running?")
            return False
        return True

    def _error(self, message: str) -> None:
        print(f"✗ Error: {message}")

    def _success(self, message: str) -> None:
        print(f"✓ {message}")

    def _info(self, message: str) -> None:
        print(f"ℹ {message}")

    def do_status(self, args: str) -> None:
        if not self._check_connection():
            return

        try:
            state = self.client.get_system_state()

            uptime = int(state.get("uptime_seconds", 0))
            uptime_fmt = f"{uptime // 3600}h {(uptime % 3600) // 60}m {uptime % 60}s"

            print("\n" + "=" * 60)
            print("AEXIS System Status".center(60))
            print("=" * 60)

            system_info = [
                ["System ID", state.get("system_id", "Unknown")],
                ["Status", "🟢 RUNNING" if state.get("running") else "🔴 STOPPED"],
                ["Uptime", uptime_fmt],
                ["Timestamp", state.get("timestamp", "")],
            ]
            print(tabulate(system_info, tablefmt="plain"))

            metrics = state.get("metrics", {})
            print("\nMetrics:")
            metrics_data = [
                [
                    "Active Pods",
                    f"{metrics.get('active_pods', 0)}/{metrics.get('total_pods', 0)}",
                ],
                [
                    "Operational Stations",
                    f"{metrics.get('operational_stations', 0)}/{metrics.get('total_stations', 0)}",
                ],
                ["Pending Passengers", metrics.get("pending_passengers", 0)],
                ["Pending Cargo", metrics.get("pending_cargo", 0)],
                ["System Efficiency", f"{metrics.get('system_efficiency', 0):.1%}"],
                ["Avg Wait Time", f"{metrics.get('average_wait_time', 0):.1f}s"],
                ["Throughput/Hour", f"{metrics.get('throughput_per_hour', 0):.0f}"],
                ["Fallback Rate", f"{metrics.get('fallback_usage_rate', 0):.1%}"],
            ]
            print(tabulate(metrics_data, tablefmt="simple"))
            print()

        except Exception as e:
            self._error(f"Failed to retrieve status: {str(e)}")

    def do_pods(self, args: str) -> None:
        if not self._check_connection():
            return

        try:
            if args.strip():

                pod_id = args.strip()
                pod_state = self.client.get_pod_state(pod_id)

                if not pod_state:
                    self._error(f"Pod '{pod_id}' not found")
                    return

                print(f"\nPod: {pod_id}")
                print(json.dumps(pod_state, indent=2))
            else:

                pods = self.client.get_all_pods()
                if not pods:
                    self._info("No pods in system")
                    return

                pods_data = []

                for pod_id in sorted(pods.keys()):
                    state = pods[pod_id]
                    pods_data.append(
                        [
                            pod_id,
                            state.get("status", "unknown"),
                            state.get("current_spine", "N/A"),
                            f"{state.get('distance', 0):.1f}",
                            state.get("load_type", "empty"),
                        ]
                    )

                print("\nPods:")
                print(
                    tabulate(
                        pods_data,
                        headers=[
                            "Pod ID",
                            "Status",
                            "Current Spine",
                            "Distance",
                            "Load",
                        ],
                        tablefmt="simple",
                    )
                )
                print()

        except Exception as e:
            self._error(f"Failed to retrieve pod information: {str(e)}")

    def do_stations(self, args: str) -> None:
        if not self._check_connection():
            return

        try:
            if args.strip():

                station_id = args.strip()
                station_state = self.client.get_station_state(station_id)

                if not station_state:
                    self._error(f"Station '{station_id}' not found")
                    return

                print(f"\nStation: {station_id}")
                print(json.dumps(station_state, indent=2))
            else:

                stations = self.client.get_all_stations()
                if not stations:
                    self._info("No stations in system")
                    return

                stations_data = []
                for station_id in sorted(stations.keys()):
                    state = stations[station_id]
                    stations_data.append(
                        [
                            station_id,
                            state.get("status", "unknown"),
                            len(state.get("passenger_queue", [])),
                            len(state.get("cargo_queue", [])),
                            state.get("avg_wait_time", 0),
                        ]
                    )

                print("\nStations:")
                print(
                    tabulate(
                        stations_data,
                        headers=[
                            "Station ID",
                            "Status",
                            "Passengers",
                            "Cargo",
                            "Avg Wait (s)",
                        ],
                        tablefmt="simple",
                    )
                )
                print()

        except Exception as e:
            self._error(f"Failed to retrieve station information: {str(e)}")

    def do_inject_passenger(self, args: str) -> None:
        if not self._check_connection():
            return

        parts = args.strip().split()
        if len(parts) < 2:
            self._error("Usage: inject_passenger <origin> <destination> [count]")
            return

        origin = parts[0]
        destination = parts[1]
        count = int(parts[2]) if len(parts) > 2 else 1

        try:
            self.client.inject_passenger(origin, destination, count)
            self._success(
                f"Injected {count} passenger(s) from {origin} to {destination}"
            )
        except Exception as e:
            self._error(f"Failed to inject passenger request: {str(e)}")

    def do_inject_cargo(self, args: str) -> None:
        if not self._check_connection():
            return

        parts = args.strip().split()
        if len(parts) < 2:
            self._error("Usage: inject_cargo <origin> <destination> [weight]")
            return

        origin = parts[0]
        destination = parts[1]
        weight = float(parts[2]) if len(parts) > 2 else 100.0

        try:
            self.client.inject_cargo(origin, destination, weight)
            self._success(f"Injected cargo ({weight}kg) from {origin} to {destination}")
        except Exception as e:
            self._error(f"Failed to inject cargo request: {str(e)}")

    def do_watch(self, args: str) -> None:
        if not self._check_connection():
            return

        interval = 1.0
        if args.strip():
            try:
                interval = float(args.strip())
            except ValueError:
                self._error("Interval must be a number")
                return

        import time
        from tabulate import tabulate

        try:
            while True:

                state = self.client.get_system_state()
                pods = self.client.get_all_pods()
                stations = self.client.get_all_stations()

                os.system("clear" if os.name == "posix" else "cls")

                uptime = int(state.get("uptime_seconds", 0))
                uptime_fmt = f"{uptime // 3600}h {(uptime % 3600) // 60}m {uptime % 60}s"
                metrics = state.get("metrics", {})

                print("=" * 60)
                print("AEXIS LIVE DASHBOARD".center(60))
                print("=" * 60)
                sys_data = [
                    ["Status", "🟢 RUNNING" if state.get("running") else "🔴 STOPPED"],
                    ["Uptime", uptime_fmt],
                    ["Pods", f"{metrics.get('active_pods', 0)}/{metrics.get('total_pods', 0)} active"],
                    ["Stations", f"{metrics.get('operational_stations', 0)} operational"],
                    ["Pending Pax", metrics.get("pending_passengers", 0)],
                    ["Pending Cargo", metrics.get("pending_cargo", 0)],
                ]
                print(tabulate(sys_data, tablefmt="plain"))
                print()

                if pods:
                    pods_data = []
                    for pid in sorted(pods.keys())[:15]:
                        pstate = pods[pid]
                        pods_data.append([
                            pid,
                            pstate.get("status", "unknown").upper(),
                            pstate.get("current_spine", "N/A"),
                            f"{pstate.get('speed', 0):.1f} m/s",
                            pstate.get("load_type", "empty")
                        ])
                    print("Active Pods (Top 15):")
                    print(tabulate(pods_data, headers=["ID", "Status", "Spine", "Speed", "Load"], tablefmt="simple"))
                    print()

                if stations:
                    st_data = []
                    for sid in sorted(stations.keys())[:10]:
                        sstate = stations[sid]
                        st_data.append([
                            sid,
                            sstate.get("status", "unknown").upper(),
                            len(sstate.get("queues", {}).get("passengers", {}).get("waiting", 0) if isinstance(sstate.get("queues"), dict) else sstate.get("passenger_queue", [])),
                            len(sstate.get("queues", {}).get("cargo", {}).get("waiting", 0) if isinstance(sstate.get("queues"), dict) else sstate.get("cargo_queue", [])),
                        ])
                    print("Stations (Top 10):")
                    print(tabulate(st_data, headers=["ID", "Status", "Pax Wait", "Cargo Wait"], tablefmt="simple"))
                    print()

                print(f"Updating every {interval}s... Press Ctrl+C to exit.")
                time.sleep(interval)

        except KeyboardInterrupt:

            print("\nExited watch mode.")
            return
        except Exception as e:
            self._error(f"Watch failed: {e}")

    def do_help(self, args: str) -> None:
        if not args:
            print("\nAvailable Commands:")
            print("  System Management:")
            print("    status              - Display system status and metrics")
            print("    pods [pod_id]       - List pods or show pod details")
            print("    stations [stn_id]   - List stations or show station details")
            print("    ")
            print("  Load Injection:")
            print("    inject_passenger    - Inject passenger request")
            print("    inject_cargo        - Inject cargo request")
            print("    ")
            print("  Navigation:")
            print("    help [command]      - Show this help or command-specific help")
            print("    clear               - Clear screen")
            print("    quit                - Exit CLI")
            print()
        else:
            super().do_help(args)

    def do_clear(self, args: str) -> None:
        os.system("clear" if os.name == "posix" else "cls")

    def do_quit(self, args: str) -> bool:
        print("Exiting AEXIS CLI. Goodbye!")
        return True

    def do_EOF(self, args: str) -> bool:
        print()
        return self.do_quit(args)

    def emptyline(self) -> bool:
        return False

    def default(self, line: str) -> None:
        self._error(f"Unknown command: '{line.split()[0] if line else ''}'.")

def main():
    try:

        api_host = os.getenv("API_HOST", "localhost")
        api_port = os.getenv("API_PORT", "8001")
        api_url = f"http://{api_host}:{api_port}"

        client = APIClient(base_url=api_url)

        cli = AexisCLI(client)

        if not client.check_health():
            print(f"Warning: Could not connect to API at {api_url}")
            print("Ensure the AEXIS system services are running.")
        else:
            print(f"Connected to AEXIS API at {api_url}")

        cli.cmdloop()

    except KeyboardInterrupt:
        print("\n")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
