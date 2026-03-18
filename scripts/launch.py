#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("launcher")

class AexisLauncher:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.absolute()
        self.aexis_root = self.project_root / "aexis"
        self.processes: List[subprocess.Popen] = []
        self.shutting_down = False

        print(f"Loading configuration from environment...")
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis_password = os.getenv("REDIS_PASSWORD", "password")
        self.network_path = os.getenv("AEXIS_NETWORK_DATA", str(self.aexis_root / "network.json"))
        self.pod_count = int(os.getenv("POD_COUNT"))
        self.cargo_ratio = int(os.getenv("CARGO_RATIO", "50"))
        self.api_port = int(os.getenv("API_PORT", "8001"))
        self.web_port = int(os.getenv("WEB_PORT", "8000"))

    def _is_port_available(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) != 0

    def _check_redis(self) -> bool:
        try:
            import redis
            from urllib.parse import urlparse
            url = urlparse(self.redis_url)
            r = redis.Redis(
                host=url.hostname or 'localhost',
                port=url.port or 6379,
                password=self.redis_password,
                socket_connect_timeout=2
            )

            r.flushall()
            return r.ping()
        except ImportError:
            logger.warning("redis-py not installed in launcher environment, skipping ping.")
            return True
        except Exception as e:
            logger.error(f"Redis connectivity check failed: {e}")
            return False

    def _get_python_cmd(self) -> str:
        venv_python = self.aexis_root / ".venv" / "bin" / "python"
        if venv_python.exists():
            return str(venv_python)
        return sys.executable

    def _get_station_ids(self) -> List[str]:
        if not Path(self.network_path).exists():
            logger.error(f"Network file not found: {self.network_path}")
            return []

        with open(self.network_path) as f:
            data = json.load(f)

        station_ids = []
        for node in data.get("nodes", []):
            station_ids.append(str(node.get("id")))
        return station_ids

    def launch_process(self, module: str, args: List[str], name: str):
        python_cmd = self._get_python_cmd()
        cmd = [python_cmd, "-m", module] + args

        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.project_root)
        env["REDIS_URL"] = self.redis_url
        env["REDIS_PASSWORD"] = self.redis_password
        env["AEXIS_NETWORK_DATA"] = self.network_path

        try:

            proc = subprocess.Popen(
                cmd,
                cwd=str(self.aexis_root),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT
            )
            self.processes.append(proc)
            logger.debug(f"Launched {name} (PID: {proc.pid})")
        except Exception as e:
            logger.error(f"Failed to launch {name}: {e}")

    def shutdown(self):
        if self.shutting_down:
            return
        self.shutting_down = True
        logger.info("\nShutting down all services...")

        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()

        start_wait = time.time()
        while time.time() - start_wait < 5:
            if all(proc.poll() is not None for proc in self.processes):
                break
            time.sleep(0.1)

        for proc in self.processes:
            if proc.poll() is None:
                logger.warning(f"Process {proc.pid} didn't stop in time, killing...")
                proc.kill()

        logger.info("All services stopped.")

    def run(self):

        if not self._is_port_available(self.api_port):
            logger.error(f"Port {self.api_port} (API) is already in use.")
            return
        if not self._is_port_available(self.web_port):
            logger.error(f"Port {self.web_port} (Web) is already in use.")
            return
        if not self._check_redis():
            logger.error("Redis check failed. Ensure Redis is running and password is correct.")
            return

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: self.shutdown())

        station_ids = self._get_station_ids()
        logger.info(f"Starting {len(station_ids)} stations...")
        for i, sid in enumerate(station_ids):
            args = [
                "--station-id", sid,
                "--redis-url", self.redis_url,
                "--redis-password", self.redis_password,
                "--network-path", self.network_path
            ]
            self.launch_process("aexis.station.main", args, f"Station {sid}")

        logger.info("Waiting for stations to initialize...")
        time.sleep(3)

        cargo_count = (self.pod_count * self.cargo_ratio) // 100
        passenger_count = self.pod_count - cargo_count

        logger.info(f"Starting {passenger_count} passenger pods...")
        for i in range(1, passenger_count + 1):
            pod_id = str(i)
            args = [
                "--pod-id", pod_id,
                "--type", "passenger",
                "--redis-url", self.redis_url,
                "--redis-password", self.redis_password,
                "--network-path", self.network_path
            ]
            self.launch_process("aexis.pod.main", args, f"Pod {pod_id}")

        logger.info(f"Starting {cargo_count} cargo pods...")
        for i in range(1, cargo_count + 1):
            idx = passenger_count + i
            pod_id = str(idx)
            args = [
                "--pod-id", pod_id,
                "--type", "cargo",
                "--redis-url", self.redis_url,
                "--redis-password", self.redis_password,
                "--network-path", self.network_path
            ]
            self.launch_process("aexis.pod.main", args, f"Pod {pod_id}")

        time.sleep(2)

        logger.info(f"Starting API on port {self.api_port}...")
        self.launch_process("aexis.api.main", [], "API")

        logger.info(f"Starting Web Dashboard on port {self.web_port}...")
        self.launch_process("aexis.web.main", [], "Web Dashboard")

        logger.info("\n" + "="*43)
        logger.info("  AEXIS Microservices Running!")
        logger.info(f"  - API:       http://localhost:{self.api_port}")
        logger.info(f"  - Dashboard: http://localhost:{self.web_port}")
        logger.info("="*43 + "\n")
        logger.info("Press Ctrl+C to stop.")

        try:
            while not self.shutting_down:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.shutdown()

if __name__ == "__main__":
    launcher = AexisLauncher()
    launcher.run()
