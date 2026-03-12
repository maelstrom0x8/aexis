import argparse
import asyncio
import random
import sys
import time
from datetime import datetime
import httpx

class PayloadInjector:
    """Simulates realistic network demand by injecting random requests into AEXIS."""

    def __init__(self, host: str, interval: float, passenger_ratio: float):
        self.base_url = f"http://{host.rstrip('/')}"
        self.interval = interval
        self.passenger_ratio = passenger_ratio
        self.stations = []
        self.client = httpx.AsyncClient(timeout=10.0)

    async def fetch_stations(self) -> bool:
        """Fetch available stations from the API."""
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching stations from {self.base_url}/api/stations...")
            response = await self.client.get(f"{self.base_url}/api/stations")
            response.raise_for_status()
            data = response.json()
            self.stations = list(data.keys())
            if not self.stations:
                print("Error: No stations found in the system.")
                return False
            print(f"Found {len(self.stations)} stations: {', '.join(self.stations)}")
            return True
        except Exception as e:
            print(f"Error fetching stations: {e}")
            return False

    async def inject_passenger(self):
        """Inject a random passenger arrival."""
        if len(self.stations) < 2:
            return

        origin, dest = random.sample(self.stations, 2)
        count = random.randint(1, 10)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚶 Injecting {count} passengers: {origin} -> {dest}")
        
        try:
            payload = {
                "origin": origin,
                "destination": dest,
                "count": count
            }
            response = await self.client.post(f"{self.base_url}/api/manual/passenger", json=payload)
            if response.status_code != 200:
                print(f"   Failed: {response.text}")
        except Exception as e:
            print(f"   Error: {e}")

    async def inject_cargo(self):
        """Inject a random cargo request."""
        if len(self.stations) < 2:
            return

        origin, dest = random.sample(self.stations, 2)
        weight = float(random.randint(10, 500))
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📦 Injecting cargo ({weight}kg): {origin} -> {dest}")
        
        try:
            payload = {
                "origin": origin,
                "destination": dest,
                "weight": weight
            }
            response = await self.client.post(f"{self.base_url}/api/manual/cargo", json=payload)
            if response.status_code != 200:
                print(f"   Failed: {response.text}")
        except Exception as e:
            print(f"   Error: {e}")

    async def run(self):
        """Main injection loop."""
        if not await self.fetch_stations():
            print("Initialization failed. Please ensure the AEXIS server is running.")
            return

        print(f"\n--- Starting Payload Injection (Interval: ~{self.interval}s, Ratio: {self.passenger_ratio}) ---")
        print("Press Ctrl+C to stop.\n")

        try:
            while True:
                # Stochastic decision
                if random.random() < self.passenger_ratio:
                    await self.inject_passenger()
                else:
                    await self.inject_cargo()

                # Randomized sleep around the mean interval
                sleep_time = random.uniform(self.interval * 0.5, self.interval * 1.5)
                await asyncio.sleep(sleep_time)
                
                # Periodically refresh stations (every ~5 minutes of simulated time)
                if random.random() < 0.05:
                    await self.fetch_stations()

        except asyncio.CancelledError:
            print("\nStopping injector...")
        finally:
            await self.client.aclose()

async def main():
    parser = argparse.ArgumentParser(description="Realistic Payload Injector for AEXIS")
    parser.add_argument("--host", default="localhost:8000", help="API host (default: localhost:8000)")
    parser.add_argument("--interval", type=float, default=5.0, help="Average interval between injections in seconds (default: 5.0)")
    parser.add_argument("--ratio", type=float, default=0.7, help="Ratio of passengers vs cargo (default: 0.7)")
    
    args = parser.parse_args()
    
    injector = PayloadInjector(args.host, args.interval, args.ratio)
    try:
        await injector.run()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    asyncio.run(main())
