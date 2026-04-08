import requests
import pandas as pd
import random
import subprocess
import time
from datetime import datetime, timedelta, timezone

API_BASE = "http://localhost:8080"
CSV_PATH = "/tmp/large_fleet.csv"

def print_step(msg):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] \033[1;36m{msg}\033[0m")

def main():
    print_step("1. Authenticating to API")
    res = requests.post(f"{API_BASE}/api/auth/login", json={"username": "admin", "password": "password12345"})
    res.raise_for_status()
    token = res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    print_step("2. Generating 1,000 miners telemetry data")
    now = datetime.now(timezone.utc)
    data = []
    
    def add_miner(prefix, count, temp_range, hash_range, volt_range, power_range):
        for i in range(count):
            miner_id = f"{prefix}-{i:04d}"
            # Add 3 historical points + 1 current point to build rolling features
            for h in range(3, -1, -1):
                data.append({
                    "timestamp": (now - timedelta(hours=h)).isoformat(),
                    "miner_id": miner_id,
                    "asic_clock_mhz": random.uniform(380, 420),
                    "asic_voltage_v": random.uniform(*volt_range),
                    "asic_hashrate_ths": random.uniform(*hash_range),
                    "asic_temperature_c": random.uniform(*temp_range),
                    "asic_power_w": random.uniform(*power_range),
                    "operating_mode": "normal",
                    "ambient_temperature_c": random.uniform(20, 35)
                })

    # 1. Healthy: 850 miners
    add_miner("HLTHY", 850, (60, 72), (95, 115), (12.0, 12.5), (2800, 3100))
    # 2. Medium Risk: 100 miners
    add_miner("MED", 100, (75, 82), (85, 95), (12.5, 13.0), (3100, 3300))
    # 3. High Risk: 30 miners
    add_miner("HIGH", 30, (85, 95), (70, 80), (13.0, 13.5), (3300, 3500))
    # 4. Critical: 20 miners
    add_miner("CRIT", 20, (100, 115), (20, 45), (13.8, 14.5), (3600, 3900))

    df = pd.DataFrame(data)
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Generated {len(df)} rows of data for {df['miner_id'].nunique()} miners.")

    print_step("3. Uploading payload (this might take a few seconds)")
    with open(CSV_PATH, "rb") as f:
        res = requests.post(f"{API_BASE}/api/ingest/csv", files={"file": f}, headers=headers)
    res.raise_for_status()
    print(f"✅ Ingestion successful: {res.json()['rows_inserted']} rows inserted.")

    print_step("4. Executing Background Pipeline (KPI -> Inference -> Alerts)")
    script = (
        "import logging; logging.basicConfig(level=logging.INFO); "
        "import os; "
        "from sqlalchemy import create_engine; "
        "from worker.ml_jobs import run_kpi_job, run_inference_job; "
        "from worker.notifier import run_notify_job; "
        "engine = create_engine(os.getenv('DATABASE_URL_SYNC')); "
        "print('--- KPI Job ---'); run_kpi_job(engine); "
        "print('--- Inference Job ---'); run_inference_job(engine); "
        "print('--- Notify Job ---'); run_notify_job(engine);"
    )
    cmd = ["docker", "exec", "aicontroller-worker-1", "python", "-c", script]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print("✅ Worker execution finished.")
    if result.stderr:
        print(f"Log Output:\n{result.stderr}")
    
    print_step("5. Checking DB for fleet status generation")
    res = requests.get(f"{API_BASE}/api/fleet/summary", headers=headers)
    summary = res.json()
    print(f"📊 Dashboard Fleet Summary:\n"
          f"Total: {summary['total_miners']} | "
          f"Healthy: {summary['healthy_count']} | "
          f"High Risk: {summary['high_risk_count']} | "
          f"Critical: {summary['critical_count']}")
    
    print("\n🎉 Simulated environment loaded! Go check out the UI dashboard at http://localhost:8080")

if __name__ == '__main__':
    main()
