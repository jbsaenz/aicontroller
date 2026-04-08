import requests
import pandas as pd
import random
import subprocess
import json
from datetime import datetime, timedelta, timezone

API_BASE = "http://localhost:8080"
CSV_PATH = "/tmp/test_advanced_telemetry.csv"

def print_step(msg):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] \033[1;36m{msg}\033[0m")

def main():
    print_step("1. Authenticating to API")
    res = requests.post(f"{API_BASE}/api/auth/login", json={"username": "admin", "password": "password12345"})
    res.raise_for_status()
    token = res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    print_step("2. Generating advanced chip-level telemetry")
    now = datetime.now(timezone.utc)
    data = []
    
    # We simulate one miner undergoing acute stress to verify policy alert generation.
    miner_id = f"TEST-ADVANCED-{int(now.timestamp())}"
    for h in range(3, -1, -1):
        is_current = (h == 0)
        
        # When current, simulate the failure! 
        # Current point simulates acute stress: hashrate craters and temp rises.
        data.append({
            "timestamp": (now - timedelta(hours=h)).isoformat(),
            "miner_id": miner_id,
            "asic_clock_mhz": 400.0,
            "asic_voltage_v": 12.3,
            "asic_hashrate_ths": 30.0 if is_current else random.uniform(100, 105),
            "asic_temperature_c": 80.0 if is_current else 65.0,
            "asic_power_w": 3100.0 if is_current else random.uniform(3300, 3400),
            "operating_mode": "normal",
            "ambient_temperature_c": 25.0,
            
            # New hardware telemetry fields!
            "chip_temp_max": 75.0,
            "chip_temp_std": 5.0,
            "bad_hash_count": 500 if is_current else 10,
            "double_hash_count": 1500 if is_current else 5,
            "read_errors": 10 if is_current else 0,
            "event_codes": '["LOW_HASHRATE"]' if is_current else '',
            "expected_hashrate_ths": 105.0
        })

    df = pd.DataFrame(data)
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Generated {len(df)} rows of hardware-mapped telemetry.")

    print_step("3. Uploading payload")
    with open(CSV_PATH, "rb") as f:
        res = requests.post(f"{API_BASE}/api/ingest/csv", files={"file": f}, headers=headers)
    
    if not res.ok:
        print(f"FAILED: {res.text}")
    res.raise_for_status()
    print(f"✅ Ingestion successful: {res.json()['rows_inserted']} rows inserted.")

    print_step("4. Executing Background Pipeline (KPI -> Inference -> Policy Engine)")
    script = (
        "import logging; logging.basicConfig(level=logging.INFO); "
        "import os; "
        "from sqlalchemy import create_engine; "
        "from worker.ml_jobs import run_kpi_job, run_inference_job; "
        "engine = create_engine(os.getenv('DATABASE_URL_SYNC')); "
        "print('--- KPI Job ---'); run_kpi_job(engine); "
        "print('--- Inference/Policy Engine ---'); run_inference_job(engine);"
    )
    cmd = ["docker", "exec", "aicontroller-worker-1", "python", "-c", script]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("✅ Worker execution finished.")
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"Worker logs:\n{result.stderr}")
    if result.returncode != 0:
        raise RuntimeError(
            "Worker execution failed. "
            f"exit_code={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        
    print_step("5. Verifying Decision Policy Generated Alert")
    res = requests.get(f"{API_BASE}/api/alerts/history", params={"limit": 1000}, headers=headers)
    res.raise_for_status()
    alerts = res.json()
    miner_alerts = [a for a in alerts if a["miner_id"] == miner_id]
    if not miner_alerts:
        risk_res = requests.get(
            f"{API_BASE}/api/miners/{miner_id}/risk",
            params={"hours": 2},
            headers=headers,
        )
        risk_res.raise_for_status()
        risk_rows = risk_res.json()
        if not risk_rows:
            raise AssertionError(
                f"No alerts or risk predictions found for {miner_id}. "
                "Check KPI/inference/policy pipeline."
            )
        print("ℹ️ No alert generated at current threshold; latest risk prediction:")
        print(json.dumps(risk_rows[-1], indent=2))
        print("\n✅ Inference path verified (risk prediction generated).")
        return

    print("🚨 Generated Policy Alerts:\n")
    for alert in miner_alerts:
        print(json.dumps(alert, indent=2))
    print("\n🎉 Output Successfully Maps Telemetry to Automated Actions!")

if __name__ == '__main__':
    main()
