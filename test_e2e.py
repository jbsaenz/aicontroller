import requests
import pandas as pd
import subprocess
import json
from datetime import datetime, timedelta, timezone

API_BASE = "http://localhost:8080"
CSV_PATH = "/tmp/test_telemetry.csv"

def print_step(msg):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] \033[1;36m{msg}\033[0m")

def main():
    print_step("1. Testing Authentication")
    res = requests.post(f"{API_BASE}/api/auth/login", json={"username": "admin", "password": "password12345"})
    assert res.status_code == 200, f"Login failed: {res.text}"
    token = res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    print("✅ Authenticated")

    print_step("2. Generating test telemetry (healthy -> trailing into critical failure)")
    now = datetime.now(timezone.utc)
    test_miner_id = f"TEST-MINER-{int(now.timestamp())}"
    data = []
    
    # Healthy telemetry for last 5 hours
    for i in range(5, 1, -1):
        ts = now - timedelta(hours=i)
        data.append({
            "timestamp": ts.isoformat(),
            "miner_id": test_miner_id,
            "asic_clock_mhz": 400.0,
            "asic_voltage_v": 12.5,
            "asic_hashrate_ths": 105.0,
            "asic_temperature_c": 55.0,
            "asic_power_w": 3000.0,
            "operating_mode": "normal",
            "ambient_temperature_c": 25.0
        })

    # Critical failure data points (right now)
    data.append({
        "timestamp": now.isoformat(),
        "miner_id": test_miner_id,
        "asic_clock_mhz": 400.0,
        "asic_voltage_v": 14.0,       # High voltage
        "asic_hashrate_ths": 40.0,    # Hashrate dropped completely
        "asic_temperature_c": 130.0,   # Temperature off the charts
        "asic_power_w": 3800.0,       # High power draw
        "operating_mode": "normal",
        "ambient_temperature_c": 40.0 # Hot ambient
    })

    df = pd.DataFrame(data)
    df.to_csv(CSV_PATH, index=False)
    print("✅ CSV payload generated")

    print_step("3. Uploading synthetic payload to ingestion endpoint")
    with open(CSV_PATH, "rb") as f:
        res = requests.post(f"{API_BASE}/api/ingest/csv", files={"file": f}, headers=headers)
    assert res.status_code == 200, f"Upload failed: {res.text}"
    print(f"✅ Data ingested: {res.json()}")

    print_step("4. Forcing Worker Jobs (KPI, Inference, Notify)")
    # Since background workers run on APScheduler, we will just manually execute them inside the container
    script = (
        "import logging; logging.basicConfig(level=logging.INFO); "
        "import os; "
        "from sqlalchemy import create_engine; "
        "from worker.ml_jobs import run_kpi_job, run_inference_job; "
        "from worker.notifier import run_notify_job; "
        "engine = create_engine(os.getenv('DATABASE_URL_SYNC')); "
        "print('--- Running KPI ---'); run_kpi_job(engine); "
        "print('--- Running Inference ---'); run_inference_job(engine); "
        "print('--- Running Notify ---'); run_notify_job(engine);"
    )
    cmd = ["docker", "exec", "aicontroller-worker-1", "python", "-c", script]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(f"Worker logs:\n{result.stderr}")
    assert result.returncode == 0, (
        "Worker jobs failed. "
        f"exit_code={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    print("✅ Worker jobs executed")

    print_step("5. Verifying DB Alerts Generation")
    res = requests.get(f"{API_BASE}/api/alerts/history", params={"limit": 1000}, headers=headers)
    assert res.status_code == 200, f"Fetching alerts failed: {res.text}"
    alerts = res.json()
    
    test_alerts = [a for a in alerts if a["miner_id"] == test_miner_id]
    if test_alerts:
        for a in test_alerts:
            print(f"🚨 ALERT FOUND:")
            print(json.dumps(a, indent=2))
        print("\n🎉 End-to-end telemetry and alert pipeline verification successful.")
    else:
        risk_res = requests.get(
            f"{API_BASE}/api/miners/{test_miner_id}/risk",
            params={"hours": 2},
            headers=headers,
        )
        assert risk_res.status_code == 200, f"Fetching miner risk failed: {risk_res.text}"
        risk_rows = risk_res.json()
        if not risk_rows:
            raise AssertionError(
                f"No alerts or risk predictions found for {test_miner_id}. "
                "Inference pipeline may have failed."
            )
        print("ℹ️ No alert generated at current threshold; latest risk prediction:")
        print(json.dumps(risk_rows[-1], indent=2))
        print("\n✅ Inference path verified (risk prediction generated).")

if __name__ == '__main__':
    main()
