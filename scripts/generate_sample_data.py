#!/usr/bin/env python3
"""
Generate realistic synthetic telemetry for ASIC miner fleet.
Produces a CSV ready for upload to AI Controller.

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --miners 200 --days 14 --output data/sample.csv
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# Default output anchored to repository root (not caller CWD).
DEFAULT_OUTPUT_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "sample_telemetry.csv"
)

# ── Antminer S19/S21 realistic specs ──────────────────────────────────────
MODE_PARAMS = {
    "eco":    {"clock": 480, "voltage": 11.9, "hash_mult": 0.88, "power_offset": -280},
    "normal": {"clock": 545, "voltage": 12.4, "hash_mult": 1.00, "power_offset":    0},
    "turbo":  {"clock": 625, "voltage": 13.1, "hash_mult": 1.12, "power_offset":  320},
}
MODE_PROBS = [0.25, 0.55, 0.20]   # eco, normal, turbo
MODE_TRANSITIONS = {
    "eco":    [0.92, 0.07, 0.01],
    "normal": [0.04, 0.91, 0.05],
    "turbo":  [0.02, 0.10, 0.88],
}
MODES = list(MODE_PARAMS.keys())


def _ambient(timestamps: pd.DatetimeIndex, rng: np.random.Generator) -> np.ndarray:
    hour = timestamps.hour + timestamps.minute / 60.0
    daily = 26.0 + 6.0 * np.sin(2 * np.pi * (hour - 14) / 24)
    noise = rng.normal(0, 1.2, len(timestamps))
    return np.clip(daily + noise, 12.0, 42.0)


def _modes(n: int, rng: np.random.Generator) -> list:
    seq = [rng.choice(MODES, p=MODE_PROBS)]
    for _ in range(n - 1):
        seq.append(rng.choice(MODES, p=MODE_TRANSITIONS[seq[-1]]))
    return seq


def _simulate_miner(miner_id: str, timestamps: pd.DatetimeIndex,
                    ambient: np.ndarray, rng: np.random.Generator,
                    fault_profile: str = "healthy") -> pd.DataFrame:
    """
    fault_profile options:
      healthy       — normal wear
      degraded      — elevated temps, lower hashrate
      unstable      — high power instability spikes
      critical      — near-failure: very high temp, low hashrate
    """
    n = len(timestamps)
    modes = _modes(n, rng)

    # Per-miner hardware variance
    eff_factor = rng.normal(1.0, 0.04)
    cooling_eff = np.clip(rng.normal(1.0, 0.07), 0.80, 1.25)

    # Degradation seed based on profile
    degradation = {
        "healthy": max(0, rng.normal(0.15, 0.05)),
        "degraded": max(0, rng.normal(1.8, 0.3)),
        "unstable": max(0, rng.normal(0.5, 0.15)),
        "critical": max(0, rng.normal(4.5, 0.5)),
    }[fault_profile]

    base_instability = {
        "healthy": np.clip(rng.beta(1.5, 10), 0.01, 0.15),
        "degraded": np.clip(rng.beta(2, 6), 0.05, 0.30),
        "unstable": np.clip(rng.beta(3, 5), 0.10, 0.60),
        "critical": np.clip(rng.beta(5, 3), 0.25, 0.80),
    }[fault_profile]

    rows = []
    for i in range(n):
        m = modes[i]
        p = MODE_PARAMS[m]
        amb = float(ambient[i])

        # Power instability
        spike = rng.uniform(0.2, 0.5) if rng.random() < 0.02 else 0.0
        instability = float(np.clip(base_instability + spike + 0.015 * degradation + rng.normal(0, 0.025), 0.0, 1.0))

        # ASIC clock
        clock = float(np.clip(p["clock"] * eff_factor - 2.0 * degradation + rng.normal(0, 5), 300, 760))

        # Voltage
        voltage = float(np.clip(p["voltage"] + 0.002 * (clock - p["clock"]) + 0.012 * degradation + rng.normal(0, 0.05), 10.5, 14.5))

        # Power
        power = float(np.clip(
            2800 + 2.5 * clock + 180 * (voltage - 11.5) + 5 * (amb - 25) + 120 * instability + p["power_offset"] + rng.normal(0, 60),
            900, 4800
        ))

        # Cooling power
        cooling = float(np.clip((140 + 0.12 * power + 15 * (amb - 20) + 50 * max(instability - 0.4, 0) + rng.normal(0, 25)) / cooling_eff, 80, 1800))

        # Temperature
        temp = float(np.clip(
            amb + 14 + 0.009 * power + 9 * instability - 0.018 * cooling + 0.8 * degradation + rng.normal(0, 1.5),
            32, 120
        ))

        # Hashrate (TH/s — S19 ≈ 95 TH/s, S21 ≈ 200 TH/s)
        expected_hash = 0.17 * clock * p["hash_mult"] * eff_factor
        temp_loss = max(0, temp - 80.0) * 0.9
        degrade_loss = 2.2 * degradation
        hash_rate = float(np.clip(expected_hash * (1 - (temp_loss + degrade_loss) / 100) + rng.normal(0, 1.2), 10, 250))

        # Evolve degradation
        temp_stress = max(0, (temp - 85) / 10)
        voltage_stress = max(0, (voltage - 12.9) / 0.5)
        hash_stress = max(0, (expected_hash - hash_rate) / max(expected_hash, 1))
        stress = 0.9 * temp_stress + 1.1 * instability + 0.8 * voltage_stress + hash_stress
        degradation = float(np.clip(0.93 * degradation + 0.3 * stress + rng.normal(0, 0.05), 0, 10))

        rows.append({
            "timestamp": timestamps[i].isoformat(),
            "miner_id": miner_id,
            "asic_clock_mhz": round(clock, 1),
            "asic_voltage_v": round(voltage, 3),
            "asic_hashrate_ths": round(hash_rate, 2),
            "asic_temperature_c": round(temp, 1),
            "asic_power_w": round(power, 1),
            "operating_mode": m,
            "ambient_temperature_c": round(amb, 1),
        })

    return pd.DataFrame(rows)


def generate(n_miners: int = 50, days: int = 7, freq_minutes: int = 15,
             seed: int = 42, output: str | Path = DEFAULT_OUTPUT_PATH) -> str:
    rng = np.random.default_rng(seed)
    end_ts = pd.Timestamp.utcnow().floor('15min')
    timestamps = pd.date_range(
        end=end_ts, periods=int(24 * 60 / freq_minutes) * days,
        freq=f"{freq_minutes}min"
    )
    ambient = _ambient(timestamps, rng)

    # Assign fault profiles — realistic fleet distribution
    profiles = (
        ["healthy"] * int(n_miners * 0.65) +
        ["degraded"] * int(n_miners * 0.20) +
        ["unstable"] * int(n_miners * 0.10) +
        ["critical"] * max(1, int(n_miners * 0.05))
    )
    # Pad to n_miners
    while len(profiles) < n_miners:
        profiles.append("healthy")
    profiles = profiles[:n_miners]
    rng.shuffle(profiles)

    frames = []
    for i, profile in enumerate(profiles):
        mid = f"miner_{i+1:04d}"
        df = _simulate_miner(mid, timestamps, ambient, rng, fault_profile=profile)
        frames.append(df)
        if (i + 1) % 50 == 0:
            print(f"  Generated {i+1}/{n_miners} miners...")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["miner_id", "timestamp"]).reset_index(drop=True)

    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out, index=False)

    # Print summary
    print(f"\n✅ Generated {len(combined):,} rows across {n_miners} miners")
    print(f"   Timespan : {timestamps[0]} → {timestamps[-1]}")
    print(f"   Profiles : healthy={profiles.count('healthy')} | degraded={profiles.count('degraded')} | unstable={profiles.count('unstable')} | critical={profiles.count('critical')}")
    print(f"   Avg hash : {combined['asic_hashrate_ths'].mean():.1f} TH/s")
    print(f"   Avg temp : {combined['asic_temperature_c'].mean():.1f}°C")
    print(f"   Avg power: {combined['asic_power_w'].mean():.0f} W")
    print(f"   Output   : {out.resolve()}")
    return str(out.resolve())


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic ASIC miner telemetry CSV")
    parser.add_argument("--miners",  type=int, default=50,    help="Number of miners (default: 50)")
    parser.add_argument("--days",    type=int, default=7,     help="Days of history (default: 7)")
    parser.add_argument("--freq",    type=int, default=15,    help="Interval in minutes (default: 15)")
    parser.add_argument("--seed",    type=int, default=42,    help="Random seed (default: 42)")
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output CSV path",
    )
    args = parser.parse_args()

    print(f"Generating {args.miners} miners × {args.days} days @ {args.freq}min intervals...")
    generate(args.miners, args.days, args.freq, args.seed, args.output)


if __name__ == "__main__":
    main()
