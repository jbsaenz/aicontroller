"""Synthetic telemetry generator for mining predictive maintenance."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from config import (
    DATA_RAW_DIR,
    DataGenerationConfig,
    OPERATING_MODES,
    RAW_TELEMETRY_PATH,
)


MODE_PARAMS: Dict[str, Dict[str, float]] = {
    "eco": {
        "clock_base": 480.0,
        "voltage_base": 11.9,
        "mode_temp_offset": -3.5,
        "mode_power_offset": -120.0,
        "mode_hash_multiplier": 0.92,
    },
    "normal": {
        "clock_base": 545.0,
        "voltage_base": 12.4,
        "mode_temp_offset": 0.0,
        "mode_power_offset": 0.0,
        "mode_hash_multiplier": 1.0,
    },
    "turbo": {
        "clock_base": 625.0,
        "voltage_base": 13.0,
        "mode_temp_offset": 5.5,
        "mode_power_offset": 170.0,
        "mode_hash_multiplier": 1.11,
    },
}


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + np.exp(-x))


def _build_timestamps(cfg: DataGenerationConfig) -> pd.DatetimeIndex:
    intervals_per_day = int((24 * 60) / cfg.freq_minutes)
    total_steps = intervals_per_day * cfg.days
    return pd.date_range(
        start=cfg.start_timestamp, periods=total_steps, freq=f"{cfg.freq_minutes}min"
    )


def _ambient_temperature_profile(
    timestamps: pd.DatetimeIndex, rng: np.random.Generator
) -> np.ndarray:
    hour_of_day = timestamps.hour + timestamps.minute / 60.0
    daily = 26.0 + 5.8 * np.sin(2.0 * np.pi * (hour_of_day - 14.0) / 24.0)
    day_of_year = timestamps.dayofyear.to_numpy()
    weekly = 1.2 * np.sin(2.0 * np.pi * day_of_year / 7.0)
    noise = rng.normal(0.0, 1.1, size=len(timestamps))
    return np.clip(daily + weekly + noise, 12.0, 42.0)


def _simulate_miner_mode_series(
    n_steps: int, rng: np.random.Generator
) -> List[str]:
    transition = {
        "eco": {"eco": 0.94, "normal": 0.05, "turbo": 0.01},
        "normal": {"eco": 0.03, "normal": 0.93, "turbo": 0.04},
        "turbo": {"eco": 0.02, "normal": 0.08, "turbo": 0.90},
    }
    initial = rng.choice(OPERATING_MODES, p=[0.30, 0.55, 0.15])
    modes: List[str] = [str(initial)]
    for _ in range(n_steps - 1):
        prev = modes[-1]
        probs = [transition[prev][m] for m in OPERATING_MODES]
        modes.append(str(rng.choice(OPERATING_MODES, p=probs)))
    return modes


def _simulate_single_miner(
    miner_id: str,
    timestamps: pd.DatetimeIndex,
    ambient_temps: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    n_steps = len(timestamps)
    modes = _simulate_miner_mode_series(n_steps, rng)

    miner_efficiency_factor = rng.normal(1.0, 0.05)
    cooling_efficiency = np.clip(rng.normal(1.0, 0.08), 0.82, 1.25)
    base_power_instability = np.clip(rng.beta(2.2, 8.5), 0.02, 0.6)

    degradation_state = max(0.0, rng.normal(0.25, 0.08))

    rows = []
    for i in range(n_steps):
        mode = modes[i]
        p = MODE_PARAMS[mode]
        ambient = float(ambient_temps[i])

        instability_spike = rng.uniform(0.25, 0.5) if rng.random() < 0.018 else 0.0
        power_instability = np.clip(
            base_power_instability
            + instability_spike
            + 0.018 * degradation_state
            + rng.normal(0.0, 0.03),
            0.0,
            1.0,
        )

        asic_clock = (
            p["clock_base"] * miner_efficiency_factor
            - 2.2 * degradation_state
            + rng.normal(0.0, 6.0)
        )
        asic_clock = float(np.clip(asic_clock, 320.0, 760.0))

        asic_voltage = (
            p["voltage_base"]
            + 0.0018 * (asic_clock - p["clock_base"])
            + 0.015 * degradation_state
            + rng.normal(0.0, 0.055)
        )
        asic_voltage = float(np.clip(asic_voltage, 10.8, 14.4))

        asic_power = (
            260.0
            + 2.65 * asic_clock
            + 160.0 * (asic_voltage - 11.5)
            + 4.5 * (ambient - 25.0)
            + 130.0 * power_instability
            + p["mode_power_offset"]
            + rng.normal(0.0, 42.0)
        )
        asic_power = float(np.clip(asic_power, 850.0, 4300.0))

        cooling_power = (
            110.0
            + 0.14 * asic_power
            + 12.5 * (ambient - 20.0)
            + (40.0 if mode == "turbo" else -30.0 if mode == "eco" else 0.0)
            + 45.0 * max(power_instability - 0.5, 0.0)
            + rng.normal(0.0, 22.0)
        )
        cooling_power = float(np.clip(cooling_power / cooling_efficiency, 70.0, 1900.0))

        asic_temp = (
            ambient
            + 13.5
            + 0.011 * asic_power
            + 10.0 * power_instability
            - 0.019 * cooling_power
            + p["mode_temp_offset"]
            + 0.85 * degradation_state
            + rng.normal(0.0, 1.8)
        )
        asic_temp = float(np.clip(asic_temp, 34.0, 118.0))

        expected_hashrate = (
            0.185 * asic_clock * p["mode_hash_multiplier"] * miner_efficiency_factor
        )
        temp_loss_pct = max(0.0, asic_temp - 79.0) * 0.95
        instability_loss_pct = 8.0 * power_instability
        degradation_loss_pct = 2.4 * degradation_state
        total_loss_pct = temp_loss_pct + instability_loss_pct + degradation_loss_pct
        asic_hashrate = (
            expected_hashrate * (1.0 - total_loss_pct / 100.0) + rng.normal(0.0, 1.3)
        )
        asic_hashrate = float(np.clip(asic_hashrate, 12.0, 240.0))

        efficiency_j_per_th = float(asic_power / max(asic_hashrate, 1e-3))

        temp_stress = max(0.0, (asic_temp - 84.0) / 10.0)
        voltage_stress = max(0.0, (asic_voltage - 12.8) / 0.45)
        hash_drop_stress = max(0.0, (expected_hashrate - asic_hashrate) / expected_hashrate)
        mode_stress = 0.23 if mode == "turbo" else 0.0
        stress = (
            0.95 * temp_stress
            + 1.10 * power_instability
            + 0.80 * voltage_stress
            + 1.00 * hash_drop_stress
            + mode_stress
        )

        degradation_state = max(
            0.0,
            min(8.0, 0.92 * degradation_state + 0.36 * stress + rng.normal(0.0, 0.06)),
        )
        if rng.random() < 0.008:
            degradation_state *= 0.75

        failure_logit = (
            -10.3
            + 0.95 * degradation_state
            + 0.55 * temp_stress
            + 0.65 * power_instability
            + (0.40 if asic_temp > 95.0 else 0.0)
        )
        failure_probability = _sigmoid(failure_logit)
        failure_event = int(rng.random() < failure_probability)

        rows.append(
            {
                "timestamp": timestamps[i],
                "miner_id": miner_id,
                "operating_mode": mode,
                "ambient_temperature_c": ambient,
                "cooling_power_w": cooling_power,
                "asic_clock_mhz": asic_clock,
                "asic_voltage_v": asic_voltage,
                "asic_hashrate_ths": asic_hashrate,
                "asic_temperature_c": asic_temp,
                "asic_power_w": asic_power,
                "efficiency_j_per_th": efficiency_j_per_th,
                "power_instability_index": float(power_instability),
                "failure_event": failure_event,
            }
        )

    miner_df = pd.DataFrame(rows)
    baseline_hashrate = miner_df.groupby("operating_mode")["asic_hashrate_ths"].transform(
        "median"
    )
    miner_df["hashrate_deviation_pct"] = (
        (miner_df["asic_hashrate_ths"] - baseline_hashrate) / baseline_hashrate
    ) * 100.0
    return miner_df


def _apply_future_failure_label(
    df: pd.DataFrame, horizon_hours: int, freq_minutes: int
) -> pd.Series:
    horizon_steps = max(1, int((horizon_hours * 60) / freq_minutes))
    labels: List[pd.Series] = []

    for _, g in df.groupby("miner_id", sort=False):
        future_events = g["failure_event"].shift(-1, fill_value=0).to_numpy(dtype=float)
        future_max = (
            pd.Series(future_events[::-1])
            .rolling(window=horizon_steps, min_periods=1)
            .max()
            .to_numpy()[::-1]
        )
        labels.append(pd.Series(future_max.astype(int), index=g.index))

    return pd.concat(labels).sort_index().astype(int)


def _inject_quality_issues(
    df: pd.DataFrame, cfg: DataGenerationConfig, rng: np.random.Generator
) -> pd.DataFrame:
    if not cfg.inject_quality_issues:
        return df

    noisy = df.copy()
    n_rows = len(noisy)
    if n_rows == 0:
        return noisy

    missing_cols = [
        "ambient_temperature_c",
        "asic_hashrate_ths",
        "asic_temperature_c",
        "asic_power_w",
    ]
    missing_rows = max(1, int(n_rows * cfg.missing_rate))
    for col in missing_cols:
        idx = rng.choice(n_rows, size=missing_rows, replace=False)
        noisy.loc[idx, col] = np.nan

    invalid_mode_rows = max(1, int(n_rows * 0.001))
    bad_idx = rng.choice(n_rows, size=invalid_mode_rows, replace=False)
    noisy.loc[bad_idx, "operating_mode"] = "boost"

    duplicate_rows = int(n_rows * cfg.duplicate_rate)
    if duplicate_rows > 0:
        dupes = noisy.sample(n=duplicate_rows, random_state=cfg.seed)
        noisy = pd.concat([noisy, dupes], ignore_index=True)

    return noisy.sample(frac=1.0, random_state=cfg.seed).reset_index(drop=True)


def generate_synthetic_telemetry(cfg: DataGenerationConfig) -> pd.DataFrame:
    """Generate synthetic miner telemetry and predictive-maintenance labels."""

    rng = np.random.default_rng(cfg.seed)
    timestamps = _build_timestamps(cfg)
    ambient_profile = _ambient_temperature_profile(timestamps, rng)

    miner_frames = []
    for idx in range(cfg.n_miners):
        miner_id = f"miner_{idx + 1:03d}"
        miner_df = _simulate_single_miner(
            miner_id=miner_id,
            timestamps=timestamps,
            ambient_temps=ambient_profile,
            rng=rng,
        )
        miner_frames.append(miner_df)

    df = pd.concat(miner_frames, ignore_index=True)
    df = df.sort_values(["miner_id", "timestamp"]).reset_index(drop=True)
    df["failure_within_horizon"] = _apply_future_failure_label(
        df=df,
        horizon_hours=cfg.prediction_horizon_hours,
        freq_minutes=cfg.freq_minutes,
    )
    df = df.drop(columns=["failure_event"])
    df = _inject_quality_issues(df=df, cfg=cfg, rng=rng)
    return df


def save_raw_telemetry(df: pd.DataFrame, path: str | None = None) -> str:
    """Save generated telemetry to CSV."""

    output_path = RAW_TELEMETRY_PATH if path is None else Path(path)
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return str(output_path)


def summarize_generated_data(df: pd.DataFrame, cfg: DataGenerationConfig) -> Dict[str, object]:
    """Return compact summary stats for logs and debugging."""

    return {
        "rows": int(len(df)),
        "miners": int(df["miner_id"].nunique()),
        "start_timestamp": str(pd.to_datetime(df["timestamp"]).min()),
        "end_timestamp": str(pd.to_datetime(df["timestamp"]).max()),
        "positive_label_rate": float(df["failure_within_horizon"].mean()),
        "config": asdict(cfg),
    }


def main() -> None:
    cfg = DataGenerationConfig()
    df = generate_synthetic_telemetry(cfg)
    RAW_TELEMETRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_TELEMETRY_PATH, index=False)
    summary = summarize_generated_data(df, cfg)
    print("Synthetic telemetry generated.")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print(f"raw_path: {RAW_TELEMETRY_PATH}")


if __name__ == "__main__":
    main()
