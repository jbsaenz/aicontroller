# Phase 2 - Synthetic Data, Ingestion, and Preprocessing

## What Was Implemented
1. Synthetic telemetry generation (`src/data_generation.py`)
   - 48 miners, 21 days, 10-minute cadence (configurable).
   - Mode-dependent behavior (`eco`, `normal`, `turbo`) with realistic clock/voltage/hashrate/power/temperature interactions.
   - Stress-driven failure-event simulation and 24-hour horizon target labeling (`failure_within_horizon`).
   - Controlled data-quality noise injection (small missingness, invalid modes, and duplicate records) to validate ingestion/cleaning.

2. Ingestion and schema validation (`src/ingestion.py`)
   - Required-column validation against assignment schema.
   - Missing/extra column checks.
   - Duplicate detection on (`timestamp`, `miner_id`).
   - Missing-value and numeric parse-null profiling.
   - Plausible-range violation checks.

3. Preprocessing (`src/preprocessing.py`)
   - Timestamp parsing and key-field validation.
   - Operating mode normalization (invalid values mapped to `normal`).
   - Numeric coercion and per-miner time-series imputation (ffill/bfill + median fallback).
   - De-duplication on (`timestamp`, `miner_id`).
   - Clipping to plausible ranges.
   - Recalculation of `efficiency_j_per_th` and `hashrate_deviation_pct`.

4. End-to-end orchestration (`src/pipeline.py`)
   - One-command Phase 2 execution.
   - Writes raw and processed data plus JSON quality summaries.

## Generated Outputs
- `data/raw/synthetic_telemetry.csv`
- `data/processed/telemetry_clean.parquet`
- `data/processed/telemetry_clean.csv`
- `outputs/metrics/phase2_generation_summary.json`
- `outputs/metrics/phase2_ingestion_report.json`
- `outputs/metrics/phase2_preprocessing_report.json`

## Current Data Snapshot (Seed=42)
- Raw rows: 145,442
- Clean rows: 145,152
- Miners: 48
- Positive label rate (`failure_within_horizon`): ~9.87%
- Duplicates removed: 290
- Invalid operating modes normalized: 145
- Missing telemetry fields imputed and resolved to 0 remaining missing values

## Assignment Alignment
- Tier 1 support: telemetry ingestion and structuring foundation completed.
- Tier 2 readiness: target label and cleaned data are ready for EDA, KPI, and modeling phases.
