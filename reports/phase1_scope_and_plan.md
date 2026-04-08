# Phase 1 - Scope, Strategy, and Plan

## 1. Assignment Constraints and Success Criteria
- Timebox: 3 weeks.
- Tier 1 Telemetry Analysis & Data Pipeline is required and must be complete first.
- Tier 2 is selected as Predictive Maintenance (Option B).
- Required deliverables:
  - technical report (2-4 pages)
  - working Python prototype
  - architecture diagram
- Prototype capabilities required:
  - data ingestion and preprocessing
  - feature engineering
  - KPI computation
  - model training and inference
  - basic visualization/reporting
- Synthetic dataset is acceptable and preferred if no provided dataset is available.
- Safety/security discussion is mandatory (false positives/negatives, control risks, telemetry integrity, human oversight).

## 2. Confirmed Project Strategy
1. Build Tier 1 foundation:
   - telemetry ingestion and structuring
   - operational relationship analysis (clock, voltage, hashrate, temperature, power)
   - anomalies/trade-off analysis
   - True Efficiency (TE) KPI definition and implementation
2. Extend to Tier 2 predictive maintenance:
   - supervised learning risk model for degradation/failure warnings
   - interpretable outputs and threshold-based alerts

## 3. Assumptions
- No external real-time MDK stream is required for prototype validation.
- Synthetic telemetry will represent realistic miner behavior under multiple operating modes.
- Failure labels are generated from stress-driven pre-failure patterns, not random noise only.
- Operating modes (`eco`, `normal`, `turbo`) alter expected power/hashrate/temperature envelopes.

## 4. Synthetic Dataset Schema and Label
Columns:
- timestamp
- miner_id
- operating_mode
- ambient_temperature_c
- cooling_power_w
- asic_clock_mhz
- asic_voltage_v
- asic_hashrate_ths
- asic_temperature_c
- asic_power_w
- efficiency_j_per_th
- power_instability_index
- hashrate_deviation_pct
- failure_within_horizon

Target design:
- `failure_within_horizon` is binary (0/1).
- Label is 1 if a miner is expected to fail/degrade within a fixed prediction horizon (planned: next 24h), based on sustained thermal/electrical/behavioral stress patterns.

## 5. Proposed True Efficiency (TE) KPI
Let:
- `P_total = asic_power_w + cooling_power_w`
- `BaseEff = asic_hashrate_ths / P_total`
- `V_stress = 1 + alpha_v * max(0, (asic_voltage_v - V_ref) / V_ref)`
- `E_stress = 1 + alpha_e * max(0, (ambient_temperature_c - T_ref) / 10)`
- `M_stress` mode factor (`eco=0.97`, `normal=1.00`, `turbo=1.08`)

Then:
- `TE = BaseEff / (V_stress * E_stress * M_stress)`

Planned constants:
- `V_ref=12.5`, `T_ref=25`, `alpha_v=0.6`, `alpha_e=0.4`

Rationale:
- Goes beyond raw J/TH by including cooling burden, voltage stress, environmental stress, and operating-mode aggressiveness.

## 6. First Coding Pass Plan
1. Implement synthetic data generator (`src/data_generation.py`).
2. Implement ingestion and schema validation (`src/ingestion.py`).
3. Implement cleaning/preprocessing (`src/preprocessing.py`).
4. Implement TE KPI computation (`src/kpi.py`).
5. Implement EDA and anomaly analysis (`src/eda.py`, `src/visualization.py`).
6. Implement features for predictive maintenance (`src/feature_engineering.py`).
7. Train baseline models and evaluate (`src/train.py`, `src/evaluation.py`).
8. Run inference and generate risk alerts (`src/inference.py`).
9. Persist outputs in `outputs/` and document in `reports/`.

## Scope Check
1. Requirement satisfied in this phase:
   - scope clarification, assumptions, KPI proposal, and implementation roadmap.
2. Missing after this phase:
   - actual dataset, pipeline code execution, trained model, plots, and final report body.
3. Must not forget:
   - finish Tier 1 first, document assumptions explicitly, include safety/security constraints.
