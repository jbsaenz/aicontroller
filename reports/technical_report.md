# Technical Report: AI-Driven Mining Optimization & Predictive Maintenance

## 1. Problem Statement and Objective
Bitcoin mining profitability depends on controlling efficiency, thermal stress, and hardware reliability under continuously changing operating conditions. A miner is not only a computational device but part of a coupled electrical and thermal system where small inefficiencies can cascade into elevated failure risk.

This project implements an assignment-aligned AI controller prototype with two layers:
- Tier 1 (mandatory foundation): telemetry ingestion, cleaning, operational analysis, anomaly detection, and a context-aware True Efficiency (TE) KPI.
- Tier 2 (selected use case): a supervised predictive maintenance model that identifies miners likely to degrade or fail within a short horizon.

The primary operational question addressed is:
"Which miners show early warning signs of degradation or failure, and how should those alerts be prioritized?"

## 2. Scope, Constraints, and Design Choices
Project constraints were kept explicit:
- Duration: 3 weeks.
- Data source: synthetic dataset only (no external live dependency).
- Required outputs: technical report, working Python prototype, and architecture diagram.
- Tier 1 completed first, Tier 2 added only after telemetry and KPI pipeline was functional.

Design choices prioritized feasibility and explainability:
- Synthetic but physically plausible telemetry dynamics.
- Strict schema validation and traceable preprocessing reports.
- Practical baseline models (Logistic Regression and Random Forest) before any complex sequence models.
- Human-in-the-loop alerting to avoid unsafe autonomous actions.

## 3. Data Pipeline Design (Tier 1 Foundation)
### 3.1 Synthetic telemetry generation
The generator models 48 miners over 21 days at 10-minute intervals with mode-dependent behavior (`eco`, `normal`, `turbo`) and realistic interactions among:
- ASIC clock and voltage
- hashrate
- ASIC power
- cooling load
- ambient and chip temperature
- power instability

Generated dataset summary:
- Raw rows: 145,442
- Miners: 48
- Time range: 2026-03-01 00:00:00 to 2026-03-21 23:50:00
- Label: `failure_within_horizon` (24-hour horizon)

### 3.2 Ingestion and validation
Ingestion enforces required assignment schema and writes quality diagnostics. It detected:
- Missing required columns: none
- Duplicate (`timestamp`, `miner_id`) rows: 290
- Injected missing telemetry values (examples):
  - `ambient_temperature_c`: 435
  - `asic_hashrate_ths`: 436
  - `asic_temperature_c`: 435
  - `asic_power_w`: 435

### 3.3 Preprocessing
Preprocessing performed:
- timestamp/type normalization
- duplicate removal
- invalid mode normalization (`boost` -> `normal`)
- per-miner temporal imputation (ffill/bfill + median fallback)
- plausible-range clipping
- metric recomputation (`efficiency_j_per_th`, `hashrate_deviation_pct`)

Post-cleaning summary:
- Clean rows: 145,152
- Duplicates removed: 290
- Invalid modes corrected: 145
- Remaining missing values: 0 across all required fields
- Positive label rate: 9.87%

## 4. Tier 1 Analysis and True Efficiency KPI
### 4.1 Relationship analysis
Telemetry analysis recovered expected operational patterns:
- clock vs hashrate correlation: 0.975
- voltage vs ASIC power correlation: 0.974
- ambient vs ASIC temperature correlation: 0.466
- ASIC power vs ASIC temperature correlation: 0.816
- power instability vs failure label correlation: 0.227

Anomaly profiling flagged 399 rows (0.275% of cleaned telemetry) using multi-signal stress criteria (temperature, instability, hashrate drop, low TE).

### 4.2 True Efficiency (TE) KPI
A context-adjusted KPI was implemented to go beyond raw J/TH:

- `P_total = asic_power_w + cooling_power_w`
- `BaseEff = asic_hashrate_ths / P_total`
- `V_stress = 1 + alpha_v * max(0, (asic_voltage_v - V_ref) / V_ref)`
- `E_stress = 1 + alpha_e * max(0, (ambient_temperature_c - T_ref) / 10)`
- `M_stress = {eco: 0.97, normal: 1.00, turbo: 1.08}`
- `TE = BaseEff / (V_stress * E_stress * M_stress)`

Constants used:
- `V_ref = 12.5`
- `T_ref = 25`
- `alpha_v = 0.6`
- `alpha_e = 0.4`

KPI summary:
- TE mean: 0.03639
- TE median: 0.03638
- TE range: [0.01735, 0.06296]

Mode-level TE behavior (average):
- `eco`: 0.0384
- `normal`: 0.0368
- `turbo`: 0.0327

This ranking is operationally coherent: higher-performance modes increase hashrate but carry heavier electrical/thermal burden, reducing context-adjusted efficiency.

## 5. Predictive Maintenance Prototype (Tier 2)
### 5.1 Feature engineering
31 model features were built:
- 11 base telemetry/KPI features
- 19 engineered temporal/deviation features
- 1 categorical mode feature

Key engineered groups:
- rolling temperature mean/max/std
- rolling power-instability trend and variance
- rolling hashrate degradation vs 12-hour baseline
- TE drift and delta
- peer-mode deviation features at each timestamp

### 5.2 Model training strategy
A time-ordered split (80/20) was used:
- Train rows: 116,160
- Validation rows: 28,992

Models compared:
- Logistic Regression (`class_weight='balanced'`)
- Random Forest (`balanced_subsample`)

Best selected model: Logistic Regression (by F1 at optimized threshold).

### 5.3 Validation performance
At default threshold 0.50:
- Precision: 0.2233
- Recall: 0.7383
- F1: 0.3429

At optimized threshold 0.6685 (recommended):
- Accuracy: 0.8409
- Precision: 0.3334
- Recall: 0.4341
- F1: 0.3772
- ROC-AUC: 0.7619
- PR-AUC: 0.3024

Confusion matrix at 0.6685:
- TP: 1,397
- FP: 2,793
- TN: 22,981
- FN: 1,821

Top contributing features were operationally plausible:
- `power_instability_roll_mean_1h`
- `hashrate_roll_mean_12h`
- `hashrate_roll_mean_1h`
- `hashrate_degradation_pct_12h`
- `asic_power_w`
- `true_efficiency_te`

## 6. Phase 5 Evaluation and Alert Prioritization
Phase 5 added deployment-facing evaluation artifacts:
- threshold sweep table (`0.20` to `0.90`)
- conservative threshold candidate selection
- flagged-miner ranking
- priority alert export
- confusion matrix, PR, ROC, and risk-distribution visualizations

Recommended operating threshold (balanced): `0.6685`
- Alerted row rate: ~14.45%

Conservative threshold candidate: `0.80`
- Precision: 0.4180
- Recall: 0.1631
- Alert rate: 4.33%

This provides a practical policy trade-off:
- use 0.6685 for broader early-warning coverage
- use 0.80 when operations require stricter, lower-noise triage

Flagged miners with highest priority scores included:
- `miner_016`
- `miner_046`
- `miner_009`
- `miner_033`

## 7. Safety and Security Considerations
This prototype is intentionally decision-support, not autonomous control.

### 7.1 False positives and false negatives
- False positives increase unnecessary inspections and maintenance overhead.
- False negatives risk missed degradation and unplanned downtime.
- Threshold policy is exposed explicitly so operations can tune precision/recall trade-offs.

### 7.2 Human-in-the-loop control
- No direct hardware command execution is triggered by the model.
- Alerts are reviewed by operators before any intervention (clock/voltage/maintenance actions).
- Priority lists support triage; final decisions remain under human supervision.

### 7.3 Telemetry integrity and data quality
- Input schema validation is mandatory at ingestion.
- Missingness, duplicates, and invalid modes are reported and remediated.
- Any production deployment should include signed telemetry transport, source authentication, and anomaly checks for spoofed or stale readings.

### 7.4 Safe operating constraints
- Even with high predicted risk, interventions should remain conservative and bounded by vendor thermal/electrical limits.
- Recommended practice: apply staged actions (inspection -> reduced mode -> maintenance), not aggressive automatic changes.

## 8. Feasibility and Operational Value
The implemented design is feasible within a student project window and credible for extension:
- modular Python pipeline
- reproducible synthetic generation
- Dockerized execution for cross-platform portability
- CI smoke checks for GitHub-readiness

Expected benefits in a mining operation context:
- earlier identification of degrading miners
- reduced downtime risk through prioritized maintenance
- improved operational visibility via TE KPI and alerting outputs

## 9. Limitations and Future Work
Current limitations:
- synthetic dataset, not yet calibrated to real MDK fleet behavior
- single-horizon binary target (24h) may miss nuanced degradation trajectories
- baseline models only; no sequential model calibration

Future improvements:
- validate on real telemetry streams
- add probability calibration and cost-sensitive optimization
- include explainability per-alert (local feature attributions)
- integrate with maintenance ticketing workflow and post-action feedback loop

## 10. Conclusion
The project meets assignment requirements by delivering:
- a complete Tier 1 telemetry and KPI foundation
- a functional Tier 2 predictive maintenance baseline
- practical evaluation and alert outputs with explicit safety controls

The resulting prototype is technically coherent, deployable in Docker, and ready for GitHub publication. It provides a credible starting point for mining-site reliability decision support while preserving human oversight for safety-critical actions.
