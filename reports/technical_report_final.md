# AI-Driven Mining Optimization & Predictive Maintenance

## 1. Executive Summary
This project delivers a complete assignment-aligned prototype for mining operations, with Tier 1 telemetry analytics as the mandatory foundation and Tier 2 predictive maintenance as the selected AI extension.

The implemented system ingests and cleans mining telemetry, defines a context-aware True Efficiency (TE) KPI, identifies operational anomalies, and trains a supervised model to flag miners with elevated near-term degradation risk. The prototype was built with synthetic data only, as permitted by the assignment, and packaged for reproducible execution via Python and Docker.

Key result highlights:
- Cleaned telemetry: 145,152 records across 48 miners.
- Tier 1 relationships recovered with strong expected correlations (clock-hashrate, voltage-power, power-temperature).
- TE KPI implemented with cooling, voltage, environmental, and operating-mode adjustments.
- Predictive maintenance baseline selected: Logistic Regression.
- Validation performance at recommended threshold 0.6685:
  - Precision: 0.3334
  - Recall: 0.4341
  - F1: 0.3772
  - ROC-AUC: 0.7619
  - PR-AUC: 0.3024
- Phase 5 outputs provide threshold policy guidance and prioritized miner alert lists for operations.

## 2. Problem and Assignment Fit
Mining profitability depends on balancing hashrate, power, cooling burden, and hardware health. The project objective was to bridge raw miner telemetry and actionable operational decisions.

Assignment fit was enforced with this sequence:
1. Tier 1 first (required): ingest/structure telemetry, analyze core variable relationships, define TE KPI, and identify anomalies.
2. Tier 2 second (selected): build supervised predictive maintenance model to detect pre-failure behavior.

No scope deviation was introduced to unrelated use cases.

## 3. Data and Pipeline
### 3.1 Synthetic data design
A synthetic fleet telemetry generator was implemented for 48 miners over 21 days at 10-minute cadence. Operating modes (`eco`, `normal`, `turbo`) alter performance envelopes and stress profiles.

Mode and stress dynamics were modeled to preserve plausible relationships:
- higher clock/voltage tends to increase hashrate and power
- higher ambient temperature increases chip temperature and cooling load
- instability and hashrate degradation increase failure likelihood

Target label:
- `failure_within_horizon` is binary and indicates expected failure/degradation within the next 24 hours.

### 3.2 Ingestion and preprocessing
The Tier 1 data path includes schema checks and quality controls.

Raw telemetry quality profile:
- rows: 145,442
- duplicate (`timestamp`, `miner_id`) rows: 290
- missing values injected in key telemetry fields (ambient, hashrate, ASIC temperature, ASIC power)

Preprocessing actions:
- type normalization and timestamp parsing
- duplicate removal
- invalid mode normalization (`boost` -> `normal`)
- per-miner temporal imputation and median fallback
- plausible-range enforcement
- recomputation of derived fields

Post-processing result:
- clean rows: 145,152
- missing required values after cleaning: 0
- positive class rate: 9.87%

## 4. Tier 1 Telemetry Analysis and TE KPI
### 4.1 Relationship and anomaly analysis
The analysis step produced correlations consistent with mining behavior:
- `asic_clock_mhz` vs `asic_hashrate_ths`: 0.975
- `asic_voltage_v` vs `asic_power_w`: 0.974
- `ambient_temperature_c` vs `asic_temperature_c`: 0.466
- `asic_power_w` vs `asic_temperature_c`: 0.816
- `power_instability_index` vs `failure_within_horizon`: 0.227

Anomaly detection (multi-signal criteria) flagged 399 rows (0.275% of clean data), generating a candidate set for investigative triage.

### 4.2 True Efficiency KPI
A context-adjusted True Efficiency (TE) KPI was implemented:

- `P_total = asic_power_w + cooling_power_w`
- `BaseEff = asic_hashrate_ths / P_total`
- `V_stress = 1 + alpha_v * max(0, (asic_voltage_v - V_ref) / V_ref)`
- `E_stress = 1 + alpha_e * max(0, (ambient_temperature_c - T_ref) / 10)`
- `M_stress = {eco:0.97, normal:1.00, turbo:1.08}`
- `TE = BaseEff / (V_stress * E_stress * M_stress)`

Constants:
- `V_ref=12.5`, `T_ref=25`, `alpha_v=0.6`, `alpha_e=0.4`

KPI behavior was coherent:
- TE mean: 0.03639
- TE mode averages: eco (0.0384) > normal (0.0368) > turbo (0.0327)

This satisfies the assignment requirement to go beyond J/TH by explicitly integrating cooling and contextual stressors.

## 5. Tier 2 Predictive Maintenance Prototype
### 5.1 Feature engineering
Feature engineering produced 31 model features:
- 11 base telemetry/KPI features
- 19 engineered temporal/behavioral features
- 1 categorical operating mode feature

Important engineered categories:
- rolling thermal statistics
- rolling instability trends
- hashrate degradation vs 12h baseline
- TE drift and delta metrics
- peer-mode deviation features

### 5.2 Baseline modeling
A time-ordered train/validation split (80/20) was used:
- train rows: 116,160
- validation rows: 28,992

Baselines evaluated:
- Logistic Regression
- Random Forest

Selected model:
- Logistic Regression, chosen by best F1 at optimized threshold.

### 5.3 Performance and threshold policy
At threshold 0.6685 (recommended balanced policy):
- Accuracy: 0.8409
- Precision: 0.3334
- Recall: 0.4341
- F1: 0.3772
- ROC-AUC: 0.7619
- PR-AUC: 0.3024

Confusion matrix:
- TP: 1,397
- FP: 2,793
- TN: 22,981
- FN: 1,821

Conservative option (precision-focused):
- threshold: 0.80
- precision: 0.4180
- recall: 0.1631
- alert rate: 4.33%

This allows operational teams to select threshold policy based on maintenance capacity and tolerance for false alarms.

## 6. Operational Outputs
The prototype produces deployable outputs for decision support:
- per-record risk scores
- threshold-based predicted risk labels
- flagged-miner ranking
- priority alert list
- evaluation metrics and threshold sweep table
- supporting figures (correlation, TE distribution, confusion matrix, PR/ROC, risk distribution)

Top flagged miners by priority score include:
- `miner_016`
- `miner_046`
- `miner_009`

## 7. Safety and Security Considerations
The system defaults to non-autonomous advisory mode (`CONTROL_MODE=advisory`), with optional controlled actuation mode for environments that provide external command acknowledgement (`CONTROL_MODE=actuation`).

1. False positive/negative trade-off:
- low thresholds improve recall but increase maintenance noise
- high thresholds improve precision but miss more true failures

2. Human-in-the-loop requirement:
- all hardware-impacting actions require operator approval
- alerts are triage inputs, not direct control commands

3. Telemetry integrity and data quality:
- schema validation and quality reports are mandatory before modeling
- missingness and duplicates are explicitly managed
- production deployment should add transport authentication and anti-tamper checks

4. Safe operating constraints:
- recommendations should remain within vendor electrical/thermal limits
- staged interventions are preferred over abrupt mode changes

## 8. Portability and Deployment Readiness
The project is prepared for cross-platform execution and publication:
- Docker runtime (`Dockerfile`, `docker-compose.yml`)
- GitHub CI smoke pipeline (`.github/workflows/ci.yml`)
- documented execution paths in `README.md`
- submission index in `reports/FINAL_DELIVERABLES.md`

## 9. Conclusion
The delivered system satisfies assignment requirements end-to-end:
- Tier 1 telemetry pipeline, analysis, and TE KPI are complete.
- Tier 2 predictive maintenance baseline is functional and evaluated.
- evaluation/alert artifacts are practical for operational triage.
- safety and governance constraints are explicitly addressed.

This prototype is a credible foundation for real-fleet validation and incremental extension to production-grade maintenance decision support.
