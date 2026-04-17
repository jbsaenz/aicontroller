# AI-Driven Mining Optimization & Predictive Maintenance Controller
## Technical Report (Assignment Submission)

**Prepared by:** Jose Saenz  
**Course/Program:** Dev Generation  
**Date:** April 17, 2026  
**Repository (HTTPS):** https://github.com/jbsaenz/aicontroller.git  
**Repository (SSH):** git@github.com:jbsaenz/aicontroller.git

## Abstract
Bitcoin mining operations require continuous optimization across hashrate, power, thermal conditions, and hardware reliability. This project implements an AI-driven controller prototype that transforms raw miner telemetry into operational intelligence and actionable risk signals. The solution covers the required data engineering foundation (telemetry ingestion, preprocessing, KPI design, exploratory analysis) and extends into predictive maintenance using supervised learning on synthetic fleet data. The pipeline generates reproducible artifacts, including model outputs, evaluation metrics, anomaly reports, and visualizations. Beyond offline analytics, the project includes a production-oriented architecture with API, worker automation, dashboard monitoring, and policy-based action recommendations. Results show the prototype can identify elevated failure risk with useful ranking performance while providing transparent, safety-gated decision logic for real-world operations.

## Overview
This submission is designed to be understandable without reading source code.

What this project does in plain language:
- Collects mining machine telemetry data (temperature, power, hashrate, voltage, mode).
- Calculates an improved efficiency metric (True Efficiency, TE).
- Detects risky machine behavior early using an AI model.
- Produces ranked alerts and recommended actions for operations teams.

What is included in this document:
- A concise technical report narrative.
- Prototype description and key performance results.
- Architecture design and security/safety considerations.
- A checklist mapping assignment deliverables to concrete evidence.

What is optional for deeper review:
- Full implementation details and code are available in the repository links listed in Section 8.

## 1. Problem Statement and Project Objective
Modern mining profitability depends on marginal gains and disciplined operational control. Miners must balance compute output and energy cost while preventing thermal and electrical stress that drives hardware degradation. The objective of this project is to design an AI-driven controller that consumes real-time or simulated telemetry and produces an optimized control policy for mining operations.

The implementation focuses on **Predictive Maintenance** (Tier 2, Option B), while fully delivering Tier 1 requirements:
- ingest and map core telemetry variables,
- define and compute an improved efficiency KPI,
- analyze correlations, trade-offs, and anomalies,
- build a supervised model to identify pre-failure behavior,
- expose outputs through reporting and operational interfaces.

## 2. System Architecture
The system is implemented as a modular pipeline with operational deployment components:

**Telemetry Source -> Ingestion/Validation -> Preprocessing -> KPI Engine -> Feature Engineering -> ML Inference -> Alerting/Policy Layer -> Operator Review/Action**

**Figure 1: Architecture Diagram**  
Suggested image: `reports/architecture_diagram.png`

Runtime services include:
- **TimescaleDB/PostgreSQL** for telemetry, KPI, predictions, and alerts.
- **FastAPI** for ingestion, analytics, fleet, alerts, and settings endpoints.
- **Worker (APScheduler)** for KPI computation, inference, source polling, notifications, automator execution, and retraining.
- **Dashboard (HTML/JS/CSS)** for fleet visibility, analytics, and alert handling.

## 3. Tier 1: Telemetry Analysis and Data Pipeline

### 3.1 Synthetic Telemetry Design
A synthetic dataset was generated to represent a mining fleet with operational variability by mode (`eco`, `normal`, `turbo`) and stress patterns over time.

Generated dataset summary:
- Rows: **145,442**
- Miners: **48**
- Label positive rate (`failure_within_horizon`): **9.87%**

The simulation includes realistic relationships among clock, voltage, power, temperature, ambient conditions, and degradation state.

### 3.2 Ingestion and Preprocessing
The ingestion stage validates schema and data quality. Preprocessing applies timestamp parsing, mode normalization, duplicate handling, numeric coercion, missing-value imputation, and range clipping.

Preprocessing summary:
- Input rows: **145,442**
- Output rows: **145,152**
- Duplicates removed: **290**
- Post-cleaning label positive rate: **9.87%**

This establishes a clean, structured base for KPI and modeling workflows.

### 3.3 True Efficiency (TE) KPI Definition
The project defines a **True Efficiency (TE)** KPI that extends basic J/TH by incorporating site-level operational context.

Formula:

`P_total = asic_power_w + cooling_power_w`  
`BaseEff = asic_hashrate_ths / P_total`  
`V_stress = 1 + alpha_v * max(0, (asic_voltage_v - V_ref)/V_ref)`  
`E_stress = 1 + alpha_e * max(0, (ambient_temperature_c - T_ref)/10)`  
`M_stress = mode_factor(operating_mode)`  
`TE = BaseEff / (V_stress * E_stress * M_stress)`

Where defaults are `V_ref=12.5`, `T_ref=25`, `alpha_v=0.6`, `alpha_e=0.4`, and mode factors `{eco: 0.97, normal: 1.00, turbo: 1.08}`.

KPI behavior by mode (mean TE):
- `eco`: **0.0384**
- `normal`: **0.0368**
- `turbo`: **0.0327**

This is consistent with expected trade-offs: turbo increases throughput but at higher total operational stress/cost.

### 3.4 Correlation, Trade-Off, and Anomaly Findings
Key Pearson correlations:
- `asic_clock_mhz` vs `asic_hashrate_ths`: **0.9751**
- `asic_voltage_v` vs `asic_power_w`: **0.9738**
- `asic_power_w` vs `asic_temperature_c`: **0.8159**
- `ambient_temperature_c` vs `asic_temperature_c`: **0.4656**
- `power_instability_index` vs `failure_within_horizon`: **0.2271**
- `true_efficiency_te` vs `failure_within_horizon`: **-0.0480**

Anomaly detection (multi-signal thresholding) identified:
- Anomalous rows: **399**
- Anomaly rate: **0.2749%**

**Figure 2: Correlation Heatmap**  
Suggested image: `outputs/figures/phase3_correlation_heatmap.png`

**Figure 3: TE by Operating Mode**  
Suggested image: `outputs/figures/phase3_te_by_mode.png`

## 4. Tier 2: Predictive Maintenance Prototype

### 4.1 Feature Engineering
The modeling layer generates **33 features**:
- 11 base telemetry/KPI features,
- 21 engineered temporal/behavioral features,
- 1 categorical feature (`operating_mode`).

Feature set includes rolling thermal behavior, instability trends, hashrate degradation, TE drift, and peer-relative mode deviations.

### 4.2 Model Training and Selection
A time-aware split was used (80% train / 20% validation), preserving chronological ordering. Candidate models:
- Logistic Regression,
- Random Forest,
- XGBoost.

Selected model: **Logistic Regression**  
Selected threshold: **0.6755**

Validation metrics at selected threshold:
- ROC-AUC: **0.7618**
- PR-AUC: **0.3022**
- Precision: **0.3388**
- Recall: **0.4220**
- F1: **0.3759**
- Confusion matrix: TP=**1358**, FP=**2650**, TN=**23124**, FN=**1860**

A conservative operational option is also provided:
- Threshold **0.80** -> Precision **0.4170**, Recall **0.1616**

This supports policy tuning based on maintenance team capacity and alert tolerance.

### 4.3 Inference and Output Reporting
Inference produces:
- per-miner risk scores,
- risk bands,
- flagged miner rankings,
- priority alerts for actioning.

Output artifacts include:
- model artifact (`outputs/models/phase4_best_model.joblib`),
- risk predictions and alert tables,
- threshold analysis and phase evaluation summaries.

**Figure 4: ROC Curve**  
Suggested image: `outputs/figures/phase5_roc_curve.png`

**Figure 5: Confusion Matrix**  
Suggested image: `outputs/figures/phase5_confusion_matrix.png`

## 5. Expected Operational Benefits
The proposed controller provides measurable operational value:
- **Earlier failure detection:** supports proactive interventions before critical downtime.
- **Maintenance prioritization:** risk-ranked alerts help allocate field resources efficiently.
- **Efficiency visibility:** TE KPI captures power/cooling/environment context beyond basic J/TH.
- **Decision consistency:** policy layer standardizes recommendations under configurable safety and economic constraints.

## 6. Security, Safety, and Control Considerations
The implementation includes explicit safeguards:
- URL allowlisting and SSRF-oriented checks for external data sources.
- Authentication via JWT with rate limiting and credential hashing.
- Encrypted storage of sensitive runtime settings.
- Advisory-by-default control mode; actuation requires explicit enablement.
- Automation gating by policy backtest and control mode.
- Circuit-breaker behavior for repeated automator remote failures.
- Human-in-the-loop retention when external action acknowledgment is missing.

These controls reduce operational and cyber risk during autonomous or semi-autonomous workflows.

## 7. Deliverables Coverage

| Expected Deliverable (Assignment) | Included in This Document | Where to Verify in Repo | Status |
|---|---|---|---|
| Technical report (2-4 pages) covering problem, approach, pipeline, KPI, benefits, security/safety | Yes (Sections 1-6, 9-10, Conclusion) | `reports/word_submission_draft.docx`, `reports/technical_report_final.md` | Met |
| Functional data pipeline | Yes (Section 3) | `src/pipeline.py`, `src/ingestion.py`, `src/preprocessing.py` | Met |
| Telemetry mapping and analysis (clock, voltage, hashrate, temperature, power) | Yes (Section 3.4) | `src/config.py`, `outputs/metrics/phase3_eda_summary.json` | Met |
| True Efficiency KPI definition and justification | Yes (Section 3.3) | `src/kpi.py`, `outputs/metrics/phase3_kpi_summary.json` | Met |
| AI prototype on synthetic dataset (training + inference/policy logic) | Yes (Section 4) | `src/feature_engineering.py`, `src/train.py`, `outputs/metrics/phase4_best_model_summary.json` | Met |
| Basic visualization/output reporting | Yes (Sections 3.4 and 4.3) | `outputs/figures/*`, `outputs/metrics/*`, `outputs/predictions/*` | Met |
| Architecture diagram | Yes (Section 2) | `reports/architecture_diagram.png`, `reports/architecture_diagram.mmd` | Met |
| Prototype description (brief, non-technical) | Yes (Overview + Sections 1-2) | `README.md` | Met |

## 8. Where to Find Full Technical Details
To keep this submission concise, full implementation detail is maintained in the repository:

- Project setup, architecture, API/worker overview, and deployment instructions: `README.md`
- Extended technical write-up: `reports/technical_report_final.md`
- End-to-end pipeline orchestration: `src/pipeline.py`
- KPI implementation: `src/kpi.py`
- Feature engineering and model training: `src/feature_engineering.py`, `src/train.py`
- Evaluation and threshold policy outputs: `outputs/metrics/phase5_evaluation_summary.json`
- Architecture figure source files: `reports/architecture_diagram.mmd`, `reports/architecture_diagram.png`

## 9. Limitations and Next Steps
Current limitations:
- Synthetic data quality can differ from real miner telemetry distributions.
- Precision/recall trade-off indicates room for calibration and model refinement.
- Real-time KPI quality depends on incoming source consistency and schema hygiene.

Priority next steps:
- Incorporate larger and more diverse telemetry histories from live fleets.
- Calibrate thresholds by operational cost curves and maintenance SLA targets.
- Expand model explainability outputs for operator trust and auditability.
- Strengthen production runbooks and CI checks around end-to-end packaging workflows.

## 10. Reproducibility and Verification
Recommended verification flow:
1. **Primary (Docker runtime):** run the full stack with Docker Compose for production-like behavior (DB + API + worker).
2. **Offline analytics/modeling path (Python):** run the full pipeline (`phase2-5`) to regenerate analysis/model artifacts.
3. Validate model artifact, metrics JSONs, and figures.
4. Run tests for regression/integration checks.

Suggested commands:
- Docker runtime: `docker compose up -d --build`
- Offline pipeline (inside venv): `PYTHONPATH=. python -m src.pipeline --phase phase2-5`
- Tests: `./.venv/bin/pytest -q`

Cross-platform note:
- The Docker Compose workflow is designed to run the same on **macOS** and **Windows** (via Docker Desktop).
- Minor differences are limited to shell/path syntax (PowerShell/WSL vs. zsh/bash), not system behavior.
- On either OS, ensure required ports (e.g., `8080`, `5433`) are available before starting the stack.

Core reproducibility artifacts:
- `outputs/metrics/phase3_kpi_summary.json`
- `outputs/metrics/phase4_best_model_summary.json`
- `outputs/metrics/phase5_evaluation_summary.json`
- `outputs/models/phase4_best_model.joblib`

## Conclusion
This project delivers an end-to-end AI-driven mining controller prototype aligned with assignment goals: telemetry ingestion and mapping, context-aware KPI engineering, exploratory analytics, supervised predictive maintenance, and operational reporting. The solution demonstrates a credible path from raw telemetry to risk-informed action recommendations while incorporating key safety and security controls. In summary, the work provides both assignment-level completeness and a practical foundation for real operational deployment and iterative production hardening.
