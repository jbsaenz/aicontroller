# AI-Driven Mining Optimization & Predictive Maintenance

## 1. Executive Summary

This project delivers a complete, production-ready system for mining fleet predictive maintenance—from raw hardware telemetry to automated operational decisions.

The system implements both required tiers of the assignment:
- **Tier 1 (Required):** A full data pipeline that ingests, cleans, and structures synthetic mining telemetry, computes a context-aware True Efficiency (TE) KPI, and performs exploratory analysis including correlation mapping, trade-off profiling, and anomaly detection.
- **Tier 2 (Option B — Predictive Maintenance):** A supervised learning prototype that engineers 33 temporal and behavioral features, trains Random Forest and Logistic Regression baselines, and generates per-miner risk scores with threshold-based alerting.

Beyond the prototype scope, the system has been extended into a deployable platform with:
- A **FastAPI REST API** serving fleet status, analytics, alerts, and runtime configuration
- An **interactive web dashboard** with real-time fleet monitoring, miner drill-down, and analytics visualization
- An **automated worker** running background ML inference, external source polling, alert notification (email/Telegram), and policy-gated action execution
- **Docker Compose** deployment with TimescaleDB, CI pipeline, and production hardening

Key metrics (validation set at recommended threshold 0.6755):
- ROC-AUC: 0.7619 | PR-AUC: 0.3022 | F1: 0.3759 | Recall: 0.4220

## 2. Problem and Assignment Fit

Bitcoin mining profitability depends on marginal gains in efficiency across power, thermal management, and uptime. The Mining Development Kit (MDK) exposes low-level telemetry and control interfaces that enable data-driven automation.

This project addresses the gap between raw hardware telemetry and intelligent operational decisions by implementing:
1. **Tier 1 (Required):** Ingest, analyze, and define KPIs from synthetic telemetry
2. **Tier 2 (Option B):** Supervised predictive maintenance to flag pre-failure hardware

Assignment fit was enforced sequentially—Tier 1 was completed before Tier 2, and all deliverables (report, codebase, architecture diagram) are present.

## 3. Data Pipeline

### 3.1 Synthetic Data Design

A configurable generator (`src/data_generation.py`) produces realistic fleet telemetry:
- **Fleet scale:** 48 miners × 21 days × 10-minute cadence = ~145,000 rows
- **Operating modes:** eco, normal, turbo—each with distinct performance envelopes
- **Physics-informed simulation:** Clock frequency drives hashrate; voltage and ambient temperature drive power and thermal stress; instability compounds degradation over time
- **Target label:** `failure_within_horizon` — binary flag indicating likely degradation within 24 hours, derived from a logistic failure model driven by stress accumulation

Quality issues (missing values, duplicates, invalid modes) are injected to exercise the preprocessing pipeline.

### 3.2 Ingestion and Preprocessing

The pipeline implements schema validation, type normalization, duplicate removal, invalid mode correction, per-miner temporal imputation, and plausible-range enforcement.

| Metric | Value |
|--------|-------|
| Raw rows | 145,442 |
| Post-cleaning | 145,152 |
| Duplicates removed | 290 |
| Positive label rate | 9.87% |

### 3.3 Live Data Ingestion (Production Extension)

Beyond the synthetic pipeline, the production system supports:
- **CSV upload** via the dashboard and `/api/ingest/csv` endpoint with payload size guardrails
- **External API polling** via configurable sources with URL validation, DNS pinning, SSRF protection, exponential backoff, and auto-disable for persistently failing sources

## 4. Tier 1: Telemetry Analysis and TE KPI

### 4.1 Correlation and Anomaly Analysis

The EDA pipeline (`src/eda.py`) computes correlations across 12 numeric telemetry and KPI columns, producing results consistent with mining physics:

| Variable Pair | Pearson r |
|---------------|-----------|
| Clock ↔ Hashrate | 0.975 |
| Voltage ↔ Power | 0.974 |
| Power ↔ Temperature | 0.816 |
| Ambient ↔ ASIC Temperature | 0.466 |
| Power Instability ↔ Failure | 0.227 |

Trade-off analysis (`compute_tradeoff_summary`) profiles per-mode means, medians, and standard deviations for hashrate, power, cooling, temperature, efficiency, and instability.

Anomaly detection uses a multi-signal approach with 5 flag types (temperature, power instability, hashrate drop, power spike, low TE). Rows with ≥2 simultaneous flags are classified as anomalous, yielding 399 anomaly rows (0.275%) for investigative triage.

### 4.2 True Efficiency (TE) KPI

The TE KPI goes beyond simple J/TH by incorporating four contextual adjustments:

```
P_total   = asic_power_w + cooling_power_w
BaseEff   = asic_hashrate_ths / P_total
V_stress  = 1 + 0.6 × max(0, (V - 12.5) / 12.5)      [voltage]
E_stress  = 1 + 0.4 × max(0, (T_ambient - 25) / 10)    [environment]
M_stress  = {eco: 0.97, normal: 1.00, turbo: 1.08}      [operating mode]
TE        = BaseEff / (V_stress × E_stress × M_stress)
```

KPI behavior is coherent: TE averages eco (0.0384) > normal (0.0368) > turbo (0.0327), reflecting that turbo mode achieves more hashrate at disproportionately higher total cost.

## 5. Tier 2: Predictive Maintenance Prototype

### 5.1 Feature Engineering

The feature engineering pipeline (`src/feature_engineering.py`) produces 33 model features from 3 categories:
- **11 base telemetry/KPI features** — raw sensor values and computed KPIs
- **21 temporal/behavioral features** — rolling statistics over 12h and 4h windows for temperature, instability, hashrate degradation, TE drift, and peer-mode deviations
- **1 categorical feature** — operating mode (one-hot encoded)

### 5.2 Model Training

A time-ordered 80/20 train/validation split preserves temporal integrity. Three model candidates were trained:

| Model | Key Hyperparameters | Rationale |
|-------|-------------------|-----------|
| **Logistic Regression** | `max_iter=1200`, `class_weight=balanced` | Linear baseline; balanced weights compensate for ~10% positive rate; extended iterations ensure convergence with 33 features |
| **Random Forest** | `n_estimators=300`, `max_depth=12`, `min_samples_leaf=5`, `class_weight=balanced_subsample` | Captures non-linear interactions; depth capped at 12 to prevent overfitting on temporal features; per-subsample balancing handles class skew |
| **XGBoost** | `n_estimators=400`, `max_depth=8`, `learning_rate=0.05`, `subsample=0.8`, `colsample_bytree=0.8`, `scale_pos_weight=auto` | Gradient-boosted ensemble; shallower trees (8) with more iterations (400) and lower learning rate for better generalization; column subsampling reduces feature co-adaptation; `scale_pos_weight` auto-computed from class imbalance ratio |

Model selection uses F1 at optimal threshold, with PR-AUC as tiebreaker.

### 5.3 Model Comparison Results

All three candidates were evaluated on the held-out validation set (28,992 rows):

| Model | F1 (optimal) | ROC-AUC | PR-AUC | Selected |
|-------|-------------|---------|--------|----------|
| **Logistic Regression** | **0.3759** | **0.7619** | **0.3022** | ✅ Best |
| Random Forest | 0.3381 | 0.7176 | 0.2345 | |
| XGBoost | 0.2804 | 0.6676 | 0.1888 | |

Logistic Regression outperformed tree-based models on this dataset. The synthetic telemetry's engineered features (rolling statistics, peer deviations, degradation trends) create approximately linear decision boundaries that LR exploits effectively. XGBoost's lower performance is attributed to the relatively small positive-class signal and the synthetic data's noise structure; on real fleet data with more complex degradation patterns, tree-based models may improve.

### 5.4 Best Model Performance and Threshold Policy

| Metric | Value |
|--------|-------|
| ROC-AUC | 0.7619 |
| PR-AUC | 0.3022 |
| Recommended threshold | 0.6755 |
| Precision | 0.3388 |
| Recall | 0.4220 |
| F1 | 0.3759 |

A threshold analysis table (`src/phase5.py`) sweeps from 0.20 to 0.90 in 0.05 increments, allowing operators to select policy based on maintenance capacity. A conservative option (threshold 0.80, precision 0.42, recall 0.16) reduces false alarms for resource-constrained operations.

### 5.4 Per-Mode Performance Breakdown

Performance varies by mode: **eco** achieves higher precision (fewer stress events to confuse the model), **normal** tracks the overall average, and **turbo** shows higher recall but lower precision due to elevated baseline stress. A per-mode chart is at `outputs/figures/phase5_mode_performance.png`.

### 5.6 Confusion Matrix Cost Analysis

At threshold 0.6755: TP=1,358 | FP=2,650 | TN=23,124 | FN=1,860. Each FP costs ~$12.50 in unnecessary inspection (15 min at $50/hr), while each TP saves ~$300 in avoided unplanned downtime. Net benefit: $407K avoided failures vs $33K inspection cost — strongly positive. Operators can raise the threshold to 0.80 for a 4.3% alert rate with higher precision.

## 6. Production System Architecture

### 6.1 System Overview

The system is deployed as three Docker Compose services:

| Service | Technology | Role |
|---------|-----------|------|
| `db` | TimescaleDB (PostgreSQL) | Hypertable storage, continuous aggregates, retention policies |
| `api` | FastAPI + Uvicorn | REST API, dashboard serving, JWT authentication |
| `worker` | APScheduler | Background jobs: KPI computation, ML inference, source polling, alerting, action execution |

### 6.2 API and Dashboard

The FastAPI application exposes 6 router modules:
- **Fleet** — Fleet overview with risk-ranked miners and summary statistics
- **Miners** — Per-miner time-series drill-down
- **Analytics** — Live correlation, trade-off, and anomaly queries (backed by TimescaleDB continuous aggregates)
- **Alerts** — Alert management with resolution tracking
- **Ingestion** — CSV upload and external API source management with URL validation
- **Settings** — Runtime configuration with encrypted secret storage

The interactive dashboard provides fleet monitoring, miner detail modals with Chart.js time-series, an analytics page with correlation heatmaps and scatter plots, an alert center, and a settings configuration panel.

### 6.3 Worker and Automation

The worker runs 6 scheduled jobs:
- **KPI Pipeline** (15 min): Computes derived KPIs for new telemetry rows
- **Inference** (15 min): Scores all miners using the latest model artifact
- **Source Polling** (10 min): Fetches data from external APIs with SSRF protection
- **Notifications** (5 min): Delivers alerts via SMTP email and Telegram
- **Automator** (5 min): Executes policy-gated actions (DOWNCLOCK, REBOOT, PULL_FOR_MAINTENANCE)
- **Retrain** (daily at 02:00 UTC): Retrains the model on recent KPI data

### 6.4 Policy Optimization Engine

The policy engine (`src/policy.py`) evaluates recommended actions using mining economics:
- Revenue modeling: hashprice (USD/PH/day), energy costs (USD/kWh with time-of-day schedules)
- Curtailment windows with penalty multipliers
- Per-action utility scoring: expected_benefit − expected_cost
- Backtest validation: actions must demonstrate positive uplift before automation

## 7. Safety and Security Considerations

### 7.1 Control Modes

The system defaults to **advisory mode** (`CONTROL_MODE=advisory`), where all alerts are informational and no hardware commands are issued. **Actuation mode** (`CONTROL_MODE=actuation`) requires explicit configuration and enables automated actions with the following safeguards:

- **Simulation mode** (`AUTOMATOR_SIMULATION=true`): Logs actions without executing
- **Circuit breaker**: Automatically pauses automation after consecutive remote failures
- **Policy backtest gate**: Actions require demonstrated positive economic uplift
- **Human-in-the-loop**: Unacknowledged actions keep alerts active for operator review
- **Endpoint allowlisting**: Only approved control-plane hosts can receive commands

### 7.2 Data Integrity

- External source URLs are validated against `API_SOURCE_ALLOWLIST` with DNS resolution and SSRF checks
- Ingestion payloads enforce size limits (`MAX_INGEST_FILE_BYTES`, `MAX_INGEST_ROWS`)
- Schema validation rejects malformed data before processing

### 7.3 Authentication and Secrets

- JWT-based authentication with bcrypt password hashing
- Encrypted secret storage for notification credentials (`APP_SETTINGS_ENCRYPTION_KEY`)
- Rate limiting on authentication and ingestion endpoints
- Cookie-based sessions with configurable security flags (Secure, SameSite, HttpOnly)

## 8. Visualization and Output Reporting

### 8.1 Static Outputs (Pipeline)

The pipeline generates 7 publication-quality figures:
- Correlation heatmap, power-hashrate trade-off scatter, TE distribution by mode
- Confusion matrix, precision-recall curve, ROC curve, risk score distribution

### 8.2 Interactive Dashboard

The web dashboard provides real-time operational visibility:
- Fleet overview with risk-coded miner cards and summary statistics
- Per-miner detail modals with temperature, hashrate, power, and risk time-series
- Analytics page with live correlation matrices, scatter plots, and anomaly tables
- Alert center with resolution actions
- Settings panel for runtime ML threshold and policy tuning

## 9. Reproducibility and Deployment

### Local Execution
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/pipeline.py --phase phase2-5
```

### Docker Deployment
```bash
cp .env.example .env    # Configure secrets
docker compose up -d --build
# Dashboard at http://localhost:8080
```

### CI/CD
GitHub Actions pipeline runs compile checks across all modules and the regression test suite (78 tests).

## 10. Conclusion

This system satisfies all assignment requirements:
- **Tier 1:** Complete telemetry pipeline with context-aware TE KPI, correlation analysis, trade-off profiling, and anomaly detection
- **Tier 2:** Functional predictive maintenance prototype with feature engineering, model training, threshold policy, and prioritized alerting

Beyond the assignment scope, the system has been extended into a production-grade platform with a live monitoring dashboard, automated ML inference, external data integration, notification delivery, policy-optimized action execution, and Docker-based deployment—demonstrating a credible foundation for real-fleet operation.
