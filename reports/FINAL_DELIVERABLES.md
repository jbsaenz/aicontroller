# Final Deliverables Index

This index maps each assignment requirement to the exact project artifact.

## A) Technical Report (2-4 pages)
- Final submission version: `reports/technical_report_final.md`
- Extended working draft with full detail: `reports/technical_report.md`

## B) Working Python Prototype
- Main pipeline entrypoint: `src/pipeline.py`
- Full run command (Tier 1 + Tier 2 + evaluation):

```bash
python src/pipeline.py --phase phase2-5
```

- Core source modules:
  - data generation and ingestion: `src/data_generation.py`, `src/ingestion.py`, `src/preprocessing.py`
  - KPI and Tier 1 analysis: `src/kpi.py`, `src/eda.py`, `src/visualization.py`
  - predictive maintenance modeling: `src/feature_engineering.py`, `src/train.py`, `src/evaluation.py`, `src/inference.py`, `src/phase5.py`

## C) Architecture Diagram
- Final architecture (Mermaid): `reports/architecture_diagram.mmd`

## D) Evidence Artifacts for Prototype Requirements
### 1. Data ingestion and preprocessing
- `outputs/metrics/phase2_generation_summary.json`
- `outputs/metrics/phase2_ingestion_report.json`
- `outputs/metrics/phase2_preprocessing_report.json`

### 2. Feature engineering
- `outputs/metrics/phase4_feature_summary.json`
- `data/processed/telemetry_features.csv`

### 3. KPI computation
- `outputs/metrics/phase3_kpi_summary.json`
- `data/processed/telemetry_with_kpi.csv`

### 4. Model training and inference
- `outputs/metrics/phase4_model_comparison.json`
- `outputs/metrics/phase4_best_model_summary.json`
- `outputs/predictions/phase4_validation_risk_scores.csv`
- `outputs/predictions/phase4_alert_examples.csv`

### 5. Evaluation and reporting visuals
- `outputs/metrics/phase5_evaluation_summary.json`
- `outputs/metrics/phase5_threshold_analysis.csv`
- `outputs/metrics/phase5_policy_backtest.json`
- `outputs/predictions/phase5_flagged_miners.csv`
- `outputs/predictions/phase5_priority_alerts.csv`
- `outputs/figures/phase3_correlation_heatmap.png`
- `outputs/figures/phase3_power_vs_hashrate.png`
- `outputs/figures/phase3_te_by_mode.png`
- `outputs/figures/phase5_confusion_matrix.png`
- `outputs/figures/phase5_precision_recall_curve.png`
- `outputs/figures/phase5_roc_curve.png`
- `outputs/figures/phase5_risk_distribution.png`

## E) Safety/Security Coverage
- Technical report section: `reports/technical_report_final.md` (Section 7)
- Supporting docs: `reports/phase5_evaluation_notes.md`, `reports/submission_checklist.md`

## F) Portability and GitHub Readiness
- Container runtime: `Dockerfile`, `docker-compose.yml`
- CI smoke workflow: `.github/workflows/ci.yml`
- Publishing guide: `reports/github_deployment_guide.md`

## G) Suggested Submission Bundle
1. `reports/technical_report_final.md`
2. `reports/architecture_diagram.mmd`
3. `reports/FINAL_DELIVERABLES.md`
4. `src/` directory
5. `README.md`
6. `requirements.txt`
7. `Dockerfile` and `docker-compose.yml`
8. `outputs/metrics/` + `outputs/figures/` + selected `outputs/predictions/` evidence files
