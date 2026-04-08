# Phase 4 - Feature Engineering and Predictive Maintenance Baseline

## Implemented Components
- `src/feature_engineering.py`
  - rolling thermal statistics
  - power instability trend features
  - rolling hashrate degradation indicators
  - TE drift features
  - peer-group deviation features by mode and timestamp
- `src/train.py`
  - time-based train/validation split
  - baseline model comparison (Logistic Regression vs Random Forest)
  - best model selection based on F1 at optimized threshold
  - model artifact export
- `src/evaluation.py`
  - confusion-matrix-aware metrics
  - ROC-AUC and PR-AUC
  - optimized threshold selection
- `src/inference.py`
  - validation risk-score generation
  - threshold-based alert examples

## Current Phase 4 Results
- Best model: `logistic_regression`
- Validation rows: 28,992
- Optimized threshold: 0.6685
- Best-model validation metrics (optimized threshold):
  - precision: 0.3334
  - recall: 0.4341
  - f1: 0.3772
  - roc_auc: 0.7619
  - pr_auc: 0.3024

## Generated Outputs
- Feature dataset:
  - `data/processed/telemetry_features.csv`
  - `data/processed/telemetry_features.parquet`
- Metrics:
  - `outputs/metrics/phase4_feature_summary.json`
  - `outputs/metrics/phase4_model_comparison.json`
  - `outputs/metrics/phase4_best_model_summary.json`
  - `outputs/metrics/phase4_feature_importance.csv`
- Predictions:
  - `outputs/predictions/phase4_validation_risk_scores.csv`
  - `outputs/predictions/phase4_alert_examples.csv`
- Model artifact:
  - `outputs/models/phase4_best_model.joblib`

## Assignment Alignment
- Tier 2 predictive maintenance baseline completed with supervised learning.
- Model output answers: "Which miners show early warning signs of degradation or failure?"
