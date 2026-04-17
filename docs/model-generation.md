# Model Generation Guide

This guide covers how to build, validate, and refresh the Phase 4 model artifact used by worker inference.

## Scope
- Builds `outputs/models/phase4_best_model.joblib` for runtime inference.
- Produces supporting analytics/metrics artifacts in `outputs/`.
- Applies to local development, staging refreshes, and pre-production model updates.

## Prerequisites
- Python 3.11+
- Repository cloned and dependencies available
- Optional: Docker stack running if you want to test the artifact end-to-end after build

## 1) Prepare environment

macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 2) Generate model artifact
Run the full analytics/modeling chain:

```bash
python src/pipeline.py --phase phase2-5
```

Optional phase-by-phase execution:

```bash
python src/pipeline.py --phase phase2
python src/pipeline.py --phase phase3
python src/pipeline.py --phase phase4
python src/pipeline.py --phase phase5
```

## 3) Verify outputs
Required runtime artifact:
- `outputs/models/phase4_best_model.joblib`

Expected supporting outputs (examples):
- `outputs/metrics/phase4_model_comparison.json`
- `outputs/metrics/phase4_best_model_summary.json`
- `outputs/metrics/phase4_feature_importance.csv`
- `outputs/metrics/phase4_feature_summary.json`
- `outputs/predictions/phase4_validation_risk_scores.csv`

Quick check:

```bash
ls -lh outputs/models/phase4_best_model.joblib
```

## 4) Use artifact in runtime
The worker reads:
- `MODEL_PATH` (default `/app/outputs/models/phase4_best_model.joblib` in containers)

If running via Docker Compose with default bind mounts, local `./outputs` is visible to the worker at `/app/outputs`.

## 5) Validate in running stack
If stack is up:

Set script credentials before running:

macOS/Linux:
```bash
export E2E_ADMIN_USERNAME=admin
export E2E_ADMIN_PASSWORD=admin
```

Windows PowerShell:
```powershell
$env:E2E_ADMIN_USERNAME = "admin"
$env:E2E_ADMIN_PASSWORD = "admin"
```

Preferred cross-platform validation:

macOS/Linux:
```bash
python3 generate_large_fleet.py
```

Windows PowerShell:
```powershell
python .\generate_large_fleet.py
```

Optional CI/Linux deep validation:
```bash
python test_e2e.py
python test_e2e_advanced.py
```

The scripts should complete without worker inference errors.

## Troubleshooting
- Missing artifact at inference time:
  - Re-run `python src/pipeline.py --phase phase2-5`.
  - Confirm file exists at `outputs/models/phase4_best_model.joblib`.
- Worker not using updated model:
  - Confirm compose bind mount includes `./outputs:/app/outputs`.
  - Confirm `MODEL_PATH` points to `/app/outputs/models/phase4_best_model.joblib`.
- Dependency issues during build:
  - Recreate virtual environment and reinstall `requirements.txt`.

## Operational notes
- Treat generated model artifacts as deployable assets and version them via release process.
- Rebuild model after major feature/KPI logic changes before promoting to staging/production.
