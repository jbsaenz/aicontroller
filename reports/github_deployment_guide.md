# GitHub Deployment Guide

## 1) Prepare repository locally
From project root:

```bash
cd /Users/saenz/Applications/aicontroller
```

Recommended checks before publishing:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/pipeline.py --phase phase2-5
```

## 2) What is already prepared
- Dockerized execution (`Dockerfile`, `docker-compose.yml`)
- `.dockerignore` and `.gitignore` configured to avoid committing generated large artifacts
- reproducible pipeline command (`python src/pipeline.py --phase ...`)
- assignment tracking docs in `reports/`

## 3) Initialize and push to GitHub (when remote is ready)

```bash
git init
git add .
git commit -m "Initial assignment-aligned telemetry + KPI + EDA pipeline"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

## 4) Suggested public README content
- project objective and assignment scope
- quickstart (local + Docker)
- generated outputs and interpretation
- safety/security caveats (human review, telemetry integrity, conservative alerting)

## 5) Cross-platform usage note
Users on macOS/Windows/Linux can run the exact same pipeline through Docker:

```bash
docker build -t aicontroller:latest .
docker run --rm -v "$(pwd):/app" aicontroller:latest
```

On Windows PowerShell, use:

```powershell
docker run --rm -v "${PWD}:/app" aicontroller:latest
```
