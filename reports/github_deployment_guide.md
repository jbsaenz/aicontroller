# GitHub Deployment Guide

## 1) Prepare repository locally
From project root:

```bash
cd /Users/saenz/Applications/aicontroller
```

Recommended checks before publishing:

macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/pipeline.py --phase phase2-5
```

Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python .\src\pipeline.py --phase phase2-5
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
Users on macOS/Windows/Linux should use Docker Compose for the full stack (`db`, `api`, `worker`) and dashboard:

```bash
docker compose up -d --build
```

First-run dashboard note:
- Login is `admin` / `admin` by default.
- The default `ADMIN_PASSWORD_HASH` in `.env.example` is already Compose-safe. If you replace it with a custom bcrypt hash, escape `$` as `$$` in `.env`.
- If Fleet is empty after login, click **Load Sample Data** in the Fleet page (calls `POST /api/ingest/seed-demo`).
- An empty dashboard after login is expected until telemetry is ingested. Seed data with:

macOS/Linux:
```bash
export E2E_ADMIN_USERNAME=admin
export E2E_ADMIN_PASSWORD=admin
python3 generate_large_fleet.py
```

Windows PowerShell:
```powershell
$env:E2E_ADMIN_USERNAME = "admin"
$env:E2E_ADMIN_PASSWORD = "admin"
python .\generate_large_fleet.py
```

For pipeline-only container execution:

```bash
docker build -t aicontroller:latest .
docker run --rm -v "$(pwd):/app" aicontroller:latest
```

On Windows PowerShell, use:

```powershell
docker run --rm -v "${PWD}:/app" aicontroller:latest
```
