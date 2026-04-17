# Deployment Guide

This guide is the operations runbook for deploying AI Controller in `local`, `staging`, and `production` environments.
For prioritized execution tickets and sequencing, see `PRODUCTION_BACKLOG.md`.
For model artifact generation and validation, see `model-generation.md`.

## Deployment model
- Runtime: Docker Compose
- Services: `db` (TimescaleDB/PostgreSQL), `api` (FastAPI + dashboard), `worker` (APScheduler background jobs)
- Persistent state: `pgdata` Docker volume (database) and `./outputs` bind mount (model artifacts, metrics, reports)

## Common prerequisites
- Docker Desktop (Windows/macOS) or Docker Engine + Docker Compose v2 (Linux)
- 4+ vCPU, 8+ GB RAM recommended
- Git
- Network access for external source polling only to allowlisted endpoints

## Common preparation
1. Clone repository and enter it:
```bash
git clone https://github.com/<owner>/<repo>.git
cd <repo>
```
2. Create env file:

macOS/Linux:
```bash
cp .env.example .env
```

Windows PowerShell:
```powershell
Copy-Item .env.example .env
```
3. Set required secrets and credentials in `.env`:
- `POSTGRES_PASSWORD`
- `JWT_SECRET`
- `APP_SETTINGS_ENCRYPTION_KEY`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD_HASH`
- `ALLOWED_ORIGINS`
- `AUTH_LOGIN_RATE_LIMIT`
- `AUTH_COOKIE_SECURE` (`true` under HTTPS)
- `API_SOURCE_ALLOWLIST`
- `AUTOMATOR_ENDPOINT_ALLOWLIST` (required if `CONTROL_MODE=actuation`)

## Local environment
Use this for developer validation and quick smoke tests.

### Local start
1. Optional model build (recommended so worker uses ML inference):

macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python src/pipeline.py --phase phase2-5
```

Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python .\src\pipeline.py --phase phase2-5
```
2. Start stack:
```bash
docker compose up -d --build
```
3. Verify:

macOS/Linux:
```bash
docker compose ps
curl -s http://localhost:8080/api/health
```

Windows PowerShell:
```powershell
docker compose ps
Invoke-RestMethod -Uri http://localhost:8080/api/health
```
4. Access dashboard at `http://localhost:8080`.

### Local first-run troubleshooting
- If login with `admin` / `admin` fails, validate `ADMIN_USERNAME` and `ADMIN_PASSWORD_HASH` in `.env` and rebuild:
```bash
docker compose up -d --build
```
- Bcrypt hashes contain `$` characters. If Docker Compose reports interpolation warnings (for example about unset vars like `2b` or `12`), escape each `$` as `$$` in `.env` (`$2b$12$...` becomes `$$2b$$12$$...`).
- If login succeeds but the dashboard is empty, this is expected on a fresh database until telemetry is ingested.
- Seed synthetic telemetry and force KPI/inference/alert processing:

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

- If data still does not appear, check worker logs for `KPI`/`Inference` execution:
```bash
docker compose logs worker --tail=200
```

### Local reset (destructive)

macOS/Linux:
```bash
docker compose down -v
rm -rf outputs/*
```

Windows PowerShell:
```powershell
docker compose down -v
Remove-Item .\outputs\* -Recurse -Force
```

## Staging environment
Use this for pre-production validation with production-like config.

### Staging principles
- Separate host or VM from production
- Separate `.env` values and credentials
- `CONTROL_MODE=advisory` unless explicitly testing actuation path
- Restrictive `API_SOURCE_ALLOWLIST` for staging data sources
- Run migrations before validation tests

### Staging rollout
1. Pull target revision:
```bash
git fetch --all --tags
git checkout <release-tag-or-commit>
```
2. Build and start:
```bash
docker compose --env-file .env up -d --build
```
3. For existing DBs, apply migrations:
```bash
docker compose exec -T db psql -U aicontroller -d aicontroller < scripts/apply_analytics_rollup.sql
docker compose exec -T db psql -U aicontroller -d aicontroller < scripts/apply_runtime_safety_settings.sql
docker compose exec -T db psql -U aicontroller -d aicontroller < scripts/apply_policy_economics_tuning.sql
```
4. If migrating from older data with duplicates, optionally run:
```bash
docker compose exec -T db psql -U aicontroller -d aicontroller < scripts/apply_dedup_indexes.sql
```
`apply_dedup_indexes.sql` deletes duplicate rows from telemetry hypertables before creating unique indexes. Run only during maintenance and after a successful backup.
5. Validate:

macOS/Linux:
```bash
docker compose ps
curl -s http://localhost:8080/api/health
export E2E_ADMIN_USERNAME=admin
export E2E_ADMIN_PASSWORD=admin
python3 generate_large_fleet.py
```

Windows PowerShell:
```powershell
docker compose ps
Invoke-RestMethod -Uri http://localhost:8080/api/health
$env:E2E_ADMIN_USERNAME = "admin"
$env:E2E_ADMIN_PASSWORD = "admin"
python .\generate_large_fleet.py
```

For scripted end-to-end tests in CI/Linux environments:
```bash
export E2E_ADMIN_USERNAME=admin
export E2E_ADMIN_PASSWORD=admin
python test_e2e.py
python test_e2e_advanced.py
```

### Staging promotion checklist
- Health endpoint stable
- KPI and inference jobs writing fresh rows
- Alerts generated and visible in UI
- No repeated errors in `docker compose logs api worker --tail=300`
- Backup job and restore test completed at least once

## Production environment
Use a dedicated host or orchestrated cluster with controlled access.

### Production baseline hardening
- Run API behind a reverse proxy (Nginx/Caddy/Traefik)
- Terminate TLS at proxy
- Do not expose DB port publicly
- Restrict inbound firewall to `80/443`
- Restrict outbound network from host/containers where possible
- Use strong secrets and rotate regularly
- Keep `CONTROL_MODE=advisory` by default

### Production rollout steps
1. Backup current DB before deployment (see Backup section).
2. Pull and checkout release:
```bash
git fetch --all --tags
git checkout <release-tag-or-commit>
```
3. Build and deploy:
```bash
docker compose --env-file .env up -d --build
```
4. Run one-time migrations for existing DBs.
5. Verify health, worker logs, and dashboard login.
6. Run post-deploy smoke checks.

### Reverse proxy + TLS (Nginx example)
Expose only proxy ports publicly and route proxy traffic to the API/dashboard service on private network/localhost.

```nginx
server {
    listen 80;
    server_name miners.example.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name miners.example.com;

    ssl_certificate /etc/letsencrypt/live/miners.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/miners.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

Notes:
- Use automated certificate renewal (`certbot renew` or equivalent).
- Keep HSTS and TLS policy aligned with your security baseline.

## Backup and restore strategy
Use both logical DB backups and filesystem backups for model/report artifacts.

### What to back up
- Database: full logical dump from `aicontroller` database
- `outputs/` directory
- Deployment configs (`.env`, compose files, proxy config)

### Backup schedule (recommended minimum)
- DB logical dump: daily
- DB dump retention: 14 daily + 8 weekly + 12 monthly
- `outputs/` snapshot: daily
- Restore drill: monthly

### Backup commands
1. Database dump:
```bash
mkdir -p backups
docker compose exec -T db pg_dump -U aicontroller -d aicontroller -Fc > backups/aicontroller_$(date +%F).dump
```
2. Outputs archive:
```bash
tar -czf backups/outputs_$(date +%F).tar.gz outputs/
```

### Restore commands
1. Restore DB dump:
```bash
cat backups/aicontroller_<date>.dump | docker compose exec -T db pg_restore -U aicontroller -d aicontroller --clean --if-exists
```
2. Restore outputs:
```bash
tar -xzf backups/outputs_<date>.tar.gz
```

Run restore only during a maintenance window or against a non-production clone first.

## Operations checks
### Health and status

macOS/Linux:
```bash
docker compose ps
curl -s http://localhost:8080/api/health
```

Windows PowerShell:
```powershell
docker compose ps
Invoke-RestMethod -Uri http://localhost:8080/api/health
```

### Logs
```bash
docker compose logs api --tail=200
docker compose logs worker --tail=200
docker compose logs db --tail=200
```

### Upgrade and rollback
- Upgrade:
```bash
git pull
docker compose up -d --build
```
- Rollback:
1. Checkout previous known-good tag/commit.
2. Re-run `docker compose up -d --build`.
3. If needed, restore DB and `outputs/` from backup.

## Security notes
- Restrict `API_SOURCE_ALLOWLIST` to known vendor domains only.
- Enforce outbound egress ACLs/proxy policy for worker containers to mitigate DNS rebinding and SSRF bypass attempts.
- Keep `AUTOMATOR_SIMULATION=true` unless you have a verified external ack path.
- Use `CONTROL_MODE=actuation` only with explicit operational approval and monitoring.
- Restrict `AUTOMATOR_ENDPOINT_ALLOWLIST` to trusted control-plane hosts only.
- Rotate admin/API secrets on a regular schedule.
