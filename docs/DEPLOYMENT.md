# Deployment Guide

This guide is the operations runbook for deploying AI Controller in `local`, `staging`, and `production` environments.
For prioritized execution tickets and sequencing, see `PRODUCTION_BACKLOG.md`.

## Deployment model
- Runtime: Docker Compose
- Services: `db` (TimescaleDB/PostgreSQL), `api` (FastAPI + dashboard), `worker` (APScheduler background jobs)
- Persistent state: `pgdata` Docker volume (database) and `./outputs` bind mount (model artifacts, metrics, reports)

## Common prerequisites
- Linux/macOS host with Docker Engine + Docker Compose v2
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
```bash
cp .env.example .env
```
3. Set required secrets and credentials in `.env`:
- `POSTGRES_PASSWORD`
- `JWT_SECRET`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `API_SOURCE_ALLOWLIST`

## Local environment
Use this for developer validation and quick smoke tests.

### Local start
1. Optional model build (recommended so worker uses ML inference):
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python src/pipeline.py --phase phase2-5
```
2. Start stack:
```bash
docker compose up -d --build
```
3. Verify:
```bash
docker compose ps
curl -s http://localhost:8080/api/health
```
4. Access dashboard at `http://localhost:8080`.

### Local reset (destructive)
```bash
docker compose down -v
rm -rf outputs/*
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
4. Validate:
```bash
docker compose ps
curl -s http://localhost:8080/api/health
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
```bash
docker compose ps
curl -s http://localhost:8080/api/health
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
- Keep `AUTOMATOR_SIMULATION=true` unless you have a verified external ack path.
- Use `CONTROL_MODE=actuation` only with explicit operational approval and monitoring.
- Rotate admin/API secrets on a regular schedule.
