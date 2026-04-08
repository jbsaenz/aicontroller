# Production Backlog

This backlog converts the current gap analysis into executable, GitHub-ready issues with acceptance criteria and estimates.

## Planning assumptions
- Team: 2 to 4 engineers (platform + backend + ML overlap)
- Iteration: 2-week sprints
- Goal: production readiness for controlled rollout, not full enterprise re-platforming

## 90-day execution strategy
1. Days 0-30: harden reliability and security baseline.
2. Days 31-60: harden model operations and alert quality.
3. Days 61-90: improve rollout safety, automation maturity, and scale controls.

## Backlog summary
| ID | Priority | Title | Estimate | Depends On |
|---|---|---|---|---|
| PB-001 | P0 | Observability Baseline (metrics, logs, alerts) | 5-7 days | none |
| PB-002 | P0 | API Security Baseline Hardening | 4-6 days | none |
| PB-003 | P0 | Database Migration Framework | 4-6 days | none |
| PB-004 | P0 | Actuation Contract Hardening | 6-8 days | PB-002 |
| PB-005 | P0 | Performance and Capacity Validation | 4-6 days | PB-001 |
| PB-006 | P0 | Release Gates in CI/CD | 3-5 days | PB-001, PB-003 |
| PB-007 | P1 | Data Quality SLA and Quarantine Flow | 4-6 days | PB-001 |
| PB-008 | P1 | Model Drift and Performance Monitoring | 5-7 days | PB-001 |
| PB-009 | P1 | Threshold Calibration Workflow | 4-5 days | PB-008 |
| PB-010 | P1 | Worker Reliability (retries/backoff/DLQ) | 4-6 days | PB-001 |
| PB-011 | P1 | Egress Guardrails at Runtime Layer | 3-4 days | PB-002 |
| PB-012 | P2 | Safe Deployment Patterns (blue/green + rollback drill) | 4-6 days | PB-006 |
| PB-013 | P2 | Model Promotion Workflow (champion/challenger) | 5-7 days | PB-008 |
| PB-014 | P2 | Cost and Capacity Optimizations | 3-5 days | PB-005 |

## GitHub-ready issues

### PB-001
- Title: `[P0] Establish observability baseline across API, worker, and DB`
- Labels: `priority:P0`, `area:platform`, `type:enhancement`
- Estimate: `5-7d`
- Problem: failures are visible only through ad-hoc logs, making incident detection and root cause analysis slow.
- Scope:
- Add structured JSON logging with request/job correlation IDs.
- Expose service metrics (`ingest_success_rate`, `inference_latency`, `alerts_created`, `fetch_failures`, `job_duration`).
- Build baseline dashboards and alert rules.
- Acceptance criteria:
- Dashboard shows API, worker, and DB health in one place.
- Alert triggers for ingestion failure spikes and inference staleness.
- Mean time to detect failures improves to less than 10 minutes.
- Dependencies: none.

### PB-002
- Title: `[P0] Harden API security baseline for production`
- Labels: `priority:P0`, `area:security`, `type:hardening`
- Estimate: `4-6d`
- Problem: current defaults are permissive for local development and risky in production.
- Scope:
- Replace wildcard CORS with allowlisted origins.
- Enforce startup checks to reject default admin credentials and weak JWT secret.
- Add rate limiting to auth and ingestion endpoints.
- Add security headers at API/proxy layer.
- Acceptance criteria:
- Service fails fast when insecure defaults are present.
- CORS only allows configured origins.
- Brute-force login attempts are rate-limited.
- Dependencies: none.

### PB-003
- Title: `[P0] Introduce versioned DB migration framework`
- Labels: `priority:P0`, `area:data`, `type:enhancement`
- Estimate: `4-6d`
- Problem: SQL patch scripts are useful but difficult to govern as deployment count grows.
- Scope:
- Add migration toolchain and migration history table.
- Convert existing schema and policy scripts into tracked migrations.
- Add migration validation in CI.
- Acceptance criteria:
- Fresh install and upgrade install both succeed from migration pipeline.
- Schema version is queryable and auditable.
- CI fails on migration drift.
- Dependencies: none.

### PB-004
- Title: `[P0] Harden action execution contract with idempotent external ack`
- Labels: `priority:P0`, `area:worker`, `type:hardening`
- Estimate: `6-8d`
- Problem: actuation mode depends on endpoint behavior; stronger guarantees are needed before production actuation.
- Scope:
- Add `action_request_id` and idempotency semantics for outbound actuation calls.
- Require explicit ack payload contract and signature verification option.
- Persist execution attempts and final status trail.
- Acceptance criteria:
- Duplicate delivery does not trigger duplicate hardware actions.
- Alerts are resolved only on valid ack contract.
- Execution audit records are queryable for every automated action.
- Dependencies: PB-002.

### PB-005
- Title: `[P0] Validate performance envelopes for ingest and analytics`
- Labels: `priority:P0`, `area:performance`, `type:test`
- Estimate: `4-6d`
- Problem: runtime behavior under production load is not yet validated with explicit limits.
- Scope:
- Create load test scenarios for ingest, analytics, and worker loops.
- Define target SLO budgets for latency/error rates.
- Tune indexes/query paths where needed.
- Acceptance criteria:
- Published capacity report with max sustainable throughput and latency percentiles.
- P95 latency and error budgets meet agreed targets.
- Dependencies: PB-001.

### PB-006
- Title: `[P0] Add deployment quality gates to CI/CD`
- Labels: `priority:P0`, `area:devops`, `type:enhancement`
- Estimate: `3-5d`
- Problem: release readiness depends too much on manual checks.
- Scope:
- Add CI gates for tests, linting, migration validation, and image vulnerability checks.
- Add release checklist artifact generation.
- Acceptance criteria:
- Deployment pipeline blocks on failing gates.
- Release artifact includes test summary and schema version.
- Dependencies: PB-001, PB-003.

### PB-007
- Title: `[P1] Add data quality SLA enforcement and quarantine path`
- Labels: `priority:P1`, `area:data`, `type:enhancement`
- Estimate: `4-6d`
- Problem: malformed telemetry is dropped or corrected, but no explicit SLA/triage workflow exists.
- Scope:
- Define quality SLAs (null rates, parse failures, duplicate rates).
- Quarantine invalid records for operator review.
- Publish quality metrics and trends.
- Acceptance criteria:
- SLA breaches raise alerts.
- Quarantined rows are recoverable and inspectable.
- Dependencies: PB-001.

### PB-008
- Title: `[P1] Implement model drift and online performance monitoring`
- Labels: `priority:P1`, `area:ml`, `type:enhancement`
- Estimate: `5-7d`
- Problem: model health over time is not continuously measured.
- Scope:
- Add feature distribution drift checks.
- Add delayed-label evaluation pipeline for precision/recall tracking.
- Add retrain trigger recommendations based on drift/performance.
- Acceptance criteria:
- Drift score and model KPI trend visible in dashboard.
- Automated report flags retrain recommendation windows.
- Dependencies: PB-001.

### PB-009
- Title: `[P1] Build threshold calibration workflow tied to ops capacity`
- Labels: `priority:P1`, `area:ml`, `type:enhancement`
- Estimate: `4-5d`
- Problem: threshold is set from offline optimization but not tied to operational bandwidth over time.
- Scope:
- Add configurable threshold policy profiles (balanced, conservative, high-recall).
- Add periodic recalibration job using recent outcomes.
- Capture policy change history.
- Acceptance criteria:
- Threshold profile can be changed without redeploy.
- Recalibration output is versioned and auditable.
- Dependencies: PB-008.

### PB-010
- Title: `[P1] Improve worker reliability with retries, backoff, and dead-letter capture`
- Labels: `priority:P1`, `area:worker`, `type:hardening`
- Estimate: `4-6d`
- Problem: transient failures in fetch/notify/actuate paths can still cause silent gaps.
- Scope:
- Add standardized retry/backoff strategy.
- Add dead-letter table/queue for failed operations.
- Add replay tooling for dead-letter events.
- Acceptance criteria:
- Transient failures recover automatically.
- Permanent failures are captured and replayable.
- Dependencies: PB-001.

### PB-011
- Title: `[P1] Enforce runtime egress restrictions beyond URL validation`
- Labels: `priority:P1`, `area:security`, `type:hardening`
- Estimate: `3-4d`
- Problem: SSRF controls exist in app layer, but network-level egress policy is still needed.
- Scope:
- Add host/container egress ACL guidance and enforcement scripts.
- Validate DNS and resolved IP against policy on each fetch cycle.
- Acceptance criteria:
- Worker can only reach approved CIDR/domain targets.
- Policy violations are blocked and logged.
- Dependencies: PB-002.

### PB-012
- Title: `[P2] Implement blue/green deployment and rollback drill automation`
- Labels: `priority:P2`, `area:devops`, `type:enhancement`
- Estimate: `4-6d`
- Problem: rollback exists procedurally but not as a tested routine.
- Scope:
- Add blue/green deployment procedure.
- Automate rollback smoke test in staging.
- Acceptance criteria:
- Rollback can be executed in less than 15 minutes with documented steps.
- Quarterly rollback drill passes.
- Dependencies: PB-006.

### PB-013
- Title: `[P2] Add champion/challenger model promotion workflow`
- Labels: `priority:P2`, `area:ml`, `type:enhancement`
- Estimate: `5-7d`
- Problem: model replacement is currently direct; safer progressive promotion is needed.
- Scope:
- Store multiple model candidates and metadata.
- Compare challenger against champion on live shadow traffic.
- Add promote/revert controls.
- Acceptance criteria:
- Challenger evaluation report generated before promotion.
- One-click revert to previous model artifact.
- Dependencies: PB-008.

### PB-014
- Title: `[P2] Optimize infrastructure cost and capacity planning`
- Labels: `priority:P2`, `area:platform`, `type:enhancement`
- Estimate: `3-5d`
- Problem: capacity targets are set, but cost/performance optimization is not yet formalized.
- Scope:
- Profile compute/storage usage by service.
- Right-size worker schedules and retention windows.
- Produce monthly capacity and cost forecast.
- Acceptance criteria:
- Documented right-sizing recommendations with measured impact.
- Monthly cost and capacity report generated automatically.
- Dependencies: PB-005.

## Suggested milestone grouping
- Milestone `Prod Readiness 1`:
- PB-001, PB-002, PB-003, PB-004, PB-005, PB-006
- Milestone `Prod Readiness 2`:
- PB-007, PB-008, PB-009, PB-010, PB-011
- Milestone `Prod Excellence`:
- PB-012, PB-013, PB-014
