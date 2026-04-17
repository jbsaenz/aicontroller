"""Regression tests for high-priority bug fixes.

These tests avoid external runtime dependencies by stubbing framework modules
and validating the exact failure paths fixed in the codebase.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {module_name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _install_sqlalchemy_stubs() -> None:
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy.text = lambda q: q
    engine_mod = types.ModuleType("sqlalchemy.engine")
    engine_mod.Engine = object
    ext_mod = types.ModuleType("sqlalchemy.ext")
    asyncio_mod = types.ModuleType("sqlalchemy.ext.asyncio")
    asyncio_mod.AsyncSession = object
    ext_mod.asyncio = asyncio_mod

    sys.modules["sqlalchemy"] = sqlalchemy
    sys.modules["sqlalchemy.engine"] = engine_mod
    sys.modules["sqlalchemy.ext"] = ext_mod
    sys.modules["sqlalchemy.ext.asyncio"] = asyncio_mod


def _install_misc_stubs() -> None:
    if "joblib" not in sys.modules:
        joblib = types.ModuleType("joblib")
        joblib.load = lambda *args, **kwargs: {}
        joblib.dump = lambda *args, **kwargs: None
        sys.modules["joblib"] = joblib

    if "httpx" not in sys.modules:
        httpx = types.ModuleType("httpx")

        class _DummyClient:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def get(self, *args, **kwargs):
                raise RuntimeError("httpx client stub should be patched in tests")

            def post(self, *args, **kwargs):
                raise RuntimeError("httpx client stub should be patched in tests")

        httpx.Client = _DummyClient
        httpx.post = lambda *args, **kwargs: None
        sys.modules["httpx"] = httpx

    if "pydantic" not in sys.modules:
        pydantic = types.ModuleType("pydantic")

        class BaseModel:
            pass

        pydantic.BaseModel = BaseModel
        sys.modules["pydantic"] = pydantic

    if "config" not in sys.modules:
        config = types.ModuleType("config")
        config.FEATURES_TELEMETRY_CSV_PATH = Path("/tmp/features.csv")
        config.FEATURES_TELEMETRY_PARQUET_PATH = Path("/tmp/features.parquet")
        config.KPI_TELEMETRY_CSV_PATH = Path("/tmp/kpi.csv")
        config.PHASE4_FEATURE_SUMMARY_PATH = Path("/tmp/feature_summary.json")
        sys.modules["config"] = config


def _install_fastapi_stubs() -> None:
    fastapi = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class APIRouter:
        def get(self, *args, **kwargs):
            def dec(fn):
                return fn

            return dec

        post = get
        delete = get
        put = get

    def Depends(dep):
        return dep

    def File(*args, **kwargs):
        return None

    class UploadFile:
        pass

    def Query(default=None, **kwargs):
        return default

    fastapi.HTTPException = HTTPException
    fastapi.APIRouter = APIRouter
    fastapi.Depends = Depends
    fastapi.File = File
    fastapi.UploadFile = UploadFile
    fastapi.Query = Query
    sys.modules["fastapi"] = fastapi


def _install_api_dependency_stubs() -> None:
    auth_mod = types.ModuleType("api.auth")
    auth_mod.verify_token = lambda: "user"
    sys.modules["api.auth"] = auth_mod

    db_mod = types.ModuleType("api.db")
    db_mod.get_db = lambda: None
    sys.modules["api.db"] = db_mod

    schemas_mod = types.ModuleType("api.schemas")
    schemas_mod.ApiSourceIn = object
    schemas_mod.ApiSourceOut = object
    schemas_mod.IngestResult = object
    sys.modules["api.schemas"] = schemas_mod


class _MappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, engine: "_FakeEngine"):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, query, params=None):
        q = str(query)
        if "SELECT key, value FROM app_settings" in q:
            return [
                ("smtp_host", "smtp.example.com"),
                ("smtp_user", "ops@example.com"),
                ("telegram_bot_token", "token"),
                ("telegram_chat_id", "chat"),
            ]
        if "FROM alerts" in q and "ORDER BY created_at DESC" in q:
            return _MappingsResult(
                [
                    {
                        "id": 7,
                        "miner_id": "m-01",
                        "severity": "warning",
                        "risk_score": 0.61,
                        "message": "test",
                        "email_sent": True,
                        "telegram_sent": False,
                    }
                ]
            )
        if "UPDATE alerts" in q:
            self.engine.updates.append(params or {})
            return []
        if "FROM app_settings" in q and "WHERE key = ANY" in q:
            return [
                ("risk_threshold", "0.77"),
                ("alert_cooldown_hours", "3"),
                ("inference_lookback_hours", "48"),
                ("cooling_power_ratio", "0.31"),
                ("retrain_days", "45"),
                ("control_mode", "actuation"),
                ("policy_optimizer_enabled", "true"),
                ("automation_require_policy_backtest", "true"),
                ("policy_min_uplift_usd_per_miner", "0.05"),
                ("energy_price_usd_per_kwh", "0.12"),
                ("hashprice_usd_per_ph_day", "60"),
                ("opex_usd_per_mwh", "9"),
                ("capex_usd_per_mwh", "18"),
                ("energy_price_schedule_json", '{"17": 0.2}'),
                ("curtailment_windows_json", '[{"start_hour":17,"end_hour":21}]'),
                ("policy_reward_per_th_hour_usd", "0.06"),
                ("policy_failure_cost_usd", "130"),
                ("policy_horizon_hours", "1.0"),
                ("risk_probability_horizon_hours", "24"),
                ("policy_timezone", "UTC"),
                ("curtailment_penalty_multiplier", "1.5"),
            ]
        return []


class _FakeEngine:
    def __init__(self):
        self.updates = []

    def connect(self):
        return _FakeConn(self)

    def begin(self):
        return _FakeConn(self)


class RegressionTests(unittest.TestCase):
    def setUp(self):
        _install_sqlalchemy_stubs()
        _install_misc_stubs()

    def test_alert_routes_are_typed_with_alert_out(self):
        alerts_py = (ROOT / "api/routers/alerts.py").read_text(encoding="utf-8")
        self.assertIn('@router.get("/alerts", response_model=list[AlertOut])', alerts_py)
        self.assertIn(
            '@router.get("/alerts/history", response_model=list[AlertOut])',
            alerts_py,
        )

    def test_alert_history_supports_offset_pagination(self):
        alerts_py = (ROOT / "api/routers/alerts.py").read_text(encoding="utf-8")
        self.assertIn("offset: int = Query(0, ge=0)", alerts_py)
        self.assertIn("LIMIT :lim OFFSET :off", alerts_py)
        self.assertIn('"off": offset', alerts_py)

    def test_ingest_preserves_event_codes_as_text(self):
        _install_fastapi_stubs()
        _install_api_dependency_stubs()

        ingest = _load_module(
            "test_api_routers_ingest", ROOT / "api/routers/ingest.py"
        )
        df = pd.DataFrame(
            [
                {
                    "timestamp": "2026-04-06T12:00:00Z",
                    "miner_id": "m-01",
                    "asic_power_w": 3200,
                    "event_codes": '["OVERTEMP","HASH_DROP"]',
                }
            ]
        )

        cleaned, errors = ingest._validate_and_clean(df)
        self.assertEqual(errors, [])
        self.assertEqual(
            cleaned.iloc[0]["event_codes"], '["OVERTEMP","HASH_DROP"]'
        )

    def test_ingest_null_text_fields_are_normalized(self):
        _install_fastapi_stubs()
        _install_api_dependency_stubs()

        ingest = _load_module(
            "test_api_routers_ingest_nulls", ROOT / "api/routers/ingest.py"
        )
        df = pd.DataFrame(
            [
                {
                    "timestamp": "2026-04-06T12:00:00Z",
                    "miner_id": "m-01",
                    "operating_mode": float("nan"),
                    "event_codes": float("nan"),
                }
            ]
        )

        cleaned, errors = ingest._validate_and_clean(df)
        self.assertEqual(errors, [])
        self.assertIsNone(cleaned.iloc[0]["operating_mode"])
        self.assertIsNone(cleaned.iloc[0]["event_codes"])

    def test_ingest_drops_rows_with_missing_miner_id(self):
        _install_fastapi_stubs()
        _install_api_dependency_stubs()

        ingest = _load_module(
            "test_api_routers_ingest_missing_mid", ROOT / "api/routers/ingest.py"
        )
        df = pd.DataFrame(
            [
                {"timestamp": "2026-04-06T12:00:00Z", "miner_id": ""},
                {"timestamp": "2026-04-06T12:05:00Z", "miner_id": "m-02"},
            ]
        )

        cleaned, errors = ingest._validate_and_clean(df)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned.iloc[0]["miner_id"], "m-02")
        self.assertTrue(any("missing miner_id" in err for err in errors))

    def test_ingest_payload_uses_json_null_for_text_nans(self):
        _install_fastapi_stubs()
        _install_api_dependency_stubs()

        ingest = _load_module(
            "test_api_routers_ingest_payload", ROOT / "api/routers/ingest.py"
        )
        df = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2026-04-06T12:00:00Z"),
                    "miner_id": "m-01",
                    "asic_voltage_v": float("nan"),
                    "event_codes": None,
                    "operating_mode": "normal",
                }
            ]
        )

        payload_raw = ingest._build_ingest_payload(df)
        payload = json.loads(payload_raw)
        self.assertEqual(payload[0]["miner_id"], "m-01")
        self.assertIsNone(payload[0]["event_codes"])
        self.assertIsNone(payload[0]["asic_voltage_v"])
        self.assertTrue(payload[0]["timestamp"].startswith("2026-04-06T12:00:00"))
        self.assertNotIn("NaN", payload_raw)

    def test_analytics_tradeoffs_no_random_ordering(self):
        analytics_py = (ROOT / "api/routers/analytics.py").read_text(encoding="utf-8")
        self.assertNotIn("ORDER BY RANDOM()", analytics_py)
        self.assertIn("kpi_hourly_rollup", analytics_py)

    def test_analytics_anomaly_query_uses_kpi_telemetry(self):
        analytics_py = (ROOT / "api/routers/analytics.py").read_text(encoding="utf-8")
        self.assertIn("FROM kpi_telemetry", analytics_py)
        self.assertNotIn("FROM telemetry\n            WHERE timestamp >= :start_dt AND timestamp <= :end_dt", analytics_py)

    def test_timescale_policies_are_declared(self):
        init_sql = (ROOT / "docker/db/init.sql").read_text(encoding="utf-8")
        rollout_sql = (ROOT / "scripts/apply_analytics_rollup.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("add_continuous_aggregate_policy", init_sql)
        self.assertIn("add_retention_policy", init_sql)
        self.assertIn("add_continuous_aggregate_policy", rollout_sql)
        self.assertIn("add_retention_policy", rollout_sql)

    def test_compose_binds_outputs_into_api_and_worker(self):
        compose_yml = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("- ./outputs:/app/outputs", compose_yml)
        self.assertNotIn("- artifacts:/app/outputs", compose_yml)
        self.assertIn("CONTROL_MODE", compose_yml)
        self.assertIn("INFERENCE_LOOKBACK_HOURS", compose_yml)
        self.assertIn("MAX_INGEST_FILE_BYTES", compose_yml)
        self.assertIn("MAX_INGEST_ROWS", compose_yml)

    def test_compose_does_not_forward_admin_password_plaintext(self):
        compose_yml = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertNotIn("ADMIN_PASSWORD:", compose_yml)
        self.assertIn("ADMIN_PASSWORD_HASH", compose_yml)
        self.assertIn("APP_SETTINGS_ENCRYPTION_KEY", compose_yml)

    def test_retrain_passes_worker_model_path_to_training(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("model_artifact_path=MODEL_PATH", ml_jobs_py)

    def test_kpi_job_uses_batched_fetching_guardrails(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("KPI_JOB_BATCH_SIZE", ml_jobs_py)
        self.assertIn("KPI_JOB_MAX_ROWS_PER_RUN", ml_jobs_py)
        self.assertIn("WHERE t.id > :last_seen_id", ml_jobs_py)
        self.assertIn("ORDER BY t.id ASC", ml_jobs_py)
        self.assertIn("while rows_scanned < KPI_JOB_MAX_ROWS_PER_RUN", ml_jobs_py)

    def test_kpi_job_sorts_rows_by_miner_and_timestamp_before_rolling(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn('sort_values(["miner_id", "timestamp"]', ml_jobs_py)
        self.assertIn("window=POWER_INSTABILITY_WINDOW", ml_jobs_py)

    def test_retrain_checks_min_rows_miner_diversity_and_timespan(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("RETRAIN_MIN_ROWS", ml_jobs_py)
        self.assertIn("RETRAIN_MIN_MINERS", ml_jobs_py)
        self.assertIn("RETRAIN_MIN_TIMESPAN_HOURS", ml_jobs_py)
        self.assertIn("not enough miner diversity", ml_jobs_py)
        self.assertIn("insufficient time span", ml_jobs_py)

    def test_worker_main_gates_automator_by_control_mode(self):
        worker_main_py = (ROOT / "worker/main.py").read_text(encoding="utf-8")
        self.assertIn("scheduler.add_job(job_automator", worker_main_py)
        self.assertIn("Automator scheduled; execution is gated at runtime", worker_main_py)

    def test_worker_wait_for_db_ready_reuses_single_engine(self):
        worker_main_py = (ROOT / "worker/main.py").read_text(encoding="utf-8")
        self.assertIn("engine = _get_engine()", worker_main_py)
        self.assertIn("with engine.connect() as conn:", worker_main_py)
        self.assertNotIn("with _get_engine().connect() as conn:", worker_main_py)

    def test_worker_engine_has_bounded_pool_overflow(self):
        worker_main_py = (ROOT / "worker/main.py").read_text(encoding="utf-8")
        compose_yml = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("DB_POOL_SIZE", worker_main_py)
        self.assertIn("DB_MAX_OVERFLOW", worker_main_py)
        self.assertIn("pool_size=DB_POOL_SIZE", worker_main_py)
        self.assertIn("max_overflow=DB_MAX_OVERFLOW", worker_main_py)
        self.assertIn("DB_POOL_SIZE:", compose_yml)
        self.assertIn("DB_MAX_OVERFLOW:", compose_yml)
        self.assertIn("DB_POOL_SIZE=5", env_example)
        self.assertIn("DB_MAX_OVERFLOW=5", env_example)

    def test_schemas_use_pydantic_v2_default_factories(self):
        schemas_py = (ROOT / "api/schemas.py").read_text(encoding="utf-8")
        self.assertIn("Field(default_factory=list)", schemas_py)
        self.assertIn("Field(default_factory=dict)", schemas_py)
        self.assertIn("model_config = ConfigDict(from_attributes=True)", schemas_py)

    def test_url_safety_blocks_private_addresses(self):
        url_safety = _load_module("test_src_url_safety", ROOT / "src/url_safety.py")
        with mock.patch.object(
            url_safety.socket,
            "getaddrinfo",
            return_value=[(None, None, None, None, ("127.0.0.1", 80))],
        ):
            with self.assertRaises(url_safety.UnsafeURLError):
                url_safety.validate_source_url("http://example.com/api")

    def test_url_inspection_reports_allowlist_and_dns_diagnostics(self):
        url_safety = _load_module(
            "test_src_url_safety_inspect", ROOT / "src/url_safety.py"
        )

        with mock.patch.dict(os.environ, {"API_SOURCE_ALLOWLIST": "example.com"}):
            with mock.patch.object(
                url_safety.socket,
                "getaddrinfo",
                return_value=[(None, None, None, None, ("93.184.216.34", 443))],
            ):
                report = url_safety.inspect_source_url("https://example.com/health")

        self.assertTrue(report["valid"])
        self.assertEqual(report["hostname"], "example.com")
        self.assertEqual(report["allowlist"], ["example.com"])
        self.assertEqual(report["resolved_ips"], ["93.184.216.34"])
        self.assertEqual(report["errors"], [])

    def test_url_safety_rejects_global_wildcard_allowlist(self):
        url_safety = _load_module(
            "test_src_url_safety_wildcard", ROOT / "src/url_safety.py"
        )

        with mock.patch.dict(os.environ, {"API_SOURCE_ALLOWLIST": "*"}):
            with mock.patch.object(
                url_safety.socket,
                "getaddrinfo",
                return_value=[(None, None, None, None, ("93.184.216.34", 443))],
            ):
                report = url_safety.inspect_source_url("https://example.com/health")

        self.assertFalse(report["valid"])
        self.assertIn("API_SOURCE_ALLOWLIST cannot include wildcard '*'", report["errors"])

    def test_ingest_router_exposes_source_validation_endpoints(self):
        ingest_py = (ROOT / "api/routers/ingest.py").read_text(encoding="utf-8")
        self.assertIn('"/ingest/sources/allowlist"', ingest_py)
        self.assertIn('"/ingest/sources/validate-url"', ingest_py)

    def test_ingest_router_has_payload_guardrails(self):
        ingest_py = (ROOT / "api/routers/ingest.py").read_text(encoding="utf-8")
        self.assertIn("MAX_INGEST_FILE_BYTES", ingest_py)
        self.assertIn("MAX_INGEST_ROWS", ingest_py)
        self.assertIn("CSV payload too large", ingest_py)
        self.assertIn("CSV has too many rows", ingest_py)

    def test_ingest_insert_sql_is_static_not_fstring(self):
        ingest_py = (ROOT / "api/routers/ingest.py").read_text(encoding="utf-8")
        self.assertIn("INGEST_JSON_INSERT_SQL = text(", ingest_py)
        self.assertNotIn('INGEST_JSON_INSERT_SQL = text(f"""', ingest_py)

    def test_ingest_source_mutations_return_404_on_missing_source(self):
        ingest_py = (ROOT / "api/routers/ingest.py").read_text(encoding="utf-8")
        self.assertIn("status_code=404, detail=\"Source not found\"", ingest_py)
        self.assertIn("DELETE FROM api_sources WHERE id = :id RETURNING id", ingest_py)
        self.assertIn("UPDATE api_sources SET enabled = NOT enabled", ingest_py)

    def test_automator_keeps_alert_active_without_ack(self):
        automator = _load_module("test_worker_automator", ROOT / "worker/automator.py")

        class Engine:
            def __init__(self):
                self.updates = []

            def connect(self):
                return self

            def begin(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                q = str(query)
                if "FROM alerts" in q:
                    return _MappingsResult(
                        [{"id": 12, "miner_id": "m-10", "recommended_action": "REBOOT"}]
                    )
                if "UPDATE alerts" in q:
                    self.updates.append(params)
                return _MappingsResult([])

        engine = Engine()
        with mock.patch.object(automator, "_execute_action", return_value=(True, False)):
            with mock.patch.dict(
                os.environ,
                {"AUTOMATOR_SIMULATION": "false", "CONTROL_MODE": "actuation"},
            ):
                automator.run_automator_job(engine)
        self.assertEqual(engine.updates, [])

    def test_automator_simulation_mode_resolves_alert(self):
        automator = _load_module(
            "test_worker_automator_sim", ROOT / "worker/automator.py"
        )

        class Engine:
            def __init__(self):
                self.updates = []

            def connect(self):
                return self

            def begin(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                q = str(query)
                if "FROM alerts" in q:
                    return _MappingsResult(
                        [{"id": 13, "miner_id": "m-11", "recommended_action": "DOWNCLOCK"}]
                    )
                if "UPDATE alerts" in q:
                    self.updates.append(params)
                return _MappingsResult([])

        engine = Engine()
        with mock.patch.dict(
            os.environ,
            {"AUTOMATOR_SIMULATION": "true", "CONTROL_MODE": "actuation"},
        ):
            automator.run_automator_job(engine)
        self.assertEqual(len(engine.updates), 1)

    def test_automator_advisory_mode_skips_actions(self):
        automator = _load_module(
            "test_worker_automator_advisory", ROOT / "worker/automator.py"
        )

        class Engine:
            def __init__(self):
                self.updates = []

            def connect(self):
                return self

            def begin(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                q = str(query)
                if "FROM alerts" in q:
                    return _MappingsResult(
                        [{"id": 14, "miner_id": "m-12", "recommended_action": "REBOOT"}]
                    )
                if "UPDATE alerts" in q:
                    self.updates.append(params)
                return _MappingsResult([])

        engine = Engine()
        with mock.patch.dict(
            os.environ,
            {"AUTOMATOR_SIMULATION": "true", "CONTROL_MODE": "advisory"},
        ):
            automator.run_automator_job(engine)
        self.assertEqual(engine.updates, [])

    def test_automator_control_mode_db_override(self):
        automator = _load_module(
            "test_worker_automator_mode_override", ROOT / "worker/automator.py"
        )

        class Engine:
            def __init__(self):
                self.updates = []

            def connect(self):
                return self

            def begin(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                q = str(query)
                if "FROM app_settings" in q and "control_mode" in q:
                    return _MappingsResult([{"value": "advisory"}])
                if "FROM alerts" in q:
                    return _MappingsResult(
                        [{"id": 15, "miner_id": "m-13", "recommended_action": "REBOOT"}]
                    )
                if "UPDATE alerts" in q:
                    self.updates.append(params)
                return _MappingsResult([])

        engine = Engine()
        with mock.patch.dict(
            os.environ,
            {"AUTOMATOR_SIMULATION": "true", "CONTROL_MODE": "actuation"},
        ):
            automator.run_automator_job(engine)
        self.assertEqual(engine.updates, [])

    def test_fetcher_skips_rows_missing_miner_or_timestamp(self):
        fetcher = _load_module("test_worker_fetcher", ROOT / "worker/fetcher.py")

        class _Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return [
                    {"miner_id": "  ", "timestamp": "2026-04-07T12:00:00Z"},
                    {"miner_id": "m-20", "timestamp": "bad-ts"},
                    {
                        "miner_id": "m-21",
                        "timestamp": "2026-04-07T12:05:00Z",
                        "event_codes": "  ",
                        "operating_mode": " normal ",
                    },
                ]

        class _Client:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def get(self, url, headers=None):
                return _Resp()

        class _Conn:
            def __init__(self, engine):
                self.engine = engine

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                q = str(query)
                if "INSERT INTO telemetry" in q:
                    self.engine.inserts.append(params)
                return []

        class Engine:
            def __init__(self):
                self.inserts = []

            def begin(self):
                return _Conn(self)

        engine = Engine()
        source = {
            "name": "mock-api",
            "url_template": "https://example.com/metrics",
            "auth_headers": {},
            "field_mapping": {},
        }

        with mock.patch.object(
            fetcher,
            "inspect_source_url",
            return_value={"valid": True, "errors": [], "resolved_ips": ["93.184.216.34"]},
        ):
            with mock.patch.object(fetcher.httpx, "Client", _Client):
                fetcher._fetch_and_store(engine, source)

        self.assertEqual(len(engine.inserts), 1)
        inserted_records = engine.inserts[0]
        self.assertEqual(len(inserted_records), 1)
        self.assertEqual(inserted_records[0]["miner_id"], "m-21")
        self.assertEqual(inserted_records[0]["operating_mode"], "normal")
        self.assertIsNone(inserted_records[0]["event_codes"])

    def test_fetcher_uses_db_backoff_and_auto_disables_persistent_failures(self):
        with mock.patch.dict(
            os.environ,
            {
                "SOURCE_FETCH_FAILURE_DISABLE_THRESHOLD": "3",
                "SOURCE_FETCH_BACKOFF_MAX_MULTIPLIER": "8",
            },
        ):
            fetcher = _load_module(
                "test_worker_fetcher_backoff", ROOT / "worker/fetcher.py"
            )

        source_state = {
            "id": 42,
            "name": "persistently-bad-source",
            "url_template": "https://example.com/metrics",
            "auth_headers": {},
            "field_mapping": {},
            "polling_interval_minutes": 1,
            "last_fetched_at": None,
            "fetch_failure_streak": 0,
            "last_fetch_attempt_at": None,
            "enabled": True,
        }

        class _Conn:
            def __init__(self, engine):
                self.engine = engine

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                q = str(query)
                if "ALTER TABLE api_sources" in q:
                    return []
                if "FROM api_sources" in q:
                    if source_state["enabled"]:
                        return _MappingsResult([dict(source_state)])
                    return _MappingsResult([])
                if "SET fetch_failure_streak = :streak" in q:
                    source_state["fetch_failure_streak"] = int(params["streak"])
                    source_state["last_fetch_attempt_at"] = datetime.now(timezone.utc)
                    self.engine.failure_updates.append(int(params["id"]))
                    return []
                if "SET enabled = FALSE" in q and "fetch_failure_streak = 0" in q:
                    source_state["enabled"] = False
                    source_state["fetch_failure_streak"] = 0
                    source_state["last_fetch_attempt_at"] = datetime.now(timezone.utc)
                    self.engine.disabled_ids.append(int(params["id"]))
                    return []
                if "SET last_fetched_at = NOW()" in q and "fetch_failure_streak = 0" in q:
                    source_state["last_fetched_at"] = datetime.now(timezone.utc)
                    source_state["last_fetch_attempt_at"] = datetime.now(timezone.utc)
                    source_state["fetch_failure_streak"] = 0
                    self.engine.success_updates.append(int(params["id"]))
                    return []
                if "SET fetch_failure_streak = 0" in q:
                    source_state["fetch_failure_streak"] = 0
                    source_state["last_fetch_attempt_at"] = None
                    return []
                return []

        class Engine:
            def __init__(self):
                self.disabled_ids = []
                self.success_updates = []
                self.failure_updates = []

            def connect(self):
                return _Conn(self)

            def begin(self):
                return _Conn(self)

        engine = Engine()

        with mock.patch.object(
            fetcher, "_fetch_and_store", side_effect=RuntimeError("upstream timeout")
        ) as failing_fetch:
            # First attempt fails and starts streak=1.
            fetcher.run_fetch_job(engine)
            self.assertEqual(failing_fetch.call_count, 1)
            self.assertEqual(source_state["fetch_failure_streak"], 1)

            # Immediate rerun should back off and not attempt again.
            fetcher.run_fetch_job(engine)
            self.assertEqual(failing_fetch.call_count, 1)
            self.assertEqual(source_state["fetch_failure_streak"], 1)

            # Force the backoff window to pass and fail two more times.
            source_state["last_fetch_attempt_at"] = datetime.now(timezone.utc) - timedelta(
                minutes=120
            )
            fetcher.run_fetch_job(engine)
            self.assertEqual(failing_fetch.call_count, 2)
            self.assertEqual(source_state["fetch_failure_streak"], 2)

            source_state["last_fetch_attempt_at"] = datetime.now(timezone.utc) - timedelta(
                minutes=120
            )
            fetcher.run_fetch_job(engine)
            self.assertEqual(failing_fetch.call_count, 3)

        self.assertEqual(engine.success_updates, [])
        self.assertEqual(engine.failure_updates, [42, 42])
        self.assertEqual(engine.disabled_ids, [42])
        self.assertFalse(source_state["enabled"])
        self.assertEqual(source_state["fetch_failure_streak"], 0)

    def test_inference_feature_prep_adds_missing_columns(self):
        ml_jobs = _load_module("test_worker_ml_jobs", ROOT / "worker/ml_jobs.py")
        src = pd.DataFrame([{"asic_temperature_c": 78.4}])
        out = ml_jobs._prepare_inference_features(
            src, ["asic_temperature_c", "temp_roll_mean_1h", "operating_mode"]
        )

        self.assertEqual(
            list(out.columns),
            ["asic_temperature_c", "temp_roll_mean_1h", "operating_mode"],
        )
        self.assertEqual(out.iloc[0]["asic_temperature_c"], 78.4)
        self.assertTrue(pd.isna(out.iloc[0]["temp_roll_mean_1h"]))
        self.assertTrue(pd.isna(out.iloc[0]["operating_mode"]))
        self.assertTrue(str(ml_jobs.MODEL_PATH).endswith("phase4_best_model.joblib"))

    def test_serving_feature_snapshot_builds_latest_rows(self):
        feature_engineering = _load_module(
            "test_src_feature_engineering_serving", ROOT / "src/feature_engineering.py"
        )
        df = pd.DataFrame(
            [
                {
                    "timestamp": "2026-04-07T12:00:00Z",
                    "miner_id": "m-01",
                    "operating_mode": "normal",
                    "ambient_temperature_c": 29.0,
                    "asic_clock_mhz": 620,
                    "asic_voltage_v": 13.0,
                    "asic_hashrate_ths": 105.0,
                    "asic_temperature_c": 79.0,
                    "asic_power_w": 3100.0,
                    "efficiency_j_per_th": 29.5,
                    "power_instability_index": 0.10,
                    "hashrate_deviation_pct": -2.0,
                    "true_efficiency_te": 0.03,
                    "failure_within_horizon": 0,
                },
                {
                    "timestamp": "2026-04-07T12:10:00Z",
                    "miner_id": "m-01",
                    "operating_mode": "normal",
                    "ambient_temperature_c": 30.0,
                    "asic_clock_mhz": 620,
                    "asic_voltage_v": 13.0,
                    "asic_hashrate_ths": 102.0,
                    "asic_temperature_c": 82.0,
                    "asic_power_w": 3120.0,
                    "efficiency_j_per_th": 30.6,
                    "power_instability_index": 0.20,
                    "hashrate_deviation_pct": -4.0,
                    "true_efficiency_te": 0.028,
                    "failure_within_horizon": 1,
                },
            ]
        )
        snapshot = feature_engineering.build_serving_feature_snapshot(df)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot.iloc[0]["miner_id"], "m-01")
        self.assertIn("cooling_power_w", snapshot.columns)
        self.assertFalse(pd.isna(snapshot.iloc[0]["cooling_power_w"]))
        self.assertIn("temp_roll_mean_1h", snapshot.columns)

    def test_kpi_insert_record_normalization_handles_nan_fields(self):
        ml_jobs = _load_module("test_worker_ml_jobs_norm", ROOT / "worker/ml_jobs.py")
        df = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2026-04-07T12:00:00Z"),
                    "miner_id": "m-01",
                    "operating_mode": float("nan"),
                    "bad_hash_count": float("nan"),
                    "double_hash_count": "5",
                    "read_errors": "oops",
                    "event_codes": float("nan"),
                    "failure_within_horizon": 0.0,
                    "asic_power_w": 3200.0,
                }
            ]
        )
        insert_cols = [
            "timestamp",
            "miner_id",
            "operating_mode",
            "bad_hash_count",
            "double_hash_count",
            "read_errors",
            "event_codes",
            "failure_within_horizon",
            "asic_power_w",
        ]
        records = ml_jobs._build_kpi_insert_records(df, insert_cols)
        self.assertEqual(len(records), 1)
        row = records[0]
        self.assertIsInstance(row["timestamp"], datetime)
        self.assertEqual(row["miner_id"], "m-01")
        self.assertIsNone(row["operating_mode"])
        self.assertIsNone(row["bad_hash_count"])
        self.assertEqual(row["double_hash_count"], 5)
        self.assertIsNone(row["read_errors"])
        self.assertIsNone(row["event_codes"])
        self.assertEqual(row["failure_within_horizon"], 0)

    def test_e2e_scripts_fail_fast_on_worker_errors(self):
        e2e_py = (ROOT / "test_e2e.py").read_text(encoding="utf-8")
        e2e_adv_py = (ROOT / "test_e2e_advanced.py").read_text(encoding="utf-8")
        self.assertIn("assert result.returncode == 0", e2e_py)
        self.assertIn("if result.returncode != 0:", e2e_adv_py)
        self.assertIn("miner_alerts = [a for a in alerts if a[\"miner_id\"] == miner_id]", e2e_adv_py)
        self.assertIn("/api/miners/", e2e_py)
        self.assertIn("/api/miners/", e2e_adv_py)

    def test_e2e_scripts_use_env_credentials_and_cookie_sessions(self):
        e2e_py = (ROOT / "test_e2e.py").read_text(encoding="utf-8")
        e2e_adv_py = (ROOT / "test_e2e_advanced.py").read_text(encoding="utf-8")
        self.assertIn("E2E_ADMIN_USERNAME", e2e_py)
        self.assertIn("E2E_ADMIN_PASSWORD", e2e_py)
        self.assertIn("E2E_ADMIN_USERNAME", e2e_adv_py)
        self.assertIn("E2E_ADMIN_PASSWORD", e2e_adv_py)
        self.assertIn("requests.Session()", e2e_py)
        self.assertIn("requests.Session()", e2e_adv_py)
        self.assertNotIn("password12345", e2e_py)
        self.assertNotIn("password12345", e2e_adv_py)
        self.assertNotIn("\"Authorization\"", e2e_py)
        self.assertNotIn("\"Authorization\"", e2e_adv_py)

    def test_support_scripts_do_not_embed_passwords_or_bearer_headers(self):
        large_fleet_py = (ROOT / "generate_large_fleet.py").read_text(encoding="utf-8")
        db_insert_py = (ROOT / "test_db_insert.py").read_text(encoding="utf-8")
        self.assertIn("E2E_ADMIN_USERNAME", large_fleet_py)
        self.assertIn("E2E_ADMIN_PASSWORD", large_fleet_py)
        self.assertIn("requests.Session()", large_fleet_py)
        self.assertNotIn("\"Authorization\"", large_fleet_py)
        self.assertNotIn("password12345", large_fleet_py)
        self.assertIn("DATABASE_URL_SYNC", db_insert_py)
        self.assertNotIn("password12345", db_insert_py)

    def test_inference_uses_lookback_serving_snapshot(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("build_serving_feature_snapshot", ml_jobs_py)
        self.assertIn("INFERENCE_LOOKBACK_HOURS", ml_jobs_py)
        self.assertIn(
            "WHERE timestamp >= NOW() - (CAST(:lookback_hours AS text) || ' hours')::interval",
            ml_jobs_py,
        )
        self.assertNotIn(
            "SELECT DISTINCT ON (miner_id)\n                miner_id, timestamp",
            ml_jobs_py,
        )

    def test_ml_jobs_heuristic_latest_row_query_uses_lateral(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("FROM miners m", ml_jobs_py)
        self.assertIn("JOIN LATERAL", ml_jobs_py)
        self.assertNotIn("SELECT DISTINCT ON (miner_id)", ml_jobs_py)

    def test_runtime_config_reads_db_threshold_and_cooldown(self):
        ml_jobs = _load_module("test_worker_ml_jobs_cfg", ROOT / "worker/ml_jobs.py")
        cfg = ml_jobs._load_runtime_config(_FakeEngine())
        self.assertEqual(cfg["risk_threshold"], 0.77)
        self.assertEqual(cfg["alert_cooldown_hours"], 3)
        self.assertEqual(cfg["retrain_days"], 45)
        self.assertTrue(cfg["policy"]["optimizer_enabled"])
        self.assertTrue(cfg["policy"]["require_backtest_for_automation"])
        self.assertEqual(cfg["policy"]["energy_price_schedule"][17], 0.2)
        self.assertEqual(cfg["policy"]["curtailment_windows"][0], (17, 21))
        self.assertEqual(cfg["policy"]["hashprice_usd_per_ph_day"], 60.0)
        self.assertEqual(cfg["policy"]["opex_usd_per_mwh"], 9.0)
        self.assertEqual(cfg["policy"]["capex_usd_per_mwh"], 18.0)
        self.assertEqual(cfg["policy"]["risk_probability_horizon_hours"], 24.0)
        self.assertEqual(cfg["control_mode"], "actuation")
        self.assertEqual(cfg["inference_lookback_hours"], 48)
        self.assertAlmostEqual(cfg["cooling_power_ratio"], 0.31, places=6)

    def test_runtime_config_query_uses_bound_array_keys(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("WHERE key = ANY(CAST(:keys AS text[]))", ml_jobs_py)
        self.assertNotIn("RUNTIME_SETTINGS_SQL_IN", ml_jobs_py)

    def test_auth_uses_rate_limit_and_clears_legacy_password(self):
        auth_py = (ROOT / "api/auth.py").read_text(encoding="utf-8")
        self.assertIn("AUTH_LOGIN_RATE_LIMIT", auth_py)
        self.assertIn("@limiter.limit(AUTH_LOGIN_RATE_LIMIT)", auth_py)
        self.assertIn('os.environ.pop("ADMIN_PASSWORD", None)', auth_py)

    def test_auth_verify_token_uses_cookie_channel_only(self):
        auth_py = (ROOT / "api/auth.py").read_text(encoding="utf-8")
        self.assertIn("request.cookies.get(AUTH_COOKIE_NAME)", auth_py)
        self.assertNotIn("HTTPBearer", auth_py)
        self.assertNotIn("credentials.credentials if credentials", auth_py)

    def test_auth_login_response_is_cookie_only_without_jwt_body(self):
        auth_py = (ROOT / "api/auth.py").read_text(encoding="utf-8")
        self.assertIn("class LoginResponse", auth_py)
        self.assertIn('@router.post("/auth/login", response_model=LoginResponse)', auth_py)
        self.assertNotIn("class TokenResponse", auth_py)
        self.assertNotIn("access_token", auth_py)
        self.assertNotIn("token_type", auth_py)

    def test_automator_validates_endpoint_urls(self):
        automator_py = (ROOT / "worker/automator.py").read_text(encoding="utf-8")
        self.assertIn("validate_automator_url", automator_py)
        self.assertIn("endpoint blocked by URL safety policy", automator_py)

    def test_automator_has_remote_failure_backoff_and_circuit_breaker(self):
        automator_py = (ROOT / "worker/automator.py").read_text(encoding="utf-8")
        self.assertIn("AUTOMATOR_FAILURE_THRESHOLD", automator_py)
        self.assertIn("AUTOMATOR_BACKOFF_BASE_SECONDS", automator_py)
        self.assertIn("AUTOMATOR_BACKOFF_MAX_SECONDS", automator_py)
        self.assertIn("AUTOMATOR_CIRCUIT_OPEN_SECONDS", automator_py)
        self.assertIn("AUTOMATOR_FAILURE_STREAK_KEY", automator_py)
        self.assertIn("AUTOMATOR_CIRCUIT_OPEN_UNTIL_KEY", automator_py)
        self.assertIn("AUTOMATOR_LAST_FAILURE_REASON_KEY", automator_py)
        self.assertIn("_compute_backoff_seconds", automator_py)
        self.assertIn("_read_automator_state", automator_py)
        self.assertIn("_write_automator_state", automator_py)
        self.assertIn("INSERT INTO app_settings (key, value, updated_at)", automator_py)
        self.assertIn("WHERE key = ANY(CAST(:keys AS text[]))", automator_py)
        self.assertIn("_record_remote_failure", automator_py)
        self.assertIn("_remote_circuit_status", automator_py)
        self.assertIn("Automator skipped: circuit open", automator_py)
        self.assertIn("Automator pausing run: circuit open", automator_py)

    def test_notifier_telegram_uses_html_parse_mode(self):
        notifier_py = (ROOT / "worker/notifier.py").read_text(encoding="utf-8")
        self.assertIn('"parse_mode": "HTML"', notifier_py)
        self.assertNotIn('"parse_mode": "Markdown"', notifier_py)

    def test_notifier_avoids_raw_secret_dict_key_access(self):
        notifier_py = (ROOT / "worker/notifier.py").read_text(encoding="utf-8")
        self.assertNotIn('cfg["smtp_user"]', notifier_py)
        self.assertNotIn('cfg["smtp_password"]', notifier_py)
        self.assertNotIn('cfg["telegram_bot_token"]', notifier_py)
        self.assertIn("decrypt_if_needed", notifier_py)

    def test_settings_router_encrypts_sensitive_values_at_rest(self):
        settings_py = (ROOT / "api/routers/settings.py").read_text(encoding="utf-8")
        self.assertIn("SENSITIVE_TEXT_KEYS", settings_py)
        self.assertIn("SECRET_MASK", settings_py)
        self.assertIn("_mask_secret_value", settings_py)
        self.assertIn("encrypt_if_needed", settings_py)
        self.assertNotIn("decrypt_if_needed", settings_py)

    def test_secret_store_requires_dedicated_encryption_key(self):
        secret_store_py = (ROOT / "src/secret_store.py").read_text(encoding="utf-8")
        self.assertIn("ENCRYPTION_KEY_ENV = \"APP_SETTINGS_ENCRYPTION_KEY\"", secret_store_py)
        self.assertIn("_require_configured_key", secret_store_py)
        self.assertIn("is unset; secret setting encryption/decryption is disabled until configured", secret_store_py)
        self.assertNotIn("JWT_SECRET", secret_store_py)
        self.assertNotIn("aicontroller-settings-seed", secret_store_py)
        self.assertNotIn("_xor_payload", secret_store_py)

    def test_shared_truthy_helper_replaces_module_duplicates(self):
        automator_py = (ROOT / "worker/automator.py").read_text(encoding="utf-8")
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        policy_py = (ROOT / "src/policy.py").read_text(encoding="utf-8")
        self.assertIn("from src.runtime_utils import is_truthy", automator_py)
        self.assertIn("from src.runtime_utils import is_truthy", ml_jobs_py)
        self.assertIn("from src.runtime_utils import is_truthy", policy_py)
        self.assertNotIn("def _is_truthy", automator_py)
        self.assertNotIn("def _is_truthy", ml_jobs_py)
        self.assertNotIn("def _is_truthy", policy_py)

    def test_serving_snapshot_peer_features_use_non_zero_fallback(self):
        feature_engineering = _load_module(
            "test_src_feature_engineering_peer_fallback", ROOT / "src/feature_engineering.py"
        )
        base = {
            "operating_mode": "normal",
            "asic_clock_mhz": 620,
            "asic_voltage_v": 12.8,
            "ambient_temperature_c": 30,
            "power_instability_index": 0.1,
            "hashrate_deviation_pct": 0.0,
            "failure_within_horizon": 0,
        }
        df = pd.DataFrame(
            [
                {
                    **base,
                    "timestamp": "2026-04-08T10:00:00Z",
                    "miner_id": "m-01",
                    "asic_hashrate_ths": 100.0,
                    "asic_temperature_c": 80.0,
                    "asic_power_w": 3000.0,
                    "true_efficiency_te": 0.0300,
                },
                {
                    **base,
                    "timestamp": "2026-04-08T10:05:00Z",
                    "miner_id": "m-02",
                    "asic_hashrate_ths": 110.0,
                    "asic_temperature_c": 82.0,
                    "asic_power_w": 3100.0,
                    "true_efficiency_te": 0.0310,
                },
            ]
        )
        snap = feature_engineering.build_serving_feature_snapshot(df)
        peer_dev = snap["mode_peer_hashrate_dev_pct"].abs().max()
        self.assertGreater(peer_dev, 0.0)

    def test_resolve_alert_returns_not_found_when_missing(self):
        alerts_py = (ROOT / "api/routers/alerts.py").read_text(encoding="utf-8")
        self.assertIn("RETURNING id", alerts_py)
        self.assertIn("status_code=404", alerts_py)

    def test_auth_supports_http_only_cookie_sessions(self):
        auth_py = (ROOT / "api/auth.py").read_text(encoding="utf-8")
        self.assertIn("AUTH_COOKIE_NAME", auth_py)
        self.assertIn("request.cookies.get(AUTH_COOKIE_NAME)", auth_py)
        self.assertIn("response.set_cookie(", auth_py)
        self.assertIn("httponly=True", auth_py)
        self.assertIn('response.delete_cookie(key=AUTH_COOKIE_NAME', auth_py)
        self.assertIn('"/auth/me"', auth_py)
        self.assertIn('"/auth/logout"', auth_py)

    def test_dashboard_uses_cookie_auth_not_local_storage_token(self):
        app_js = (ROOT / "dashboard/app.js").read_text(encoding="utf-8")
        self.assertNotIn("localStorage.getItem('aic_token')", app_js)
        self.assertNotIn("localStorage.setItem('aic_token'", app_js)
        self.assertNotIn("localStorage.removeItem('aic_token')", app_js)
        self.assertIn("credentials: 'include'", app_js)
        self.assertIn("api('/api/auth/me'", app_js)
        self.assertIn("fetch('/api/auth/logout'", app_js)

    def test_dashboard_escapes_dynamic_html_fields(self):
        app_js = (ROOT / "dashboard/app.js").read_text(encoding="utf-8")
        self.assertIn("function esc(value)", app_js)
        self.assertIn("const minerLabel = esc(m.miner_id);", app_js)
        self.assertIn("${minerLabel}", app_js)
        self.assertIn("${esc(a.miner_id)}", app_js)
        self.assertIn("${esc(a.trigger_reason || '—')}", app_js)
        self.assertIn("${esc(s.url_template)}", app_js)
        self.assertIn("safeClassToken(", app_js)

    def test_settings_router_enforces_allowlist_and_validation(self):
        settings_py = (ROOT / "api/routers/settings.py").read_text(encoding="utf-8")
        self.assertIn("ALLOWED_SETTING_KEYS", settings_py)
        self.assertIn("unknown_keys", settings_py)
        self.assertIn("_validate_setting_value", settings_py)
        self.assertIn("Unknown setting keys", settings_py)
        self.assertIn("status_code=404, detail=\"Setting key not found\"", settings_py)

    def test_fetcher_has_dns_rebinding_hardening(self):
        fetcher_py = (ROOT / "worker/fetcher.py").read_text(encoding="utf-8")
        self.assertIn("_pinned_http_url", fetcher_py)
        self.assertIn("inspect_source_url(source_url)", fetcher_py)
        self.assertIn("DNS changed during preflight", fetcher_py)

    def test_fleet_queries_use_lateral_latest_row_pattern(self):
        fleet_py = (ROOT / "api/routers/fleet.py").read_text(encoding="utf-8")
        self.assertIn("JOIN LATERAL", fleet_py)
        self.assertNotIn("SELECT DISTINCT ON", fleet_py)
        self.assertIn("FROM kpi_hourly_rollup", fleet_py)
        self.assertIn("FROM kpi_telemetry", fleet_py)
        self.assertIn("NOT EXISTS (SELECT 1 FROM seeded_miners)", fleet_py)
        # Fallback CTE is time-bounded to avoid full table scans
        self.assertIn("INTERVAL '7 days'", fleet_py)
        # CTE is extracted into a shared function
        self.assertIn("_fleet_miners_cte()", fleet_py)

    def test_analytics_rollup_schema_check_is_cached(self):
        analytics_py = (ROOT / "api/routers/analytics.py").read_text(encoding="utf-8")
        self.assertIn("ANALYTICS_ROLLUP_SCHEMA_CACHE_SECONDS", analytics_py)
        self.assertIn("_ROLLUP_SCHEMA_EXISTS_CACHE", analytics_py)
        self.assertIn("time.monotonic()", analytics_py)

    def test_analytics_correlation_aliases_are_name_based(self):
        analytics_py = (ROOT / "api/routers/analytics.py").read_text(encoding="utf-8")
        self.assertIn("def _correlation_alias(", analytics_py)
        self.assertIn("corr__{left_col}__{right_col}", analytics_py)
        self.assertNotIn("AS c_{row_idx}_{col_idx}", analytics_py)
        self.assertNotIn('row.get(f"c_{row_idx}_{col_idx}")', analytics_py)

    def test_policy_zero_length_curtailment_window_is_not_active(self):
        policy = _load_module("test_src_policy_zero_window", ROOT / "src/policy.py")
        cfg = policy.parse_policy_config(
            {
                "curtailment_windows_json": '[{"start_hour":5,"end_hour":5}]',
                "policy_timezone": "UTC",
            }
        )
        row = {
            "timestamp": "2026-04-07T05:00:00+00:00",
            "risk_score": 0.6,
            "asic_power_w": 3000,
            "asic_hashrate_ths": 100,
            "asic_temperature_c": 80,
            "event_codes": "",
        }
        breakdown = policy.estimate_action_utility(row, "CONTINUE", cfg)
        self.assertFalse(breakdown["in_curtailment_window"])

    def test_load_sources_has_error_handling(self):
        app_js = (ROOT / "dashboard/app.js").read_text(encoding="utf-8")
        self.assertIn("Sources load failed", app_js)
        self.assertIn("Failed to load API sources. Try again.", app_js)

    def test_dashboard_settings_preserve_falsy_values(self):
        app_js = (ROOT / "dashboard/app.js").read_text(encoding="utf-8")
        self.assertIn("el.value = s[k] ?? ''", app_js)

    def test_dockerfiles_run_as_non_root_user(self):
        api_dockerfile = (ROOT / "docker/api/Dockerfile").read_text(encoding="utf-8")
        worker_dockerfile = (ROOT / "docker/worker/Dockerfile").read_text(encoding="utf-8")
        self.assertIn("USER appuser", api_dockerfile)
        self.assertIn("USER appuser", worker_dockerfile)

    def test_dockerfiles_use_multistage_build_and_keep_runtime_slim(self):
        api_dockerfile = (ROOT / "docker/api/Dockerfile").read_text(encoding="utf-8")
        worker_dockerfile = (ROOT / "docker/worker/Dockerfile").read_text(encoding="utf-8")

        for dockerfile in (api_dockerfile, worker_dockerfile):
            self.assertIn("AS builder", dockerfile)
            self.assertIn("COPY --from=builder /wheels /wheels", dockerfile)
            final_stage = dockerfile.split("FROM python:3.11-slim")[-1]
            self.assertIn("libgomp1", final_stage)
            self.assertNotIn("build-essential", final_stage)

    def test_requirements_excludes_unused_python_telegram_bot_dependency(self):
        requirements_txt = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertNotIn("python-telegram-bot", requirements_txt)

    def test_sample_data_default_output_is_repo_anchored(self):
        sample_py = (ROOT / "scripts/generate_sample_data.py").read_text(encoding="utf-8")
        self.assertIn("DEFAULT_OUTPUT_PATH", sample_py)
        self.assertIn("Path(__file__).resolve().parents[1]", sample_py)

    def test_ml_jobs_avoids_runtime_sys_path_mutations(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertNotIn("sys.path.insert(", ml_jobs_py)
        self.assertIn(
            "from src.feature_engineering import build_serving_feature_snapshot",
            ml_jobs_py,
        )
        self.assertIn("from src.feature_engineering import run_feature_engineering", ml_jobs_py)
        self.assertIn("from src.train import run_training_pipeline", ml_jobs_py)
        self.assertIn("normalize_control_mode", ml_jobs_py)

    def test_control_mode_normalization_is_shared_across_worker_modules(self):
        runtime_utils_py = (ROOT / "src/runtime_utils.py").read_text(encoding="utf-8")
        automator_py = (ROOT / "worker/automator.py").read_text(encoding="utf-8")
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("def normalize_control_mode(", runtime_utils_py)
        self.assertIn("from src.runtime_utils import is_truthy, normalize_control_mode", automator_py)
        self.assertIn("from src.runtime_utils import is_truthy, normalize_control_mode", ml_jobs_py)

    def test_api_and_worker_configure_structured_json_logging(self):
        logging_utils_py = (ROOT / "src/logging_utils.py").read_text(encoding="utf-8")
        api_main_py = (ROOT / "api/main.py").read_text(encoding="utf-8")
        worker_main_py = (ROOT / "worker/main.py").read_text(encoding="utf-8")
        self.assertIn("class JsonFormatter", logging_utils_py)
        self.assertIn("json.dumps(", logging_utils_py)
        self.assertIn('configure_logging("api")', api_main_py)
        self.assertIn('configure_logging("worker")', worker_main_py)
        self.assertNotIn("logging.basicConfig(", worker_main_py)

    def test_src_and_api_avoid_legacy_typing_generics_imports(self):
        targets = [
            ROOT / "api/schemas.py",
            ROOT / "api/routers/alerts.py",
            ROOT / "api/routers/analytics.py",
            ROOT / "api/routers/ingest.py",
            ROOT / "src/kpi.py",
            ROOT / "src/inference.py",
            ROOT / "src/train.py",
            ROOT / "src/pipeline.py",
            ROOT / "src/preprocessing.py",
            ROOT / "src/eda.py",
            ROOT / "src/visualization.py",
            ROOT / "src/data_generation.py",
            ROOT / "src/evaluation.py",
            ROOT / "src/phase5.py",
            ROOT / "src/ingestion.py",
            ROOT / "src/feature_engineering.py",
        ]
        forbidden = (
            "from typing import Dict",
            "from typing import List",
            "from typing import Tuple",
            "from typing import Optional",
        )
        for path in targets:
            content = path.read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(token, content)

    def test_dedup_hypertable_delete_is_documented(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        deploy = (ROOT / "docs/DEPLOYMENT.md").read_text(encoding="utf-8")
        self.assertIn("apply_dedup_indexes.sql", readme)
        self.assertIn("DELETE", readme)
        self.assertIn("apply_dedup_indexes.sql", deploy)
        self.assertIn("deletes duplicate rows from telemetry hypertables", deploy)

    def test_policy_utility_applies_curtailment_penalty(self):
        policy = _load_module("test_src_policy", ROOT / "src/policy.py")
        cfg = policy.parse_policy_config(
            {
                "energy_price_usd_per_kwh": "0.10",
                "energy_price_schedule_json": '{"18": 0.25}',
                "curtailment_windows_json": '[{"start_hour":17,"end_hour":21}]',
                "policy_timezone": "UTC",
            }
        )
        row = {
            "timestamp": "2026-04-07T18:00:00+00:00",
            "risk_score": 0.68,
            "asic_power_w": 3200,
            "asic_hashrate_ths": 110,
            "asic_temperature_c": 88,
            "event_codes": "",
        }
        breakdown = policy.estimate_action_utility(row, "CONTINUE", cfg)
        self.assertTrue(breakdown["in_curtailment_window"])
        self.assertAlmostEqual(breakdown["energy_price_usd_per_kwh"], 0.5, places=6)

    def test_policy_reward_default_is_derived_from_hashprice(self):
        policy = _load_module("test_src_policy_reward_default", ROOT / "src/policy.py")
        cfg = policy.parse_policy_config({"hashprice_usd_per_ph_day": "55"})
        expected = 55.0 / 1000.0 / 24.0
        self.assertAlmostEqual(cfg["reward_per_th_hour_usd"], expected, places=9)

    def test_policy_backtest_reports_uplift_shape(self):
        policy = _load_module("test_src_policy_backtest", ROOT / "src/policy.py")
        cfg = policy.parse_policy_config(
            {"policy_min_uplift_usd_per_miner": "0.0", "policy_timezone": "UTC"}
        )
        rows = [
            {
                "timestamp": "2026-04-07T03:00:00+00:00",
                "risk_score": 0.81,
                "asic_power_w": 2900,
                "asic_hashrate_ths": 108,
                "asic_temperature_c": 90,
                "event_codes": "HASH_DROP",
            },
            {
                "timestamp": "2026-04-07T03:00:00+00:00",
                "risk_score": 0.42,
                "asic_power_w": 2100,
                "asic_hashrate_ths": 86,
                "asic_temperature_c": 78,
                "event_codes": "",
            },
        ]
        result = policy.backtest_policy_uplift(rows, cfg)
        self.assertEqual(result["samples"], 2)
        self.assertIn("avg_uplift_usd_per_miner", result)
        self.assertIn("baseline_action_mix", result)
        self.assertIn("optimized_action_mix", result)

    def test_notifier_respects_sent_flags(self):
        notifier = _load_module("test_worker_notifier", ROOT / "worker/notifier.py")
        engine = _FakeEngine()

        with mock.patch.object(notifier, "_send_email", return_value=True) as send_email:
            with mock.patch.object(
                notifier, "_send_telegram", return_value=True
            ) as send_telegram:
                notifier.run_notify_job(engine)

        send_email.assert_not_called()
        send_telegram.assert_called_once()
        self.assertEqual(len(engine.updates), 1)
        self.assertFalse(engine.updates[0]["es"])
        self.assertTrue(engine.updates[0]["ts"])


if __name__ == "__main__":
    unittest.main()
