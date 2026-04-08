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
from datetime import datetime
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
        if "FROM app_settings" in q and "WHERE key IN" in q:
            return [
                ("risk_threshold", "0.77"),
                ("alert_cooldown_hours", "3"),
                ("inference_lookback_hours", "48"),
                ("cooling_power_ratio", "0.31"),
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
        self.assertIn('@router.get("/alerts", response_model=List[AlertOut])', alerts_py)
        self.assertIn(
            '@router.get("/alerts/history", response_model=List[AlertOut])',
            alerts_py,
        )

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

    def test_retrain_passes_worker_model_path_to_training(self):
        ml_jobs_py = (ROOT / "worker/ml_jobs.py").read_text(encoding="utf-8")
        self.assertIn("model_artifact_path=MODEL_PATH", ml_jobs_py)

    def test_worker_main_gates_automator_by_control_mode(self):
        worker_main_py = (ROOT / "worker/main.py").read_text(encoding="utf-8")
        self.assertIn("scheduler.add_job(job_automator", worker_main_py)
        self.assertIn("Automator scheduled; execution is gated at runtime", worker_main_py)

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

        with mock.patch.object(fetcher, "validate_source_url", return_value=None):
            with mock.patch.object(fetcher.httpx, "Client", _Client):
                fetcher._fetch_and_store(engine, source)

        self.assertEqual(len(engine.inserts), 1)
        inserted_records = engine.inserts[0]
        self.assertEqual(len(inserted_records), 1)
        self.assertEqual(inserted_records[0]["miner_id"], "m-21")
        self.assertEqual(inserted_records[0]["operating_mode"], "normal")
        self.assertIsNone(inserted_records[0]["event_codes"])

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

    def test_runtime_config_reads_db_threshold_and_cooldown(self):
        ml_jobs = _load_module("test_worker_ml_jobs_cfg", ROOT / "worker/ml_jobs.py")
        cfg = ml_jobs._load_runtime_config(_FakeEngine())
        self.assertEqual(cfg["risk_threshold"], 0.77)
        self.assertEqual(cfg["alert_cooldown_hours"], 3)
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
