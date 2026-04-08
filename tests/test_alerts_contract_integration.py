"""Integration-style contract tests for alert endpoints.

Uses real FastAPI routing with dependency overrides for auth/DB.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.auth import verify_token
from api.db import get_db
from api.main import app


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self):
        now = datetime.now(timezone.utc).isoformat()
        self.base_row = {
            "id": 101,
            "created_at": now,
            "miner_id": "miner-test-001",
            "severity": "critical",
            "risk_score": 0.88,
            "trigger_reason": "Risk score 88.00% exceeds threshold",
            "message": "Miner miner-test-001 — CRITICAL: predicted failure risk 88.00%",
            "recommended_action": "PULL_FOR_MAINTENANCE",
            "automation_triggered": False,
            "resolved": False,
            "resolved_at": None,
            "email_sent": True,
            "telegram_sent": False,
        }

    async def execute(self, query, params=None):
        q = str(query)
        if "FROM alerts" in q:
            if "WHERE resolved = :resolved" in q:
                row = dict(self.base_row)
                row["resolved"] = bool((params or {}).get("resolved", False))
                return _FakeResult([row])
            return _FakeResult([dict(self.base_row)])
        return _FakeResult([])


async def _override_get_db():
    yield _FakeSession()


def _override_verify_token():
    return "admin"


class AlertsContractIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app.dependency_overrides[get_db] = _override_get_db
        app.dependency_overrides[verify_token] = _override_verify_token
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        app.dependency_overrides.clear()

    def test_alerts_endpoint_includes_action_and_delivery_fields(self):
        res = self.client.get("/api/alerts")
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(isinstance(body, list) and body)
        row = body[0]
        self.assertIn("recommended_action", row)
        self.assertIn("automation_triggered", row)
        self.assertIn("email_sent", row)
        self.assertIn("telegram_sent", row)

    def test_alerts_history_endpoint_includes_action_and_delivery_fields(self):
        res = self.client.get("/api/alerts/history")
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(isinstance(body, list) and body)
        row = body[0]
        self.assertIn("recommended_action", row)
        self.assertIn("automation_triggered", row)
        self.assertIn("email_sent", row)
        self.assertIn("telegram_sent", row)


if __name__ == "__main__":
    unittest.main()
