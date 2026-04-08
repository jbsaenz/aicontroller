"""Email and Telegram alert delivery."""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger("worker.notifier")


def _get_settings(engine: Engine) -> dict:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT key, value FROM app_settings"))
        return {r[0]: r[1] for r in result}


def run_notify_job(engine: Engine):
    cfg = _get_settings(engine)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, miner_id, severity, risk_score, message,
                   email_sent, telegram_sent
            FROM alerts
            WHERE resolved = FALSE
              AND (email_sent = FALSE OR telegram_sent = FALSE)
            ORDER BY created_at DESC
            LIMIT 50
        """))
        pending = result.mappings().all()

    if not pending:
        return

    email_ok = bool(cfg.get("smtp_host") and cfg.get("smtp_user"))
    telegram_ok = bool(cfg.get("telegram_bot_token") and cfg.get("telegram_chat_id"))

    for alert in pending:
        aid = alert["id"]
        email_sent = False
        tg_sent = False

        if email_ok and not alert["email_sent"]:
            email_sent = _send_email(cfg, alert)

        if telegram_ok and not alert["telegram_sent"]:
            tg_sent = _send_telegram(cfg, alert)

        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE alerts
                    SET email_sent = CASE WHEN :es THEN TRUE ELSE email_sent END,
                        telegram_sent = CASE WHEN :ts THEN TRUE ELSE telegram_sent END
                    WHERE id = :aid
                """),
                {"es": email_sent, "ts": tg_sent, "aid": aid},
            )


def _send_email(cfg: dict, alert: dict) -> bool:
    try:
        risk_score = float(alert.get("risk_score") or 0.0)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[AI Controller] {alert['severity'].upper()} — Miner {alert['miner_id']}"
        msg["From"] = cfg.get("alert_from_email", cfg["smtp_user"])
        recipients = [e.strip() for e in (cfg.get("alert_to_emails") or "").split(",") if e.strip()]
        if not recipients:
            return False
        msg["To"] = ", ".join(recipients)

        html = f"""
        <html><body style="font-family:sans-serif;background:#0a0f1e;color:#e2e8f0;padding:24px">
          <h2 style="color:{'#ef4444' if alert['severity']=='critical' else '#f59e0b'}">
            ⚠️ {alert['severity'].upper()} Alert
          </h2>
          <p><b>Miner:</b> {alert['miner_id']}</p>
          <p><b>Risk Score:</b> {risk_score:.2%}</p>
          <p><b>Message:</b> {alert['message']}</p>
          <hr/>
          <small>AI Controller Predictive Maintenance</small>
        </body></html>
        """
        msg.attach(MIMEText(html, "html"))

        port = int(cfg.get("smtp_port", 587))
        with smtplib.SMTP(cfg["smtp_host"], port) as server:
            server.starttls()
            server.login(cfg["smtp_user"], cfg["smtp_password"])
            server.sendmail(msg["From"], recipients, msg.as_string())
        logger.info("Email sent for alert %s", alert["id"])
        return True
    except Exception as exc:
        logger.warning("Email failed: %s", exc)
        return False


def _send_telegram(cfg: dict, alert: dict) -> bool:
    try:
        import httpx
        token = cfg["telegram_bot_token"]
        chat_id = cfg["telegram_chat_id"]
        risk_score = float(alert.get("risk_score") or 0.0)
        emoji = "🔴" if alert["severity"] == "critical" else "🟡"
        text_msg = (
            f"{emoji} *{alert['severity'].upper()} Alert*\n"
            f"*Miner:* `{alert['miner_id']}`\n"
            f"*Risk Score:* `{risk_score:.2%}`\n"
            f"*Message:* {alert['message']}"
        )
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = httpx.post(url, json={"chat_id": chat_id, "text": text_msg, "parse_mode": "Markdown"}, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram sent for alert %s", alert["id"])
        return True
    except Exception as exc:
        logger.warning("Telegram failed: %s", exc)
        return False
