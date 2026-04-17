"""Email and Telegram alert delivery."""

import html
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.secret_store import decrypt_if_needed

logger = logging.getLogger("worker.notifier")
SENSITIVE_SETTING_KEYS = {"smtp_password", "telegram_bot_token"}
_NOTIFIER_KEYS = (
    "smtp_host", "smtp_user", "smtp_password", "smtp_port",
    "alert_from_email", "alert_to_emails",
    "telegram_bot_token", "telegram_chat_id",
)


def _get_settings(engine: Engine) -> dict:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT key, value FROM app_settings WHERE key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(_NOTIFIER_KEYS)},
        )
        settings = {r[0]: r[1] for r in result}
    for key in SENSITIVE_SETTING_KEYS:
        if key in settings:
            settings[key] = decrypt_if_needed(settings[key])
    return settings


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

    email_ok = bool(
        cfg.get("smtp_host") and cfg.get("smtp_user") and cfg.get("smtp_password")
    )
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
        smtp_host = str(cfg.get("smtp_host") or "").strip()
        smtp_user = str(cfg.get("smtp_user") or "").strip()
        smtp_password = str(cfg.get("smtp_password") or "")
        if not (smtp_host and smtp_user and smtp_password):
            logger.warning("Email failed: smtp_host/smtp_user/smtp_password not configured")
            return False

        risk_score = float(alert.get("risk_score") or 0.0)
        severity = str(alert.get("severity") or "warning").strip().lower()
        severity_label = severity.upper()
        miner_id = str(alert.get("miner_id") or "unknown").replace("\n", " ").replace(
            "\r", " "
        )
        message = str(alert.get("message") or "")
        message_html = html.escape(message)
        miner_id_html = html.escape(miner_id)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[AI Controller] {severity_label} - Miner {miner_id}"
        msg["From"] = str(cfg.get("alert_from_email") or "").strip() or smtp_user
        recipients = [e.strip() for e in (cfg.get("alert_to_emails") or "").split(",") if e.strip()]
        if not recipients:
            return False
        msg["To"] = ", ".join(recipients)

        html_body = f"""
        <html><body style="font-family:sans-serif;background:#0a0f1e;color:#e2e8f0;padding:24px">
          <h2 style="color:{'#ef4444' if severity == 'critical' else '#f59e0b'}">
            ⚠️ {severity_label} Alert
          </h2>
          <p><b>Miner:</b> {miner_id_html}</p>
          <p><b>Risk Score:</b> {risk_score:.2%}</p>
          <p><b>Message:</b> {message_html}</p>
          <hr/>
          <small>AI Controller Predictive Maintenance</small>
        </body></html>
        """
        msg.attach(MIMEText(html_body, "html"))

        port = int(cfg.get("smtp_port", 587))
        with smtplib.SMTP(smtp_host, port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(msg["From"], recipients, msg.as_string())
        logger.info("Email sent for alert %s", alert["id"])
        return True
    except Exception as exc:
        logger.warning("Email failed: %s", exc)
        return False


def _send_telegram(cfg: dict, alert: dict) -> bool:
    try:
        import httpx

        token = str(cfg.get("telegram_bot_token") or "").strip()
        chat_id = str(cfg.get("telegram_chat_id") or "").strip()
        if not token or not chat_id:
            logger.warning("Telegram failed: telegram_bot_token/telegram_chat_id not configured")
            return False
        risk_score = float(alert.get("risk_score") or 0.0)
        severity = str(alert.get("severity") or "warning").strip().lower()
        severity_label = html.escape(severity.upper())
        miner_id = html.escape(str(alert.get("miner_id") or "unknown"))
        message = html.escape(str(alert.get("message") or ""))
        emoji = "🔴" if severity == "critical" else "🟡"
        text_msg = (
            f"{emoji} <b>{severity_label} Alert</b>\n"
            f"<b>Miner:</b> <code>{miner_id}</code>\n"
            f"<b>Risk Score:</b> <code>{risk_score:.2%}</code>\n"
            f"<b>Message:</b> {message}"
        )
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = httpx.post(
            url,
            json={"chat_id": chat_id, "text": text_msg, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Telegram sent for alert %s", alert["id"])
        return True
    except Exception as exc:
        logger.warning("Telegram failed: %s", exc)
        return False
