"""Helpers for encrypting sensitive app setting values at rest."""

from __future__ import annotations

import base64
import hashlib
import logging
import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger("src.secret_store")

ENCRYPTED_PREFIX = "enc:v1:"
ENCRYPTION_KEY_ENV = "APP_SETTINGS_ENCRYPTION_KEY"


def _derive_fernet_key(seed: str) -> bytes:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _normalize_fernet_key(raw_key: str) -> bytes:
    candidate = raw_key.strip().encode("utf-8")
    try:
        Fernet(candidate)
        return candidate
    except Exception:
        return _derive_fernet_key(raw_key)


def is_encryption_configured() -> bool:
    return bool(os.getenv(ENCRYPTION_KEY_ENV, "").strip())


def validate_secret_store_configuration() -> bool:
    configured = is_encryption_configured()
    if not configured:
        logger.warning(
            "%s is unset; secret setting encryption/decryption is disabled until configured.",
            ENCRYPTION_KEY_ENV,
        )
    return configured


def _require_configured_key() -> str:
    key = os.getenv(ENCRYPTION_KEY_ENV, "").strip()
    if key:
        return key
    raise RuntimeError(
        f"{ENCRYPTION_KEY_ENV} is required to encrypt/decrypt sensitive settings"
    )


@lru_cache(maxsize=1)
def _get_fernet():
    return Fernet(_normalize_fernet_key(_require_configured_key()))


def encrypt_if_needed(value: str) -> str:
    raw = "" if value is None else str(value)
    if raw == "":
        return ""
    if raw.startswith(ENCRYPTED_PREFIX):
        return raw
    token = _get_fernet().encrypt(raw.encode("utf-8")).decode("utf-8")
    return ENCRYPTED_PREFIX + token


def decrypt_if_needed(value: str) -> str:
    raw = "" if value is None else str(value)
    if raw == "" or not raw.startswith(ENCRYPTED_PREFIX):
        return raw
    token_text = raw[len(ENCRYPTED_PREFIX):]
    try:
        token = token_text.encode("utf-8")
        return _get_fernet().decrypt(token).decode("utf-8")
    except RuntimeError as exc:
        logger.error("Secret decryption unavailable: %s", exc)
        return ""
    except (InvalidToken, ValueError, TypeError, UnicodeDecodeError):
        return ""
