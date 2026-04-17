"""JWT authentication for AI Controller API."""

import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from jose import JWTError, jwt
import bcrypt
from pydantic import BaseModel

from api.rate_limit import limiter

router = APIRouter()

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 8
AUTH_LOGIN_RATE_LIMIT = os.getenv("AUTH_LOGIN_RATE_LIMIT", "5/minute").strip() or "5/minute"
AUTH_COOKIE_NAME = os.getenv("AUTH_COOKIE_NAME", "aic_session").strip() or "aic_session"
AUTH_COOKIE_PATH = os.getenv("AUTH_COOKIE_PATH", "/").strip() or "/"
AUTH_COOKIE_SECURE = str(os.getenv("AUTH_COOKIE_SECURE", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
AUTH_COOKIE_SAMESITE = os.getenv("AUTH_COOKIE_SAMESITE", "strict").strip().lower()
if AUTH_COOKIE_SAMESITE not in {"strict", "lax", "none"}:
    AUTH_COOKIE_SAMESITE = "strict"

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "").strip()


def _load_admin_password_hash() -> str:
    configured_hash = os.getenv("ADMIN_PASSWORD_HASH", "").strip()
    legacy_password = os.getenv("ADMIN_PASSWORD", "").strip()

    # Always clear plaintext password from the process environment.
    if legacy_password:
        os.environ.pop("ADMIN_PASSWORD", None)
        import logging
        logging.getLogger("api.auth").warning(
            "ADMIN_PASSWORD is deprecated and ignored. "
            "Set ADMIN_PASSWORD_HASH with a bcrypt hash instead. "
            "Generate one with: python -c \"from passlib.context import CryptContext; "
            "print(CryptContext(schemes=['bcrypt']).hash('YOUR_PASSWORD'))\""
        )

    if configured_hash:
        return configured_hash

    return ""


ADMIN_PASSWORD_HASH = _load_admin_password_hash()


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    username: str


def create_token(username: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": username,
        "exp": now + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": now,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(request: Request) -> str:
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Missing authentication token")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.post("/auth/login", response_model=LoginResponse)
@limiter.limit(AUTH_LOGIN_RATE_LIMIT)
async def login(request: Request, req: LoginRequest, response: Response):
    _ = request
    if not ADMIN_USERNAME or not ADMIN_PASSWORD_HASH:
        raise HTTPException(status_code=503, detail="Admin credentials are not configured")

    password_ok = False
    if req.username == ADMIN_USERNAME:
        try:
            password_ok = bcrypt.checkpw(
                req.password.encode("utf-8"),
                ADMIN_PASSWORD_HASH.encode("utf-8")
            )
        except Exception:
            password_ok = False

    if not password_ok:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_token(req.username)
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite=AUTH_COOKIE_SAMESITE,
        max_age=int(JWT_EXPIRE_HOURS * 3600),
        path=AUTH_COOKIE_PATH,
    )
    return LoginResponse(username=req.username)


@router.get("/auth/me")
async def get_current_user(username: str = Depends(verify_token)):
    return {"username": username}


@router.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie(key=AUTH_COOKIE_NAME, path=AUTH_COOKIE_PATH)
    return {"status": "logged_out"}
