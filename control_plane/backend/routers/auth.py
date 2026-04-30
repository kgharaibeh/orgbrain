"""
Auth Router — login / token verification for the OrgBrain Control Plane.
Uses PBKDF2-SHA256 for password hashing and HS256 JWTs.

Users are defined here (or override via AUTH_USERS env var as JSON).
Tokens expire after 12 hours; the JWT secret is AUTH_SECRET (default set).
"""

import base64
import hashlib
import json
import logging
import os
import time
from typing import Optional

import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter()

# ── Config ─────────────────────────────────────────────────────────────────────
JWT_SECRET  = os.getenv("AUTH_SECRET",  "orgbrain-jwt-secret-change-in-production-2024")
JWT_ALG     = "HS256"
TOKEN_TTL_S = int(os.getenv("AUTH_TOKEN_TTL", str(12 * 3600)))  # 12 h

# ── User store ─────────────────────────────────────────────────────────────────
# Each entry: {username, hash (base64 PBKDF2), salt (hex), role}
# Passwords hashed with: PBKDF2-HMAC-SHA256, 260 000 iterations
_DEFAULT_USERS = [
    {
        "username": "admin",
        "hash":     "k+nekeE3bVmcChXUx6BCWkla3RLO7fRKkZ8a4Ljb7dw=",
        "salt":     "6530e2f822cfb645fff0422752b7775a",
        "role":     "admin",
    },
    {
        "username": "analyst",
        "hash":     "YFO5IGUOmon8aObcxW+wRqAWtFCZ1PxziDoxfusH228=",
        "salt":     "d72bb854884871027f359eeefa3e611a",
        "role":     "viewer",
    },
    {
        "username": "operator",
        "hash":     "Py68MjtAiXTtguk4AvDU6w2oQY2/kHrAwyMiYYnH8zc=",
        "salt":     "887acf9313e50fca27563d409bf30e54",
        "role":     "viewer",
    },
]

def _load_users() -> dict:
    raw = os.getenv("AUTH_USERS")
    users = json.loads(raw) if raw else _DEFAULT_USERS
    return {u["username"]: u for u in users}


def _verify_password(plain: str, stored_hash: str, salt: str) -> bool:
    derived = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt.encode(), 260_000)
    return base64.b64encode(derived).decode() == stored_hash


# ── Token helpers ──────────────────────────────────────────────────────────────

def _issue_token(username: str, role: str) -> str:
    payload = {
        "sub":  username,
        "role": role,
        "iat":  int(time.time()),
        "exp":  int(time.time()) + TOKEN_TTL_S,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")


# ── Dependency — inject into protected routes ──────────────────────────────────
_bearer = HTTPBearer(auto_error=False)


def require_auth(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> dict:
    if not creds:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    return _decode_token(creds.credentials)


def require_admin(claims: dict = Depends(require_auth)) -> dict:
    if claims.get("role") != "admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin role required")
    return claims


# ── Endpoints ──────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(body: LoginRequest):
    users = _load_users()
    user  = users.get(body.username)
    if not user or not _verify_password(body.password, user["hash"], user["salt"]):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    token = _issue_token(body.username, user["role"])
    log.info(f"Login: {body.username} ({user['role']})")
    return {"token": token, "username": body.username, "role": user["role"],
            "expires_in": TOKEN_TTL_S}


@router.get("/verify")
def verify(claims: dict = Depends(require_auth)):
    """Validate a token and return its claims."""
    return {"username": claims["sub"], "role": claims["role"], "exp": claims["exp"]}


@router.get("/me")
def me(claims: dict = Depends(require_auth)):
    return {"username": claims["sub"], "role": claims["role"]}
