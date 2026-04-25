"""
FastAPI dependency for OIDC/Keycloak JWT validation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional

import structlog
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from jose.backends import RSAKey

logger = structlog.get_logger(__name__)
bearer_scheme = HTTPBearer()

KEYCLOAK_ISSUER   = os.environ.get("KEYCLOAK_ISSUER", "http://localhost:8080/realms/nsoe")
KEYCLOAK_AUDIENCE = os.environ.get("KEYCLOAK_AUDIENCE", "nsoe-api")
_JWKS_CACHE: Optional[dict] = None


@dataclass
class AnalystUser:
    sub: str
    email: str
    roles: List[str]
    clearance_level: str


async def _get_jwks() -> dict:
    global _JWKS_CACHE
    if _JWKS_CACHE is None:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{KEYCLOAK_ISSUER}/protocol/openid-connect/certs")
            resp.raise_for_status()
            _JWKS_CACHE = resp.json()
    return _JWKS_CACHE


async def require_analyst(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> AnalystUser:
    token = credentials.credentials
    try:
        jwks = await _get_jwks()
        payload = jwt.decode(
            token,
            jwks,
            algorithms=["RS256"],
            audience=KEYCLOAK_AUDIENCE,
            issuer=KEYCLOAK_ISSUER,
        )
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    roles: List[str] = payload.get("realm_access", {}).get("roles", [])
    if "nsoe_analyst" not in roles and "nsoe_admin" not in roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient role",
        )

    return AnalystUser(
        sub=payload["sub"],
        email=payload.get("email", ""),
        roles=roles,
        clearance_level=payload.get("clearance_level", "RESTRICTED"),
    )
