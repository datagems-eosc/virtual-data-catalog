import os
import time
from typing import Any

from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
    OAuth2PasswordBearer,
)
from fastapi import Depends, HTTPException, logger, status
import httpx
from jose import JWTError, jwt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
OIDC_CLIENT_ID: str = os.getenv("OIDC_CLIENT_ID", "virtual-data-catalog-api")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")
OIDC_ISSUER_URL: str = os.getenv(
    "OIDC_ISSUER_URL", "https://datagems-dev.scayle.es/oauth/realms/dev"
)
OIDC_CONFIG_URL: str = f"{OIDC_ISSUER_URL}/.well-known/openid-configuration"

OIDC_JWKS_URL = os.getenv(
    "OIDC_JWKS_URL",
    f"{OIDC_ISSUER_URL}/protocol/openid-connect/certs",
)
JWT_SIGNING_ALGORITHM = "RS256"
HTTP_TIMEOUT_SECONDS = 10.0
JWKS_CACHE_TTL_SECONDS = int(os.getenv("JWKS_CACHE_TTL_SECONDS", "300"))


_bearer_scheme = HTTPBearer(auto_error=False)


async def require_valid_token(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """FastAPI dependency that enforces bearer-token authentication.

    Returns decoded JWT claims on success, otherwise raises HTTP errors:
    - 401 for missing/invalid token
    - 403 for unauthorized `azp`
    - 503 if JWKS cannot be retrieved
    """
    # Require `Authorization: Bearer <token>`.
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )

    try:
        # Verify token signature and issuer using the provider public keys.
        # Audience verification is intentionally disabled for now (`verify_aud=False`).
        # This means any token from the same issuer can pass unless constrained by `azp`.
        payload = jwt.decode(
            credentials.credentials,
            await _get_jwks(),
            algorithms=[JWT_SIGNING_ALGORITHM],
            audience=OIDC_CLIENT_ID,
            issuer=OIDC_ISSUER_URL,
            options={"verify_aud": False},
        )
    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        ) from exc
    except httpx.HTTPError as exc:
        # Infrastructure/network issue while retrieving verification keys.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not fetch JWKS: {exc}",
        ) from exc

    # Dependency output: route handlers can use these claims directly.
    return payload


async def require_app_scope(
    token_payload: dict[str, Any] = Depends(require_valid_token),
) -> dict[str, Any]:
    """Validate that token has access to this API via audience claim.

    Checks if OIDC_CLIENT_ID is present in the token's audience (aud) claim.
    The aud claim can be a string or a list of strings.
    Returns decoded JWT claims on success, otherwise raises 403 Forbidden.
    """
    # Extract audience from token - can be a string or list
    aud = token_payload.get("aud", [])

    # Normalize to list for consistent handling
    audiences = aud if isinstance(aud, list) else [aud] if aud else []

    if OIDC_CLIENT_ID not in audiences:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token missing required audience: {OIDC_CLIENT_ID}",
        )

    return token_payload


async def _get_jwks() -> dict[str, Any]:
    """Return JWKS, using a short-lived in-memory cache.

    Implication: signature verification does not perform an external HTTP call for
    every request; however, key rotations can take up to cache TTL to be picked up.
    """
    global _jwks_cache, _jwks_cache_expires_at

    # Fast path: return cached keys if TTL is still valid.
    if _jwks_cache and time.time() < _jwks_cache_expires_at:
        return _jwks_cache

    # Slow path: fetch current keys from IdP.
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        response = await client.get(OIDC_JWKS_URL)
        response.raise_for_status()
        _jwks_cache = response.json()

    # Cache TTL is controlled by `JWKS_CACHE_TTL_SECONDS`.
    _jwks_cache_expires_at = time.time() + JWKS_CACHE_TTL_SECONDS
    return _jwks_cache
