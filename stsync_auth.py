from __future__ import annotations

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from stsync_settings import require_settings
from typing import Any, cast

logger = structlog.get_logger()


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential_jitter(1, 5),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
)
def token(auth_url: str, client_id: str, client_secret: str, scope: str = "") -> str:
    """Fetch an OAuth2 client-credentials token.

    Raises RuntimeError with a descriptive message on failure.
    """
    settings = require_settings()
    env_name = "Integration" if "integration" in (auth_url or "").lower() else "Production"

    logger.info(f"Fetching OAuth token for {env_name}", url=auth_url)
    data = {"grant_type": "client_credentials"}
    if scope:
        data["scope"] = scope

    try:
        r = httpx.post(auth_url, data=data, auth=(client_id, client_secret), timeout=settings.HTTP_TIMEOUT)
        r.raise_for_status()
        data = cast(dict[str, Any], r.json())
        token_val = data.get("access_token")
        if isinstance(token_val, str):
            return token_val
        raise RuntimeError("Authentication succeeded but no access_token in response")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            logger.warning(
                f"Rate limited for {env_name}, backing off",
                status_code=e.response.status_code,
            )
            raise
        logger.error(
            f"{env_name} auth failed",
            status_code=e.response.status_code,
            response=e.response.text,
        )
        error_msg = f"{env_name} authentication failed: {e.response.text}"
        if "invalid_client" in e.response.text:
            error_msg += f"\nCheck your {env_name} CLIENT_ID and CLIENT_SECRET"
        raise RuntimeError(error_msg) from e
    except Exception as e:
        logger.error(f"{env_name} auth error", error=str(e))
        raise RuntimeError(f"{env_name} authentication error: {str(e)}") from e


def prod_token() -> str:
    """Short-hand: token() for Production environment settings."""
    s = require_settings()
    return token(s.AUTH_URL_PROD, s.CLIENT_ID_PROD, s.CLIENT_SECRET_PROD)


def int_token() -> str:
    """Short-hand: token() for Integration environment settings."""
    s = require_settings()
    return token(s.AUTH_URL_INT, s.CLIENT_ID_INT, s.CLIENT_SECRET_INT)
