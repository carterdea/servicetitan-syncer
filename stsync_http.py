from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from stsync_settings import require_settings

logger = structlog.get_logger()


def build_url(base: str, path: str, tenant_id: str) -> str:
    if "{tenant}" in path:
        path = path.replace("{tenant}", str(tenant_id))
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential_jitter(1, 5),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
)
def http_get(base: str, path: str, bearer: str, params: dict[str, Any]) -> dict[str, Any]:
    s = require_settings()

    if base == s.API_BASE_PROD:
        tenant_id = s.TENANT_ID_PROD
        app_key = s.APP_KEY_PROD
        env_name = "Production"
    elif base == s.API_BASE_INT:
        tenant_id = s.TENANT_ID_INT
        app_key = s.APP_KEY_INT
        env_name = "Integration"
    else:
        raise ValueError(f"Unknown API base: {base}")

    url = build_url(base, path, tenant_id)
    logger.debug("Making GET request", url=url, params=params)

    try:
        headers = {
            "Authorization": f"Bearer {bearer}",
            "ST-App-Key": app_key,
        }
        r = httpx.get(url, headers=headers, params=params, timeout=s.HTTP_TIMEOUT)

        if r.status_code == 429:
            logger.warning("Rate limited, backing off", url=url, status_code=r.status_code)
            raise httpx.HTTPStatusError("Rate limited", request=r.request, response=r)
        elif r.status_code >= 500:
            logger.error("Server error", url=url, status_code=r.status_code, response=r.text)
            raise RuntimeError(f"GET {url} -> {r.status_code}")

        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        logger.error(
            "HTTP error",
            url=url,
            status_code=e.response.status_code,
            response=e.response.text[:500],
            env=env_name,
        )
        raise
    except Exception as e:
        logger.error("Request error", url=url, error=str(e))
        raise


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential_jitter(1, 5),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
)
def http_post_json(
    base: str,
    path: str,
    bearer: str,
    payload: dict[str, Any],
    allow_wrapper_retry: bool = True,
) -> dict[str, Any]:
    s = require_settings()

    if base == s.API_BASE_PROD:
        tenant_id = s.TENANT_ID_PROD
        app_key = s.APP_KEY_PROD
        env_name = "Production"
    elif base == s.API_BASE_INT:
        tenant_id = s.TENANT_ID_INT
        app_key = s.APP_KEY_INT
        env_name = "Integration"
    else:
        raise ValueError(f"Unknown API base: {base}")

    url = build_url(base, path, tenant_id)
    logger.debug("Making POST request", url=url, payload_keys=list(payload.keys()))

    try:
        headers = {
            "Authorization": f"Bearer {bearer}",
            "Content-Type": "application/json",
            "ST-App-Key": app_key,
        }
        r = httpx.post(url, headers=headers, json=payload, timeout=s.HTTP_TIMEOUT)

        if r.status_code == 429:
            logger.warning("Rate limited, backing off", url=url, status_code=r.status_code)
            raise httpx.HTTPStatusError("Rate limited", request=r.request, response=r)
        elif r.status_code >= 500:
            logger.error(
                "Server error",
                url=url,
                status_code=r.status_code,
                response=r.text[:500],
                env=env_name,
            )
            if allow_wrapper_retry and "request" in (r.text or "").lower():
                try:
                    wrapped = {"request": payload}
                    logger.info("Retrying POST with request wrapper", url=url)
                    r2 = httpx.post(url, headers=headers, json=wrapped, timeout=s.HTTP_TIMEOUT)
                    r2.raise_for_status()
                    try:
                        return r2.json()
                    except Exception:
                        return {}
                except Exception:
                    pass
            raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text[:200]}")

        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            logger.warning("No JSON response from POST", url=url, status_code=r.status_code)
            return {}

    except httpx.HTTPStatusError as e:
        logger.error(
            "HTTP POST error",
            url=url,
            status_code=e.response.status_code,
            response=e.response.text[:500],
            env=env_name,
        )
        raise
    except Exception as e:
        logger.error("POST request error", url=url, error=str(e))
        raise


def fetch_all(
    cfg: dict[str, Any], base: str, bearer: str, since: str | None
) -> Iterable[dict[str, Any]]:
    s = require_settings()

    params = dict(cfg.get("list_params") or {})
    if "pageSize" in params and not params["pageSize"]:
        params["pageSize"] = s.PAGE_SIZE_DEFAULT

    since_param = cfg.get("since_param")
    if since and since_param:
        params[since_param] = since

    list_key = cfg.get("list_data_key") or "items"
    next_key = cfg.get("next_page_key") or "hasMore"
    path = cfg["prod_list_path"]

    page_count = 0
    total_items = 0

    while True:
        page_count += 1
        data = http_get(base, path, bearer, params)
        items = data.get(list_key) or []

        logger.info(
            "Fetched page",
            page=page_count,
            item_count=len(items),
            total_so_far=total_items,
        )

        for it in items:
            total_items += 1
            yield it

        if "hasMore" in data:
            if data.get("hasMore"):
                params["page"] = int(params.get("page", 1)) + 1
                continue
            else:
                break

        next_page = data.get(next_key)
        if isinstance(next_page, int):
            params["page"] = next_page
            continue
        if isinstance(next_page, str) and next_page:
            params["continuationToken"] = next_page
            continue

        if "page" in params and "pageSize" in params and len(items) >= int(params["pageSize"]):
            params["page"] = int(params["page"]) + 1
            continue
        break
