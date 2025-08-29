#!/usr/bin/env python3
"""
ServiceTitan Prod → Integration copier (Jobs, Items, POs)

Features:
- OAuth2 client_credentials on both envs
- Config-driven endpoints (stsync.config.json)
- SQLite ID crosswalk: (kind, prod_id) -> integration_id
- Robust pagination, retries, dry-run, since filter, limit
- Pydantic models for data validation
- Structured logging with Rich console output

Usage:
  export $(grep -v '^#' .env | xargs)  # or rely on python-dotenv
  python stsync.py verify
  python stsync.py sync items --since 2025-08-01 --limit 50
  python stsync.py sync pos --since 2025-08-01
  python stsync.py sync jobs --since 2025-08-01 --dry-run
"""

import os
import json
import sqlite3
import time
from typing import Dict, Any, Iterable, Optional, List
from pathlib import Path

import click
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
)
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
import structlog
import logging

# Configure structured logging
logging.basicConfig(
    level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()]
)
logger = structlog.get_logger()


# Simple console for output
def print_msg(msg):
    print(msg)


def print_error(msg):
    print(f"ERROR: {msg}")


def print_success(msg):
    print(f"SUCCESS: {msg}")


# ---------- Pydantic Models ----------
class ItemCreate(BaseModel):
    code: str
    name: str
    active: bool = True


class POLineCreate(BaseModel):
    itemId: int
    quantity: float
    unitCost: Optional[float] = None


class POCreate(BaseModel):
    vendorId: int
    warehouseId: Optional[int] = None
    externalNumber: str
    lines: List[POLineCreate] = Field(default_factory=list)


class JobCreate(BaseModel):
    customerId: int
    locationId: int
    jobTypeId: int
    campaignId: Optional[int] = None
    source: str = "stsync"
    externalNumber: str
    notes: str


class APIResponse(BaseModel):
    id: Optional[int] = None
    guid: Optional[str] = None
    externalId: Optional[str] = None


# ---------- Environment Variables ----------
load_dotenv()
AUTH_URL_PROD = os.getenv("ST_AUTH_URL_PROD")
AUTH_URL_INT = os.getenv("ST_AUTH_URL_INT")
API_BASE_PROD = os.getenv("ST_API_BASE_PROD")
API_BASE_INT = os.getenv("ST_API_BASE_INT")
CLIENT_ID_PROD = os.getenv("ST_CLIENT_ID_PROD")
CLIENT_SECRET_PROD = os.getenv("ST_CLIENT_SECRET_PROD")
CLIENT_ID_INT = os.getenv("ST_CLIENT_ID_INT")
CLIENT_SECRET_INT = os.getenv("ST_CLIENT_SECRET_INT")
TENANT_ID_PROD = os.getenv("ST_TENANT_ID_PROD")
TENANT_ID_INT = os.getenv("ST_TENANT_ID_INT")
# App Keys (ServiceTitan v2 APIs)
APP_KEY_PROD = os.getenv("ST_APP_KEY_PROD") or os.getenv("ST_APP_KEY")
APP_KEY_INT = os.getenv("ST_APP_KEY_INT") or os.getenv("ST_APP_KEY")
DB_PATH = os.getenv("STSYNC_DB", "stsync.sqlite3")
PAGE_SIZE_DEFAULT = int(os.getenv("ST_PAGE_SIZE", "200"))
HTTP_TIMEOUT = int(os.getenv("ST_HTTP_TIMEOUT", "30"))


# ---------- Configuration ----------
def load_config() -> Dict[str, Any]:
    config_path = Path("stsync.config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------- URL helper ----------
def build_url(base: str, path: str, tenant_id: str) -> str:
    """Build full URL; replace optional {tenant} placeholder with tenant_id."""
    if "{tenant}" in path:
        path = path.replace("{tenant}", str(tenant_id))
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


# ---------- Database (ID crosswalk) ----------
class IDMapper:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("""CREATE TABLE IF NOT EXISTS id_map(
                kind TEXT NOT NULL,
                prod_id TEXT NOT NULL,
                int_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY(kind, prod_id)
            )""")

    def get(self, kind: str, prod_id: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as cx:
            cur = cx.execute(
                "SELECT int_id FROM id_map WHERE kind=? AND prod_id=?", (kind, prod_id)
            )
            r = cur.fetchone()
            return r[0] if r else None

    def put(self, kind: str, prod_id: str, int_id: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                "INSERT OR REPLACE INTO id_map(kind, prod_id, int_id, created_at) VALUES(?,?,?,?)",
                (kind, prod_id, int_id, time.time()),
            )
            cx.commit()

    def exists(self, kind: str, prod_id: str) -> bool:
        return self.get(kind, prod_id) is not None


# ---------- OAuth ----------
@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential_jitter(1, 5),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
)
def token(auth_url: str, client_id: str, client_secret: str, scope: str = "") -> str:
    # Determine environment from URL for better error messages
    if "integration" in auth_url:
        env_name = "Integration"
    else:
        env_name = "Production"

    logger.info(f"Fetching OAuth token for {env_name}", url=auth_url)
    data = {"grant_type": "client_credentials"}
    if scope:
        data["scope"] = scope

    try:
        r = httpx.post(
            auth_url, data=data, auth=(client_id, client_secret), timeout=HTTP_TIMEOUT
        )
        r.raise_for_status()
        return r.json()["access_token"]
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            logger.warning(
                f"Rate limited for {env_name}, backing off",
                status_code=e.response.status_code,
            )
            raise  # Let tenacity handle retry
        logger.error(
            f"{env_name} auth failed",
            status_code=e.response.status_code,
            response=e.response.text,
        )
        # Create a more descriptive error
        error_msg = f"{env_name} authentication failed: {e.response.text}"
        if "invalid_client" in e.response.text:
            error_msg += f"\nCheck your {env_name} CLIENT_ID and CLIENT_SECRET"
        raise RuntimeError(error_msg)
    except Exception as e:
        logger.error(f"{env_name} auth error", error=str(e))
        raise RuntimeError(f"{env_name} authentication error: {str(e)}")


def prod_token() -> str:
    return token(AUTH_URL_PROD, CLIENT_ID_PROD, CLIENT_SECRET_PROD)


def int_token() -> str:
    return token(AUTH_URL_INT, CLIENT_ID_INT, CLIENT_SECRET_INT)


# ---------- HTTP helpers ----------
@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential_jitter(1, 5),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
)
def http_get(
    base: str, path: str, bearer: str, params: Dict[str, Any]
) -> Dict[str, Any]:
    # Determine env-specific values
    if base == API_BASE_PROD:
        tenant_id = TENANT_ID_PROD
        app_key = APP_KEY_PROD
        env_name = "Production"
    elif base == API_BASE_INT:
        tenant_id = TENANT_ID_INT
        app_key = APP_KEY_INT
        env_name = "Integration"
    else:
        raise ValueError(f"Unknown API base: {base}")

    url = build_url(base, path, tenant_id)
    logger.debug("Making GET request", url=url, params=params)

    try:
        headers = {
            "Authorization": f"Bearer {bearer}",
            # v2 APIs require App Key; tenant is in path
            "ST-App-Key": app_key,
        }
        r = httpx.get(
            url,
            headers=headers,
            params=params,
            timeout=HTTP_TIMEOUT,
        )

        if r.status_code == 429:
            logger.warning(
                "Rate limited, backing off", url=url, status_code=r.status_code
            )
            raise httpx.HTTPStatusError("Rate limited", request=r.request, response=r)
        elif r.status_code >= 500:
            logger.error(
                "Server error", url=url, status_code=r.status_code, response=r.text
            )
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
    base: str, path: str, bearer: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    # Determine env-specific values
    if base == API_BASE_PROD:
        tenant_id = TENANT_ID_PROD
        app_key = APP_KEY_PROD
        env_name = "Production"
    elif base == API_BASE_INT:
        tenant_id = TENANT_ID_INT
        app_key = APP_KEY_INT
        env_name = "Integration"
    else:
        raise ValueError(f"Unknown API base: {base}")

    url = build_url(base, path, tenant_id)
    logger.debug("Making POST request", url=url, payload_keys=list(payload.keys()))

    try:
        headers = {
            "Authorization": f"Bearer {bearer}",
            "Content-Type": "application/json",
            # v2 APIs require App Key; tenant is in path
            "ST-App-Key": app_key,
        }
        r = httpx.post(
            url,
            headers=headers,
            json=payload,
            timeout=HTTP_TIMEOUT,
        )

        if r.status_code == 429:
            logger.warning(
                "Rate limited, backing off", url=url, status_code=r.status_code
            )
            raise httpx.HTTPStatusError("Rate limited", request=r.request, response=r)

        r.raise_for_status()

        try:
            return r.json()
        except Exception:
            logger.warning(
                "No JSON response from POST", url=url, status_code=r.status_code
            )
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
    cfg: Dict[str, Any], base: str, bearer: str, since: Optional[str]
) -> Iterable[Dict[str, Any]]:
    params = dict(cfg.get("list_params") or {})
    if "pageSize" in params and not params["pageSize"]:
        params["pageSize"] = PAGE_SIZE_DEFAULT

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

        # Pagination handling variants
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

        # Fallback: advance if page appears full
        if (
            "page" in params
            and "pageSize" in params
            and len(items) >= int(params["pageSize"])
        ):
            params["page"] = int(params["page"]) + 1
            continue
        break


# ---------- Field mappers (with Pydantic validation) ----------
def map_item_for_create(src: Dict[str, Any]) -> Dict[str, Any]:
    """Map production item to integration create payload"""
    try:
        item = ItemCreate(
            code=src.get("code") or src.get("itemCode") or f"PROD-{src.get('id')}",
            name=src.get("name") or src.get("description") or "Unknown Item",
            active=bool(src.get("active", True)),
        )
        return item.model_dump()
    except ValidationError as e:
        logger.error("Invalid item data", source_data=src, validation_errors=e.errors())
        raise


def map_po_for_create(src: Dict[str, Any], xlate) -> Dict[str, Any]:
    """Map production PO to integration create payload"""
    lines = []
    for ln in src.get("lines") or []:
        prod_item_id = str(ln.get("itemId"))
        if not prod_item_id:
            continue

        int_item_id_str = xlate("items", prod_item_id)
        if not int_item_id_str:
            logger.warning(
                "Skipping PO line - item not found in integration",
                prod_item_id=prod_item_id,
            )
            continue

        try:
            int_item_id = int(int_item_id_str)
            line = POLineCreate(
                itemId=int_item_id,
                quantity=ln.get("quantity", 0),
                unitCost=ln.get("unitCost"),
            )
            lines.append(line)
        except (ValueError, ValidationError) as e:
            logger.error("Invalid PO line", line_data=ln, error=str(e))
            continue

    if not lines:
        raise ValueError("No valid lines found for PO")

    vendor_id = src.get("vendorId")
    if vendor_id:
        vendor_int_str = xlate("vendors", str(vendor_id))
        vendor_id = int(vendor_int_str) if vendor_int_str else vendor_id

    try:
        po = POCreate(
            vendorId=vendor_id,
            warehouseId=src.get("warehouseId"),
            externalNumber=f"PROD-{src.get('id')}",
            lines=lines,
        )
        return po.model_dump()
    except ValidationError as e:
        logger.error("Invalid PO data", source_data=src, validation_errors=e.errors())
        raise


def map_job_for_create(src: Dict[str, Any], xlate) -> Dict[str, Any]:
    """Map production job to integration create payload"""
    try:
        cust_id = src.get("customerId")
        loc_id = src.get("locationId")

        if cust_id:
            cust_int_str = xlate("customers", str(cust_id))
            cust_id = int(cust_int_str) if cust_int_str else cust_id

        if loc_id:
            loc_int_str = xlate("locations", str(loc_id))
            loc_id = int(loc_int_str) if loc_int_str else loc_id

        job_type_id = src.get("jobTypeId")
        if job_type_id:
            job_type_int_str = xlate("jobTypes", str(job_type_id))
            job_type_id = int(job_type_int_str) if job_type_int_str else job_type_id

        campaign_id = src.get("campaignId")
        if campaign_id:
            campaign_int_str = xlate("campaigns", str(campaign_id))
            campaign_id = int(campaign_int_str) if campaign_int_str else campaign_id

        job = JobCreate(
            customerId=cust_id,
            locationId=loc_id,
            jobTypeId=job_type_id,
            campaignId=campaign_id,
            externalNumber=f"PROD-{src.get('id')}",
            notes=f"Cloned from Prod {src.get('id')}",
        )
        return job.model_dump()
    except ValidationError as e:
        logger.error("Invalid job data", source_data=src, validation_errors=e.errors())
        raise


# ---------- CLI helpers ----------
def ensure_env():
    """Validate required environment variables"""
    required = [
        ("ST_AUTH_URL_PROD", AUTH_URL_PROD),
        ("ST_AUTH_URL_INT", AUTH_URL_INT),
        ("ST_API_BASE_PROD", API_BASE_PROD),
        ("ST_API_BASE_INT", API_BASE_INT),
        ("ST_CLIENT_ID_PROD", CLIENT_ID_PROD),
        ("ST_CLIENT_SECRET_PROD", CLIENT_SECRET_PROD),
        ("ST_CLIENT_ID_INT", CLIENT_ID_INT),
        ("ST_CLIENT_SECRET_INT", CLIENT_SECRET_INT),
        ("ST_TENANT_ID_PROD", TENANT_ID_PROD),
        ("ST_TENANT_ID_INT", TENANT_ID_INT),
        ("ST_APP_KEY_PROD or ST_APP_KEY", APP_KEY_PROD),
        ("ST_APP_KEY_INT or ST_APP_KEY", APP_KEY_INT),
    ]

    missing = [k for k, v in required if not v]
    if missing:
        print_error(f"Missing environment variables: {', '.join(missing)}")
        print_msg("Copy env.example to .env and fill in the values")
        raise click.ClickException("Missing required environment variables")


@click.group()
def cli():
    """ServiceTitan Prod → Integration copier (Jobs, Items, POs)"""
    pass


@cli.command()
def verify():
    """Check env, config, and authenticate to both envs."""
    print_msg("Verifying setup...")

    try:
        ensure_env()
        print_success("Environment variables OK")
    except click.ClickException:
        return

    try:
        cfg = load_config()
        print_success("Configuration file OK")
    except Exception as e:
        print_error(f"Configuration error: {e}")
        return

    try:
        pt = prod_token()
        print_success("Production authentication OK")
    except Exception as e:
        print_error(f"Production auth failed: {e}")
        return

    try:
        it = int_token()
        print_success("Integration authentication OK")
    except Exception as e:
        print_error(f"Integration auth failed: {e}")
        return

    # Test basic API call
    try:
        ent = cfg["entities"]["items"]
        data = http_get(
            API_BASE_PROD, ent["prod_list_path"], pt, {"page": 1, "pageSize": 1}
        )
        print_success("Production API connection OK")
    except Exception as e:
        print_error(f"Production API test failed: {e}")
        return

    print_success("All checks passed! Ready to sync.")


@cli.command()
@click.argument("kind", type=click.Choice(["items", "pos", "jobs"]))
@click.option("--since", help="ISO date (e.g., 2025-08-01)")
@click.option("--limit", type=int, default=0, help="max records; 0 = unlimited")
@click.option("--dry-run", is_flag=True, help="print payloads; don't POST")
@click.option("--verbose", is_flag=True, help="verbose logging")
def sync(kind, since, limit, dry_run, verbose):
    """Copy records from Prod → Integration for a given entity kind."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    print_msg(f"Starting sync: {kind}")

    try:
        ensure_env()
        cfg = load_config()
    except Exception as e:
        print_error(f"Setup error: {e}")
        return

    ent = cfg["entities"][kind]
    mapper = db = None

    try:
        db = IDMapper()
        pt = prod_token()
        it = int_token()

        # Select mapper
        if kind == "items":
            mapper = lambda src: map_item_for_create(src)
        elif kind == "pos":
            mapper = lambda src: map_po_for_create(src, db.get)
        elif kind == "jobs":
            mapper = lambda src: map_job_for_create(src, db.get)
        else:
            print_error(f"Unsupported kind: {kind}")
            return

        processed = 0
        skipped = 0
        created = 0
        errors = 0

        print_msg(f"Syncing {kind}...")
        for src in fetch_all(ent, API_BASE_PROD, pt, since):
            prod_id = str(
                src.get("id") or src.get("guid") or src.get("externalId") or ""
            )
            if not prod_id:
                logger.warning("Skipping record with no ID", source_data=src)
                continue

            # Skip if already synced
            if db.exists(kind, prod_id):
                skipped += 1
                continue

            try:
                payload = mapper(src)

                if dry_run:
                    print_msg(f"DRY RUN - Would create {kind}:")
                    print(json.dumps(payload, indent=2))
                else:
                    created_data = http_post_json(
                        API_BASE_INT, ent["int_create_path"], it, payload
                    )
                    int_id = str(
                        created_data.get("id")
                        or created_data.get("guid")
                        or created_data.get("externalId")
                        or ""
                    )

                    if int_id:
                        db.put(kind, prod_id, int_id)
                        created += 1
                        logger.info(
                            "Created record", kind=kind, prod_id=prod_id, int_id=int_id
                        )
                    else:
                        print_msg(f"Warning: no id returned for Prod {prod_id}")
                        errors += 1

            except Exception as e:
                logger.error(
                    "Failed to process record", kind=kind, prod_id=prod_id, error=str(e)
                )
                errors += 1
                continue

            processed += 1

            # Rate limiting - small delay between requests
            time.sleep(0.1)

            if limit and processed >= limit:
                break

        # Summary
        print_msg("\nSync Summary:")
        print_msg(f"  Processed: {processed}")
        print_msg(f"  Created: {created}")
        print_msg(f"  Skipped (already exists): {skipped}")
        print_msg(f"  Errors: {errors}")

        if dry_run:
            print_msg("DRY RUN - No records were actually created")
        else:
            print_success("Sync completed!")

    except Exception as e:
        print_error(f"Sync failed: {e}")
        logger.exception("Sync error")


if __name__ == "__main__":
    cli()
