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

import json
import logging
import os
import time
from typing import Any

import click
import structlog
from pydantic import ValidationError

from stsync_auth import int_token, prod_token
from stsync_config import load_config
from stsync_db import IDMapper
from stsync_http import fetch_all, http_get, http_post_json
from stsync_models import ItemCreate, JobCreate, POCreate, POLineCreate
from stsync_settings import get_settings, missing_required_keys, require_settings

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()])
logger = structlog.get_logger()


# Simple console for output
def print_msg(msg):
    print(msg)


def print_error(msg):
    print(f"ERROR: {msg}")


def print_success(msg):
    print(f"SUCCESS: {msg}")


# ---------- (moved) Pydantic Models ----------
# Models are now in stsync_models.py and imported above.


# ---------- Environment Variables (via Pydantic Settings) ----------
_raw = get_settings()
AUTH_URL_PROD = _raw.AUTH_URL_PROD
AUTH_URL_INT = _raw.AUTH_URL_INT
API_BASE_PROD = _raw.API_BASE_PROD
API_BASE_INT = _raw.API_BASE_INT
CLIENT_ID_PROD = _raw.CLIENT_ID_PROD
CLIENT_SECRET_PROD = _raw.CLIENT_SECRET_PROD
CLIENT_ID_INT = _raw.CLIENT_ID_INT
CLIENT_SECRET_INT = _raw.CLIENT_SECRET_INT
TENANT_ID_PROD = _raw.TENANT_ID_PROD
TENANT_ID_INT = _raw.TENANT_ID_INT
APP_KEY_PROD = _raw.APP_KEY_PROD
APP_KEY_INT = _raw.APP_KEY_INT
DB_PATH = _raw.DB_PATH
PAGE_SIZE_DEFAULT = _raw.PAGE_SIZE_DEFAULT
HTTP_TIMEOUT = _raw.HTTP_TIMEOUT


# Config loader and URL helper have been moved to modules.


# IDMapper is now in stsync_db.py


# OAuth helpers moved to stsync_auth.py


# HTTP helpers now live in stsync_http.py


# ---------- Field mappers (with Pydantic validation) ----------
def map_item_for_create(src: dict[str, Any]) -> dict[str, Any]:
    """Map production item to integration create payload"""
    try:
        item = ItemCreate(
            code=src.get("code") or src.get("itemCode") or f"PROD-{src.get('id')}",
            name=src.get("name") or src.get("description") or "Unknown Item",
            description=src.get("description") or src.get("name") or "Unknown Item",
            active=bool(src.get("active", True)),
        )
        return item.model_dump()
    except ValidationError as e:
        logger.error("Invalid item data", source_data=src, validation_errors=e.errors())
        raise


def map_po_for_create(src: dict[str, Any], xlate) -> dict[str, Any]:
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
            # Try to map warehouse if present; else omit so API chooses default if allowed
            warehouseId=(
                int(xlate("warehouses", str(src.get("warehouseId"))))
                if (src.get("warehouseId") and xlate("warehouses", str(src.get("warehouseId"))))
                else None
            ),
            externalNumber=f"PROD-{src.get('id')}",
            lines=lines,
        )
        return po.model_dump()
    except ValidationError as e:
        logger.error("Invalid PO data", source_data=src, validation_errors=e.errors())
        raise


def map_job_for_create(src: dict[str, Any], xlate) -> dict[str, Any]:
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
    missing = missing_required_keys()
    if missing:
        print_error(f"Missing environment variables: {', '.join(missing)}")
        print_msg("Copy env.example to .env and fill in the values")
        raise click.ClickException("Missing required environment variables")
    # Type validation via Pydantic
    try:
        _ = require_settings()
    except Exception as e:
        print_error(f"Invalid environment configuration: {e}")
        raise click.ClickException("Invalid environment configuration") from e


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
        int_token()
        print_success("Integration authentication OK")
    except Exception as e:
        print_error(f"Integration auth failed: {e}")
        return

    # Test basic API call
    try:
        ent = cfg["entities"]["items"]
        _ = http_get(API_BASE_PROD, ent["prod_list_path"], pt, {"page": 1, "pageSize": 1})
        print_success("Production API connection OK")
    except Exception as e:
        print_error(f"Production API test failed: {e}")
        return

    print_success("All checks passed! Ready to sync.")


def _get_prod_po_by_id(po_id: str, bearer: str) -> dict[str, Any]:
    """Fetch a single Purchase Order from Production by ID (v2 path)."""
    # Common v2 path
    path = f"/inventory/v2/tenant/{{tenant}}/purchase-orders/{po_id}"
    return http_get(API_BASE_PROD, path, bearer, params={})


def _ensure_vendor_integration(
    vendor_id: int, pt: str, it: str, db: IDMapper, dry_run: bool
) -> int | None:
    vid = str(vendor_id)
    existing = db.get("vendors", vid)
    if existing:
        try:
            return int(existing)
        except Exception:
            return None

    # Fetch from Prod
    v = http_get(API_BASE_PROD, f"/inventory/v2/tenant/{{tenant}}/vendors/{vendor_id}", pt, {})
    v_name = v.get("name") or v.get("displayName") or f"Vendor {vendor_id}"
    # Try Integration lookup by name to avoid duplicate vendor creation
    try:
        existing_integration_id = _find_integration_vendor_by_name(v_name, it)
    except Exception:
        existing_integration_id = None
    if existing_integration_id:
        db.put("vendors", vid, str(existing_integration_id))
        return int(existing_integration_id)
    # Build richer payload to satisfy required fields in Integration
    payload: dict[str, Any] = {
        "name": v.get("name") or v.get("displayName") or f"Vendor {vendor_id}",
        "externalNumber": v.get("externalNumber") or f"PROD-{vendor_id}",
        "active": bool(v.get("active", True)),
        "taxRate": v.get("taxRate", 0.0),
        "isTruckReplenishment": bool(v.get("isTruckReplenishment", False)),
        "restrictedMobileCreation": bool(v.get("restrictedMobileCreation", False)),
    }
    # Reuse Prod address if present
    addr = v.get("address") or {}
    if addr:
        payload["address"] = addr
    else:
        payload["address"] = {
            "addressLine1": "",
            "addressLine2": "",
            "city": "",
            "state": "",
            "postalCode": "",
            "country": "US",
        }

    if dry_run:
        logger.info("DRY RUN - Would create vendor", payload=payload)
        return None

    created = http_post_json(API_BASE_INT, "/inventory/v2/tenant/{tenant}/vendors", it, payload)
    new_id = created.get("id") or created.get("vendorId")
    if new_id is not None:
        db.put("vendors", vid, str(new_id))
        return int(new_id)
    return None


def _ensure_material_integration(
    item_id: int,
    pt: str,
    it: str,
    db: IDMapper,
    dry_run: bool,
    fallback_code: str | None = None,
    fallback_name: str | None = None,
) -> int | None:
    iid = str(item_id)
    existing = db.get("items", iid)
    if existing:
        try:
            return int(existing)
        except Exception:
            return None

    # Try fetch as material
    try:
        m = http_get(API_BASE_PROD, f"/pricebook/v2/tenant/{{tenant}}/materials/{item_id}", pt, {})
        item = ItemCreate(
            code=m.get("code") or m.get("itemCode") or f"PROD-{item_id}",
            name=m.get("name") or m.get("description") or f"Material {item_id}",
            description=m.get("description") or m.get("name") or f"Material {item_id}",
            active=bool(m.get("active", True)),
        )
    except Exception:
        # Try fetch as equipment
        try:
            m = http_get(
                API_BASE_PROD,
                f"/pricebook/v2/tenant/{{tenant}}/equipment/{item_id}",
                pt,
                {},
            )
            item = ItemCreate(
                code=m.get("code") or m.get("itemCode") or f"PROD-{item_id}",
                name=m.get("name") or m.get("description") or f"Equipment {item_id}",
                description=m.get("description") or m.get("name") or f"Equipment {item_id}",
                active=bool(m.get("active", True)),
            )
        except Exception:
            # Fallback: synthesize from PO line context
            if not (fallback_code or fallback_name):
                raise
            item = ItemCreate(
                code=(fallback_code or f"PROD-{item_id}"),
                name=(fallback_name or f"Material {item_id}"),
                description=(fallback_name or fallback_code or f"Material {item_id}"),
                active=True,
            )

    payload = item.model_dump()

    # If a material with the same code already exists in Integration, reuse it
    try:
        existing_by_code = _find_integration_material_by_code(payload.get("code") or "", it)
    except NameError:
        existing_by_code = None
    if existing_by_code:
        db.put("items", iid, str(existing_by_code))
        return int(existing_by_code)

    if dry_run:
        logger.info("DRY RUN - Would create material", payload=payload)
        return None

    try:
        created = http_post_json(
            API_BASE_INT, "/pricebook/v2/tenant/{tenant}/materials", it, payload
        )
        new_id = created.get("id")
        if new_id is not None:
            db.put("items", iid, str(new_id))
            return int(new_id)
    except Exception as e:
        # If code uniqueness caused 400, append a disambiguator and retry once
        code = payload.get("code") or f"PROD-{item_id}"
        if isinstance(code, str) and "unique" in str(e).lower():
            alt = {**payload, "code": f"{code} - PROD-{item_id}"}
            created = http_post_json(
                API_BASE_INT,
                "/pricebook/v2/tenant/{tenant}/materials",
                it,
                alt,
                allow_wrapper_retry=False,
            )
            new_id = created.get("id")
            if new_id is not None:
                db.put("items", iid, str(new_id))
                return int(new_id)
        raise
    return None


def _find_integration_vendor_by_name(name: str, it: str) -> int | None:
    """Scan Integration vendors and return id by exact name (case-insensitive)."""
    cfg = {
        "prod_list_path": "/inventory/v2/tenant/{tenant}/vendors",
        "list_params": {"page": 1, "pageSize": 200},
        "list_data_key": "data",
        "next_page_key": "hasMore",
    }
    name_l = (name or "").strip().lower()
    if not name_l:
        return None
    for ven in fetch_all(cfg, API_BASE_INT, it, since=None):
        cand = (
            (ven.get("name") or ven.get("displayName") or ven.get("legalName") or "")
            .strip()
            .lower()
        )
        if cand == name_l:
            return ven.get("id")
    return None


def _find_integration_material_by_code(code: str, it: str) -> int | None:
    """Scan Integration materials and return id by exact code (case-insensitive)."""
    cfg = {
        "prod_list_path": "/pricebook/v2/tenant/{tenant}/materials",
        "list_params": {"page": 1, "pageSize": 200},
        "list_data_key": "data",
        "next_page_key": "hasMore",
    }
    code_l = (code or "").strip().lower()
    if not code_l:
        return None
    for m in fetch_all(cfg, API_BASE_INT, it, since=None):
        cand = (m.get("code") or m.get("itemCode") or "").strip().lower()
        if cand == code_l:
            return m.get("id")
    return None


def _find_integration_business_unit_by_name(name: str, it: str) -> int | None:
    """Return Integration businessUnit id by exact name (case-insensitive)."""
    paths = [
        "/crm/v2/tenant/{tenant}/business-units",
        "/settings/v2/tenant/{tenant}/business-units",
    ]
    name_l = (name or "").strip().lower()
    if not name_l:
        return None
    for path in paths:
        try:
            data = http_get(API_BASE_INT, path, it, {"page": 1, "pageSize": 200})
        except Exception:
            continue
        items = data.get("data") or data.get("items") or []
        for bu in items:
            if (bu.get("name") or "").strip().lower() == name_l:
                return bu.get("id")
    return None


def _get_prod_business_unit_name(bu_id: int, pt: str) -> str | None:
    """Look up Production business unit name by id (try CRM and Settings paths)."""
    paths = [
        f"/crm/v2/tenant/{{tenant}}/business-units/{bu_id}",
        f"/settings/v2/tenant/{{tenant}}/business-units/{bu_id}",
    ]
    for path in paths:
        try:
            d = http_get(API_BASE_PROD, path, pt, {})
            name = d.get("name") or (d.get("businessUnit") or {}).get("name")
            if name:
                return str(name)
        except Exception:
            continue
    return None


def _find_integration_warehouse_by_name(name: str, it: str) -> int | None:
    """Scan Integration warehouses and return id for a name match (case-insensitive)."""
    cfg = {
        "prod_list_path": "/inventory/v2/tenant/{tenant}/warehouses",
        "list_params": {"page": 1, "pageSize": 200},
        "list_data_key": "data",
        "next_page_key": "hasMore",
    }
    name_l = (name or "").strip().lower()
    if not name_l:
        return None
    for wh in fetch_all(cfg, API_BASE_INT, it, since=None):
        wh_name = (wh.get("name") or wh.get("displayName") or "").strip().lower()
        if wh_name == name_l:
            return wh.get("id")
    return None


def _normalize_address(addr: dict[str, Any]) -> dict[str, Any]:
    """Map various address shapes to the required keys: street, unit, city, state, zip, country."""
    if not isinstance(addr, dict):
        addr = {}
    street = addr.get("street") or addr.get("addressLine1") or addr.get("address1") or ""
    unit = addr.get("unit") or addr.get("addressLine2") or addr.get("address2") or ""
    city = addr.get("city") or ""
    state = addr.get("state") or addr.get("stateCode") or ""
    zipc = addr.get("zip") or addr.get("postalCode") or ""
    country = addr.get("country") or "US"
    return {
        "street": street,
        "unit": unit,
        "city": city,
        "state": state,
        "zip": zipc,
        "country": country,
    }


def _get_integration_warehouse_info(wh_id: int, it: str) -> dict[str, Any]:
    """Find a warehouse in Integration by id via list scan and return its dict (or {})."""
    cfg = {
        "prod_list_path": "/inventory/v2/tenant/{tenant}/warehouses",
        "list_params": {"page": 1, "pageSize": 200},
        "list_data_key": "data",
        "next_page_key": "hasMore",
    }
    for wh in fetch_all(cfg, API_BASE_INT, it, since=None):
        try:
            if int(wh.get("id")) == int(wh_id):
                return wh
        except Exception:
            continue
    return {}


def _get_integration_po_type_id(bearer: str) -> int | None:
    try:
        data = http_get(
            API_BASE_INT,
            "/inventory/v2/tenant/{tenant}/purchase-order-types",
            bearer,
            {"page": 1, "pageSize": 200},
        )
        kinds = data.get("data") or data.get("items") or []
        for k in kinds:
            nm = (k.get("name") or "").lower()
            if "stock" in nm or "inventory" in nm:
                return k.get("id")
        return kinds[0].get("id") if kinds else None
    except Exception:
        return None


def _ensure_warehouse_integration(
    warehouse_id: int, pt: str, it: str, db: IDMapper, dry_run: bool
) -> int | None:
    wid = str(warehouse_id)
    existing = db.get("warehouses", wid)
    if existing:
        try:
            return int(existing)
        except Exception:
            return None

    # Fetch from Prod
    w = http_get(
        API_BASE_PROD, f"/inventory/v2/tenant/{{tenant}}/warehouses/{warehouse_id}", pt, {}
    )

    w_name = w.get("name") or w.get("displayName") or f"Warehouse {warehouse_id}"

    # Try Integration lookup by name first to avoid duplicates
    try:
        maybe_id = _find_integration_warehouse_by_name(w_name, it)
    except Exception:
        maybe_id = None
    if maybe_id:
        db.put("warehouses", wid, str(maybe_id))
        return int(maybe_id)

    # Build payload; include address if available
    payload: dict[str, Any] = {
        "name": w_name,
        "active": bool(w.get("active", True)),
        "externalNumber": w.get("externalNumber") or f"PROD-{warehouse_id}",
    }
    addr = w.get("address") or {}
    if addr:
        payload["address"] = addr

    if dry_run:
        logger.info("DRY RUN - Would create warehouse", payload=payload)
        return None

    created = http_post_json(API_BASE_INT, "/inventory/v2/tenant/{tenant}/warehouses", it, payload)
    new_id = created.get("id") or created.get("warehouseId")
    if new_id is not None:
        db.put("warehouses", wid, str(new_id))
        return int(new_id)
    return None


@cli.command("copy-po")
@click.option("--id", "po_id", required=True, help="Production PO ID to copy")
@click.option(
    "--default-warehouse-id",
    type=int,
    default=None,
    help="Fallback Integration warehouse id if source warehouse is missing",
)
@click.option("--dry-run", is_flag=True, help="print payloads; don't POST")
@click.option("--verbose", is_flag=True, help="verbose logging")
def copy_po(po_id, default_warehouse_id, dry_run, verbose):
    """Copy a single PO by ID from Prod to Integration, ensuring dependencies (vendor, materials)."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        ensure_env()
        _ = load_config()
    except Exception as e:
        print_error(f"Setup error: {e}")
        return

    db = IDMapper()
    try:
        pt = prod_token()
        it = int_token()
    except Exception as e:
        print_error(f"Auth error: {e}")
        return

    # Fetch PO from Prod
    try:
        src = _get_prod_po_by_id(po_id, pt)
    except Exception as e:
        print_error(f"Failed to fetch Production PO {po_id}: {e}")
        return

    # Resolve vendor
    vendor_id = src.get("vendorId") or (src.get("vendor") or {}).get("id")
    vendor_int_id: int | None = None
    if vendor_id:
        try:
            vendor_int_id = _ensure_vendor_integration(int(vendor_id), pt, it, db, dry_run)
        except Exception as e:
            print_error(f"Vendor ensure failed: {e}")
            return

    # Resolve warehouse (best-effort)
    warehouse_id = src.get("warehouseId") or (src.get("warehouse") or {}).get("id")
    warehouse_name = (src.get("warehouse") or {}).get("name") or ""
    wh_int_id: int | None = None
    if warehouse_id:
        try:
            wh_int_id = _ensure_warehouse_integration(int(warehouse_id), pt, it, db, dry_run)
        except Exception:
            wh_int_id = None
    if wh_int_id is None and warehouse_name:
        try:
            wh_int_id = _find_integration_warehouse_by_name(warehouse_name, it)
        except Exception:
            wh_int_id = None
    if wh_int_id is None and default_warehouse_id:
        wh_int_id = default_warehouse_id

    # Resolve line items → materials
    lines_src = src.get("items") or src.get("lines") or []
    lines_payload: list[dict[str, Any]] = []  # for Integration POST (items)
    lines_payload_dry: list[dict[str, Any]] = []
    for ln in lines_src:
        # Prefer explicit pricebook identifiers; never use the PO line's own id
        src_item_id = (
            ln.get("itemId")
            or ln.get("pricebookItemId")
            or ln.get("materialId")
            or ln.get("equipmentId")
            or ln.get("skuId")
        )
        if not src_item_id:
            logger.warning("Skipping PO line with no item id", line=ln)
            continue
        try:
            code_hint = ln.get("code") or ln.get("itemCode") or ln.get("skuCode") or ln.get("sku")
            name_hint = ln.get("name") or ln.get("skuName") or ln.get("description")
            int_item_id = _ensure_material_integration(
                int(src_item_id), pt, it, db, dry_run, code_hint, name_hint
            )
        except Exception as e:
            print_error(f"Material ensure failed for {src_item_id}: {e}")
            return
        qty = ln.get("quantity") or ln.get("qty") or 0
        unit_cost = ln.get("unitCost") or ln.get("unitPrice") or ln.get("cost")
        # Include fields some tenants require: skuId and cost
        lines_payload.append(
            {
                "itemId": int_item_id,
                "skuId": int_item_id,
                "quantity": qty,
                "quantityOrdered": qty,
                "unitCost": unit_cost,
                "cost": unit_cost,
                **({"description": name_hint} if name_hint else {}),
                **(
                    {"vendorPartNumber": ln.get("vendorPartNumber")}
                    if ln.get("vendorPartNumber")
                    else {}
                ),
            }
        )
        lines_payload_dry.append(
            {
                "itemId": int_item_id if int_item_id is not None else f"(from Prod {src_item_id})",
                "quantity": qty,
                "unitCost": unit_cost,
                "hints": {"code": code_hint, "name": name_hint},
            }
        )

    # Build Integration PO payload
    # Purchase Order Type (required by v2)
    type_id = _get_integration_po_type_id(it)
    if not type_id:
        print_error("Could not determine a purchase order typeId in Integration")
        return

    # Resolve shipTo warehouse id (prefer mapped/name; then CLI flag; then env fallback)
    if not wh_int_id and default_warehouse_id:
        wh_int_id = default_warehouse_id
    if not wh_int_id:
        env_wh = os.getenv("ST_DEFAULT_WAREHOUSE_ID_INT")
        if env_wh:
            try:
                wh_int_id = int(env_wh)
            except Exception:
                wh_int_id = wh_int_id
    if not wh_int_id:
        print_error(
            "No Integration warehouse id resolved. Provide --default-warehouse-id or set ST_DEFAULT_WAREHOUSE_ID_INT."
        )
        return

    # Resolve Business Unit: prefer Production BU name mapped to Integration
    bu_int_id: int | None = None
    # Try to get name from Production PO payload
    bu_name = None
    bu_obj = src.get("businessUnit") if isinstance(src.get("businessUnit"), dict) else None
    if bu_obj:
        bu_name = bu_obj.get("name")
    bu_id_prod = src.get("businessUnitId") or (bu_obj or {}).get("id")
    if not bu_name and bu_id_prod:
        try:
            bu_name = _get_prod_business_unit_name(int(bu_id_prod), pt)
        except Exception:
            bu_name = None
    if bu_name:
        try:
            bu_int_id = _find_integration_business_unit_by_name(bu_name, it)
        except Exception:
            bu_int_id = None
    if not bu_int_id:
        # env fallback already handled later when building payload
        bu_env = os.getenv("ST_DEFAULT_BUSINESS_UNIT_ID_INT")
        if bu_env and bu_env.isdigit():
            bu_int_id = int(bu_env)

    # Optional BU fallback from env
    bu_id = bu_int_id if bu_int_id is not None else None

    # Fetch Integration warehouse details for shipTo address/description via list scan
    wh_details = _get_integration_warehouse_info(int(wh_int_id), it) if wh_int_id else {}
    # Build ship-to address (env fallback if missing)
    addr_env = {
        "street": os.getenv("ST_SHIPTO_STREET", ""),
        "unit": os.getenv("ST_SHIPTO_UNIT", ""),
        "city": os.getenv("ST_SHIPTO_CITY", ""),
        "state": os.getenv("ST_SHIPTO_STATE", ""),
        "zip": os.getenv("ST_SHIPTO_ZIP", ""),
        "country": os.getenv("ST_SHIPTO_COUNTRY", "US"),
    }
    addr_norm = _normalize_address(wh_details.get("address") or {})
    # Overlay env values if provided
    for k, v in addr_env.items():
        if v:
            addr_norm[k] = v

    po_body = {
        "vendorId": int(vendor_int_id)
        if vendor_int_id is not None
        else int(vendor_id)
        if vendor_id
        else 0,
        "date": src.get("createdOn")
        or src.get("orderedOn")
        or src.get("modifiedOn")
        or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "typeId": int(type_id),
        "externalNumber": f"PROD-{src.get('id') or po_id}",
        # Some tenants require both a top-level inventoryLocationId and shipTo object
        "inventoryLocationId": int(wh_int_id),
        "shipTo": {
            "inventoryLocationId": int(wh_int_id),
            "description": wh_details.get("name")
            or wh_details.get("displayName")
            or "Ship to Integration Warehouse",
            "address": addr_norm,
        },
        "tax": 0,
        "shipping": 0,
        "requiredOn": src.get("requiredOn")
        or src.get("expectedOn")
        or src.get("createdOn")
        or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "businessUnitId": int(bu_id) if bu_id else None,
        "impactsTechnicianPayroll": False,
        "items": [{k: v for k, v in itm.items() if v is not None} for itm in lines_payload],
    }

    # Drop None fields
    po_body = {k: v for k, v in po_body.items() if v is not None}

    if dry_run:
        print_msg("DRY RUN - Would create PO in Integration with payload:")
        print(json.dumps(po_body, indent=2))
        return

    # Create PO in Integration
    try:
        # POST plain body (no wrapper) for purchase-orders
        created = http_post_json(
            API_BASE_INT,
            "/inventory/v2/tenant/{tenant}/purchase-orders",
            it,
            po_body,
            allow_wrapper_retry=False,
        )
        int_po_id = created.get("id") or created.get("purchaseOrderId")
        if int_po_id:
            db.put("pos", str(src.get("id") or po_id), str(int_po_id))
            print_success(f"Created Integration PO {int_po_id} for Prod {po_id}")
        else:
            print_msg("Warning: Create PO succeeded but no id returned")
    except Exception as e:
        print_error(f"Create Integration PO failed: {e}")
        return


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

        # Select mapper or specialized flow
        if kind == "items":

            def item_mapper(src):
                return map_item_for_create(src)

            mapper = item_mapper
        elif kind == "pos":
            mapper = None  # handled by v2 flow below
        elif kind == "jobs":

            def job_mapper(src):
                return map_job_for_create(src, db.get)

            mapper = job_mapper
        else:
            print_error(f"Unsupported kind: {kind}")
            return

        processed = 0
        skipped = 0
        created = 0
        errors = 0

        print_msg(f"Syncing {kind}...")

        # Pre-fetch optional constants
        po_type_id_cache: int | None = None

        for src in fetch_all(ent, API_BASE_PROD, pt, since):
            prod_id = str(src.get("id") or src.get("guid") or src.get("externalId") or "")
            if not prod_id:
                logger.warning("Skipping record with no ID", source_data=src)
                continue

            # Skip if already synced
            if db.exists(kind, prod_id):
                skipped += 1
                continue

            try:
                if kind != "pos":
                    # Legacy flow for items/jobs
                    payload = mapper(src)  # type: ignore
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
                            logger.info("Created record", kind=kind, prod_id=prod_id, int_id=int_id)
                        else:
                            print_msg(f"Warning: no id returned for Prod {prod_id}")
                            errors += 1
                else:
                    # v2 Purchase Order flow
                    # Ensure vendor
                    vendor_id = src.get("vendorId") or (src.get("vendor") or {}).get("id")
                    vendor_int_id: int | None = None
                    if vendor_id:
                        vendor_int_id = _ensure_vendor_integration(
                            int(vendor_id), pt, it, db, dry_run
                        )

                    # Ensure warehouse (from id or name, else env/default)
                    warehouse_id = src.get("warehouseId") or (src.get("warehouse") or {}).get("id")
                    warehouse_name = (src.get("warehouse") or {}).get("name") or ""
                    wh_int_id: int | None = None
                    if warehouse_id:
                        wh_int_id = _ensure_warehouse_integration(
                            int(warehouse_id), pt, it, db, dry_run
                        )
                    if wh_int_id is None and warehouse_name:
                        wh_int_id = _find_integration_warehouse_by_name(warehouse_name, it)
                    if wh_int_id is None:
                        env_wh = os.getenv("ST_DEFAULT_WAREHOUSE_ID_INT")
                        if env_wh and env_wh.isdigit():
                            wh_int_id = int(env_wh)

                    # Resolve BU
                    bu_int_id: int | None = None
                    bu_obj = (
                        src.get("businessUnit")
                        if isinstance(src.get("businessUnit"), dict)
                        else None
                    )
                    bu_name = bu_obj.get("name") if bu_obj else None
                    bu_id_prod = src.get("businessUnitId") or (bu_obj or {}).get("id")
                    if not bu_name and bu_id_prod:
                        bu_name = _get_prod_business_unit_name(int(bu_id_prod), pt)
                    if bu_name:
                        bu_int_id = _find_integration_business_unit_by_name(bu_name, it)
                    if not bu_int_id:
                        bu_env = os.getenv("ST_DEFAULT_BUSINESS_UNIT_ID_INT")
                        if bu_env and bu_env.isdigit():
                            bu_int_id = int(bu_env)

                    # Items → ensure materials + build items[]
                    lines_src = src.get("items") or src.get("lines") or []
                    items_payload: list[dict[str, Any]] = []
                    for ln in lines_src:
                        src_item_id = (
                            ln.get("itemId")
                            or ln.get("pricebookItemId")
                            or ln.get("materialId")
                            or ln.get("equipmentId")
                            or ln.get("skuId")
                        )
                        if not src_item_id:
                            continue
                        code_hint = (
                            ln.get("code")
                            or ln.get("itemCode")
                            or ln.get("skuCode")
                            or ln.get("sku")
                        )
                        name_hint = ln.get("name") or ln.get("skuName") or ln.get("description")
                        int_item_id = _ensure_material_integration(
                            int(src_item_id), pt, it, db, dry_run, code_hint, name_hint
                        )
                        qty = ln.get("quantity") or ln.get("qty") or 0
                        unit_cost = ln.get("unitCost") or ln.get("unitPrice") or ln.get("cost")
                        items_payload.append(
                            {
                                "itemId": int_item_id,
                                "skuId": int_item_id,
                                "quantity": qty,
                                "quantityOrdered": qty,
                                "unitCost": unit_cost,
                                "cost": unit_cost,
                                **({"description": name_hint} if name_hint else {}),
                                **(
                                    {"vendorPartNumber": ln.get("vendorPartNumber")}
                                    if ln.get("vendorPartNumber")
                                    else {}
                                ),
                            }
                        )

                    if not items_payload:
                        logger.warning("Skipping PO with no items", prod_id=prod_id)
                        skipped += 1
                        continue

                    # PO type id (cache)
                    if po_type_id_cache is None:
                        po_type_id_cache = _get_integration_po_type_id(it)
                    if not po_type_id_cache:
                        print_error("Could not determine a purchase order typeId in Integration")
                        errors += 1
                        continue

                    # Warehouse needed
                    if not wh_int_id:
                        print_error(
                            "No Integration warehouse id resolved; set ST_DEFAULT_WAREHOUSE_ID_INT"
                        )
                        errors += 1
                        continue

                    # Warehouse details for shipTo
                    wh_details = (
                        _get_integration_warehouse_info(int(wh_int_id), it) if wh_int_id else {}
                    )
                    addr_env = {
                        "street": os.getenv("ST_SHIPTO_STREET", ""),
                        "unit": os.getenv("ST_SHIPTO_UNIT", ""),
                        "city": os.getenv("ST_SHIPTO_CITY", ""),
                        "state": os.getenv("ST_SHIPTO_STATE", ""),
                        "zip": os.getenv("ST_SHIPTO_ZIP", ""),
                        "country": os.getenv("ST_SHIPTO_COUNTRY", "US"),
                    }
                    addr_norm = _normalize_address(wh_details.get("address") or {})
                    for k, v in addr_env.items():
                        if v:
                            addr_norm[k] = v

                    po_body = {
                        "vendorId": int(vendor_int_id)
                        if vendor_int_id is not None
                        else int(vendor_id)
                        if vendor_id
                        else 0,
                        "date": src.get("createdOn")
                        or src.get("orderedOn")
                        or src.get("modifiedOn")
                        or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "typeId": int(po_type_id_cache),
                        "externalNumber": f"PROD-{src.get('id') or prod_id}",
                        "inventoryLocationId": int(wh_int_id),
                        "shipTo": {
                            "inventoryLocationId": int(wh_int_id),
                            "description": wh_details.get("name")
                            or wh_details.get("displayName")
                            or "Ship to Integration Warehouse",
                            "address": addr_norm,
                        },
                        "tax": 0,
                        "shipping": 0,
                        "requiredOn": src.get("requiredOn")
                        or src.get("expectedOn")
                        or src.get("createdOn")
                        or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "businessUnitId": int(bu_int_id) if bu_int_id else None,
                        "impactsTechnicianPayroll": False,
                        "items": [
                            {k: v for k, v in itm.items() if v is not None} for itm in items_payload
                        ],
                    }
                    po_body = {k: v for k, v in po_body.items() if v is not None}

                    if dry_run:
                        print_msg("DRY RUN - Would create pos:")
                        print(json.dumps(po_body, indent=2))
                    else:
                        created_data = http_post_json(
                            API_BASE_INT,
                            "/inventory/v2/tenant/{tenant}/purchase-orders",
                            it,
                            po_body,
                            allow_wrapper_retry=False,
                        )
                        int_id = str(created_data.get("id") or "")
                        if int_id:
                            db.put(kind, prod_id, int_id)
                            created += 1
                            logger.info("Created record", kind=kind, prod_id=prod_id, int_id=int_id)
                        else:
                            print_msg(f"Warning: no id returned for Prod {prod_id}")
                            errors += 1

            except Exception as e:
                logger.error("Failed to process record", kind=kind, prod_id=prod_id, error=str(e))
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
