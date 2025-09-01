from __future__ import annotations

import os

from dotenv import load_dotenv
from pydantic import BaseModel

# Load .env at import so env vars are available early
load_dotenv()


class RawSettings(BaseModel):
    # Auth endpoints
    AUTH_URL_PROD: str | None = None
    AUTH_URL_INT: str | None = None

    # API bases
    API_BASE_PROD: str | None = None
    API_BASE_INT: str | None = None

    # OAuth clients
    CLIENT_ID_PROD: str | None = None
    CLIENT_SECRET_PROD: str | None = None
    CLIENT_ID_INT: str | None = None
    CLIENT_SECRET_INT: str | None = None

    # Tenant IDs
    TENANT_ID_PROD: str | None = None
    TENANT_ID_INT: str | None = None

    # App Keys (v2 APIs)
    APP_KEY_PROD: str | None = None
    APP_KEY_INT: str | None = None

    # Optional / tuning
    DB_PATH: str = "stsync.sqlite3"
    PAGE_SIZE_DEFAULT: int = 200
    HTTP_TIMEOUT: int = 30

    # Optional fallbacks for PO creation
    ST_DEFAULT_WAREHOUSE_ID_INT: int | None = None
    ST_DEFAULT_BUSINESS_UNIT_ID_INT: int | None = None

    # Optional ship-to override
    ST_SHIPTO_STREET: str | None = None
    ST_SHIPTO_UNIT: str | None = None
    ST_SHIPTO_CITY: str | None = None
    ST_SHIPTO_STATE: str | None = None
    ST_SHIPTO_ZIP: str | None = None
    ST_SHIPTO_COUNTRY: str | None = "US"


class SettingsStrict(BaseModel):
    AUTH_URL_PROD: str
    AUTH_URL_INT: str
    API_BASE_PROD: str
    API_BASE_INT: str
    CLIENT_ID_PROD: str
    CLIENT_SECRET_PROD: str
    CLIENT_ID_INT: str
    CLIENT_SECRET_INT: str
    TENANT_ID_PROD: str
    TENANT_ID_INT: str
    APP_KEY_PROD: str
    APP_KEY_INT: str

    DB_PATH: str = "stsync.sqlite3"
    PAGE_SIZE_DEFAULT: int = 200
    HTTP_TIMEOUT: int = 30

    ST_DEFAULT_WAREHOUSE_ID_INT: int | None = None
    ST_DEFAULT_BUSINESS_UNIT_ID_INT: int | None = None
    ST_SHIPTO_STREET: str | None = None
    ST_SHIPTO_UNIT: str | None = None
    ST_SHIPTO_CITY: str | None = None
    ST_SHIPTO_STATE: str | None = None
    ST_SHIPTO_ZIP: str | None = None
    ST_SHIPTO_COUNTRY: str | None = "US"


_cache: RawSettings | None = None


def _read_env_dict() -> dict:
    # Support global fallback for ST_APP_KEY_*
    app_key_global = os.getenv("ST_APP_KEY")
    return {
        # Auth
        "AUTH_URL_PROD": os.getenv("ST_AUTH_URL_PROD"),
        "AUTH_URL_INT": os.getenv("ST_AUTH_URL_INT"),
        # API Bases
        "API_BASE_PROD": os.getenv("ST_API_BASE_PROD"),
        "API_BASE_INT": os.getenv("ST_API_BASE_INT"),
        # OAuth
        "CLIENT_ID_PROD": os.getenv("ST_CLIENT_ID_PROD"),
        "CLIENT_SECRET_PROD": os.getenv("ST_CLIENT_SECRET_PROD"),
        "CLIENT_ID_INT": os.getenv("ST_CLIENT_ID_INT"),
        "CLIENT_SECRET_INT": os.getenv("ST_CLIENT_SECRET_INT"),
        # Tenants
        "TENANT_ID_PROD": os.getenv("ST_TENANT_ID_PROD"),
        "TENANT_ID_INT": os.getenv("ST_TENANT_ID_INT"),
        # App keys
        "APP_KEY_PROD": os.getenv("ST_APP_KEY_PROD") or app_key_global,
        "APP_KEY_INT": os.getenv("ST_APP_KEY_INT") or app_key_global,
        # DB + tuning
        "DB_PATH": os.getenv("STSYNC_DB", "stsync.sqlite3"),
        "PAGE_SIZE_DEFAULT": int(os.getenv("ST_PAGE_SIZE", "200")),
        "HTTP_TIMEOUT": int(os.getenv("ST_HTTP_TIMEOUT", "30")),
        # Optional fallbacks
        "ST_DEFAULT_WAREHOUSE_ID_INT": (
            int(os.getenv("ST_DEFAULT_WAREHOUSE_ID_INT"))
            if os.getenv("ST_DEFAULT_WAREHOUSE_ID_INT")
            else None
        ),
        "ST_DEFAULT_BUSINESS_UNIT_ID_INT": (
            int(os.getenv("ST_DEFAULT_BUSINESS_UNIT_ID_INT"))
            if os.getenv("ST_DEFAULT_BUSINESS_UNIT_ID_INT")
            else None
        ),
        # Optional ship-to
        "ST_SHIPTO_STREET": os.getenv("ST_SHIPTO_STREET"),
        "ST_SHIPTO_UNIT": os.getenv("ST_SHIPTO_UNIT"),
        "ST_SHIPTO_CITY": os.getenv("ST_SHIPTO_CITY"),
        "ST_SHIPTO_STATE": os.getenv("ST_SHIPTO_STATE"),
        "ST_SHIPTO_ZIP": os.getenv("ST_SHIPTO_ZIP"),
        "ST_SHIPTO_COUNTRY": os.getenv("ST_SHIPTO_COUNTRY", "US"),
    }


def get_settings() -> RawSettings:
    global _cache
    if _cache is None:
        _cache = RawSettings(**_read_env_dict())
    return _cache


def require_settings() -> SettingsStrict:
    """Return validated settings; raises ValidationError if any required are missing."""
    data = _read_env_dict()
    return SettingsStrict(**data)


def missing_required_keys() -> list[str]:
    """Return list of missing required env keys for user-friendly errors."""
    required = [
        "ST_AUTH_URL_PROD",
        "ST_AUTH_URL_INT",
        "ST_API_BASE_PROD",
        "ST_API_BASE_INT",
        "ST_CLIENT_ID_PROD",
        "ST_CLIENT_SECRET_PROD",
        "ST_CLIENT_ID_INT",
        "ST_CLIENT_SECRET_INT",
        "ST_TENANT_ID_PROD",
        "ST_TENANT_ID_INT",
    ]
    # App keys can come from env-specific or global
    if not (os.getenv("ST_APP_KEY_PROD") or os.getenv("ST_APP_KEY")):
        required.append("ST_APP_KEY_PROD or ST_APP_KEY")
    if not (os.getenv("ST_APP_KEY_INT") or os.getenv("ST_APP_KEY")):
        required.append("ST_APP_KEY_INT or ST_APP_KEY")

    missing = [k for k in required if os.getenv(k) in (None, "")]
    return missing
