# CLAUDE.md

Project-specific instructions for AI assistants working on the ServiceTitan Syncer codebase.

---

## PROJECT OVERVIEW

**STSync** is a Python CLI tool that copies Items, Purchase Orders, and Jobs from ServiceTitan Production to Integration environments. The tool maintains an SQLite ID crosswalk to map production IDs to integration IDs, ensuring idempotent operations.

---

## ARCHITECTURE

### Module Organization

The codebase follows a modular architecture with clear separation of concerns:

- `stsync.py` - Main CLI entry point (Click-based commands)
- `stsync_settings.py` - Pydantic settings loader and validators
- `stsync_config.py` - Configuration file schema and loader
- `stsync_http.py` - HTTP helpers (GET, POST, pagination, retries)
- `stsync_auth.py` - OAuth2 client_credentials authentication
- `stsync_db.py` - SQLite ID crosswalk mapper
- `stsync_models.py` - Pydantic models for API payloads

### Key Design Patterns

1. **ID Translation**: All production IDs are mapped to integration IDs via SQLite before creating records
2. **Dependency Resolution**: Automatically ensures dependencies exist (vendors, items, warehouses) before creating POs
3. **Idempotency**: Checks SQLite crosswalk before creating records to prevent duplicates
4. **Retry Logic**: Uses Tenacity for exponential backoff on rate limits and network errors
5. **Structured Logging**: Uses structlog for detailed operation tracking

---

## SERVICETITAN API SPECIFICS

### Authentication

- OAuth2 `client_credentials` flow
- Separate credentials for Production and Integration environments
- Tokens cached in-memory for the session duration

### API Quirks

1. **Request Wrapper**: Purchase Order creation endpoint requires `{"request": {...}}` wrapper
2. **Required Fields**:
   - `vendorPartNumber` is required for all PO line items (use empty string if not available)
   - `typeId` is required for POs (fetched from purchase-order-types endpoint)
   - `businessUnitId` may be required depending on tenant configuration
3. **Business Unit Endpoints**: Try `/settings/v2/tenant/{tenant}/business-units` if `/crm/v2/...` returns 404
4. **Tenant ID Injection**: All paths use `{tenant}` placeholder that gets replaced at runtime

### HTTP Headers

All API requests must include:
- `Authorization: Bearer {token}`
- `ST-App-Key: {app_key}`
- `Content-Type: application/json` (for POST)

---

## CODING STANDARDS

### General Rules

- ALWAYS use `underscore_case` for filenames
- NEVER insert emoji into any file (use unicode codepoints instead)
- ALWAYS respect ignored file patterns in `.gitignore`

### Excluded Files

NEVER scan files matching these patterns:
- `node_modules/`
- `.git/`
- `*.env`
- `.env.*`
- `.venv/`
- `*.lock`

### Python Style

- ALWAYS use Python 3.11+ features
- ALWAYS use absolute imports
- ALWAYS import modules at the top of the file
- NEVER import modules from within a function or class
- ALWAYS use `underscore_case` for filenames
- ALWAYS use `uv` to manage project and package dependencies
- ALWAYS use `ruff` to lint the code
- ALWAYS use `pyright` to type check the code
- ALWAYS respect Ruff linter rules in `ruff.toml`
- ALWAYS respect Pyright rules in `pyrightconfig.json`

### Type Annotations

- ALWAYS add type hints to all function signatures
- NEVER use the `Any` static type
- NEVER use `dict[..., ...]` or `typing.Dict[..., ...]` (use `TypedDict` instead)
- ALWAYS add a `typing.TypedDict` type definition to all dictionaries
- NEVER use `typing.Tuple[...]` (use `tuple[]` instead)
- NEVER use the `object` static type (always specify specific ABC abstract base class or `class`)
- ALWAYS use the `Final` static type for all variables (renamed to `Fin`)
- NEVER add return type annotations to functions, unless they are recursive or type stubs
- NEVER conditionally import types modules with `if TYPE_CHECKING:`

### Code Style

- ALWAYS use Pydantic models for data validation
- ALWAYS prefer a functional style of code
- ALWAYS avoid using mutable state
- ALWAYS avoid using mutable variables
- NEVER use `for` loops (use list comprehensions or functional patterns)
- NEVER use `let` (Python doesn't have `let`, use `const` pattern)
- NEVER add `print()` statements
- NEVER insert emoji into code (use unicode codepoints instead)
- NEVER add code comments, unless specifically requested

### Error Handling

- NEVER use try/catch (except for external vendor functions which will throw errors under normal circumstances)
- Log errors with structured logging, never swallow exceptions
- Use Tenacity retry decorators for transient failures
- Validate data with Pydantic before making API calls
- Provide actionable error messages to users

### Functions

- Keep functions under 50 lines
- Single responsibility principle
- Use absolute imports
- Type all parameters and return values

---

## TESTING WORKFLOW

### Before Making Changes

1. Run `uv run stsync verify` to ensure environment is configured
2. Use `--dry-run` flag to validate payloads before real operations
3. Test with `--limit 1` to process minimal records

### Common Test Commands

```bash
# Verify setup
uv run stsync verify

# Dry-run single PO
uv run stsync copy-po --id <PO_ID> --dry-run --verbose

# Test small batch
uv run stsync sync items --since 2025-08-01 --limit 5 --dry-run
```

---

## COMMON TASKS

### Adding New Entity Types

1. Add endpoint configuration to `stsync.config.json`
2. Create Pydantic model in `stsync_models.py`
3. Implement mapper function in `stsync.py`
4. Add CLI command or extend existing sync command
5. Update README.md with usage examples

### Modifying API Payloads

1. Check ServiceTitan API documentation for required fields
2. Update Pydantic model in `stsync_models.py`
3. Update mapper function in `stsync.py`
4. Test with `--dry-run` to verify payload structure
5. Test with `--limit 1` for real API call

### Debugging API Errors

1. Enable verbose logging: `--verbose` flag
2. Check structured logs for request/response details
3. Verify required fields are present in payload
4. Check if endpoint requires request wrapper
5. Verify tenant has required master data (vendors, warehouses, etc.)

---

## DEPENDENCY MANAGEMENT

### Sync Order Matters

Dependencies must be synced in this order:

1. **Items** (no dependencies)
2. **Vendors** (auto-created when syncing POs)
3. **Warehouses** (auto-created when syncing POs)
4. **Purchase Orders** (requires items + vendors + warehouses)
5. **Jobs** (requires customers + locations + job types + campaigns)

### Auto-Resolution

The `copy-po` command automatically:
- Fetches vendor from production and creates in integration
- Fetches items from production and creates in integration
- Fetches warehouse from production and creates in integration
- Maps all IDs via SQLite crosswalk

---

## ENVIRONMENT VARIABLES

### Required Variables

```bash
ST_AUTH_URL_PROD          # Production OAuth endpoint
ST_AUTH_URL_INT           # Integration OAuth endpoint
ST_API_BASE_PROD          # Production API base URL
ST_API_BASE_INT           # Integration API base URL
ST_CLIENT_ID_PROD         # Production OAuth client ID
ST_CLIENT_SECRET_PROD     # Production OAuth client secret
ST_CLIENT_ID_INT          # Integration OAuth client ID
ST_CLIENT_SECRET_INT      # Integration OAuth client secret
ST_TENANT_ID_PROD         # Production tenant ID
ST_TENANT_ID_INT          # Integration tenant ID
ST_APP_KEY_PROD           # Production app key (v2 APIs)
ST_APP_KEY_INT            # Integration app key (v2 APIs)
```

### Optional Variables

```bash
ST_DEFAULT_WAREHOUSE_ID_INT     # Fallback warehouse ID
ST_DEFAULT_BUSINESS_UNIT_ID_INT # Fallback business unit ID
ST_PO_TYPE_KEYWORDS             # Keywords for PO type selection (default: "stock,inventory")
ST_PAGE_SIZE                    # API pagination size (default: 200)
ST_HTTP_TIMEOUT                 # HTTP timeout in seconds (default: 30)
STSYNC_DB                       # SQLite database path (default: stsync.sqlite3)
```

---

## COMMON PITFALLS

### API Payload Issues

- **Missing `request` wrapper**: PO creation requires `{"request": {...}}`
- **Missing `vendorPartNumber`**: Required for all PO line items (use empty string)
- **Invalid dates**: Ensure `requiredOn >= date` for POs
- **Missing `typeId`**: Required for POs (fetch from purchase-order-types endpoint)

### ID Mapping Issues

- **Wrong entity kind**: Use correct kind string ('items', 'pos', 'vendors', 'warehouses')
- **String vs int IDs**: Always store IDs as strings in SQLite, convert when needed
- **Missing crosswalk**: Check if item/vendor exists before creating PO

### Business Unit Handling

- **404 on CRM endpoint**: Try `/settings/v2/tenant/{tenant}/business-units` instead
- **Missing BU in integration**: May need to set `ST_DEFAULT_BUSINESS_UNIT_ID_INT`
- **BU required vs optional**: Varies by tenant configuration

---

## TYPE CHECKING

Run type checking before committing:

```bash
uv run mypy .
```

Strict mode is enabled:
- `disallow_untyped_defs`
- `no_implicit_optional`
- `warn_redundant_casts`
- `warn_unused_ignores`

---

## LOGGING

### Structured Logging Format

Use structlog with context:

```python
logger.info("Created record", kind="items", prod_id="123", int_id="456")
logger.error("HTTP error", url=url, status_code=404, response=body)
logger.warning("Skipping record", reason="missing vendor", prod_id="789")
```

### Log Levels

- `DEBUG`: HTTP requests, pagination details (use `--verbose`)
- `INFO`: Operation progress, record creation
- `WARNING`: Skipped records, fallback behavior
- `ERROR`: API errors, validation failures

---

## EXTENDING THE TOOL

### Adding New Commands

1. Add Click command decorator to function in `stsync.py`
2. Use existing patterns for auth, config loading, and error handling
3. Follow dry-run and verbose flag conventions
4. Add structured logging for operations
5. Update README.md with command documentation

### Adding New Endpoints

1. Check if endpoint uses v2 API format with `{tenant}` placeholder
2. Add to `stsync.config.json` if paginated list endpoint
3. Use `http_get` or `http_post_json` from `stsync_http.py`
4. Handle request wrapper if required (check API docs)

---

## NOTES

- This tool is for authorized ServiceTitan development environments only
- Always test with `--dry-run` before real operations
- SQLite database (`stsync.sqlite3`) tracks all ID mappings
- Re-running sync operations is safe (idempotent)
- Use `--verbose` for debugging API issues
