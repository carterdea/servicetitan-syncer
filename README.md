# STSync (Prod → Integration)

**What it does:** Copies Items, POs, and Jobs from ServiceTitan Production into Integration, with minimal mapping and an ID crosswalk in SQLite.

## Features

- ✅ OAuth2 client_credentials authentication
- ✅ Config-driven endpoints (stsync.config.json)
- ✅ SQLite ID crosswalk: (kind, prod_id) → integration_id
- ✅ Robust pagination, retries, dry-run, since filter, limit
- ✅ Pydantic data validation for API payloads
- ✅ Structured logging with Rich console output
- ✅ Rate limiting protection
- ✅ Idempotent operations (skips already-synced records)

## Setup (UV only)

1. Create a virtual env (Python 3.11 recommended):
   ```bash
   uv venv --python 3.11
   source .venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   uv pip install -r requirements.txt
   ```

3. Configure env:
   ```bash
   cp env.example .env
   # Fill in OAuth clients + API bases for both environments
   ```

## Verify Setup

```bash
python stsync.py verify
```

This will:
- Check environment variables
- Validate configuration file
- Test authentication to both environments
- Make a test API call

## Dry-run Small Windows First

```bash
# Test with small limits first
python stsync.py sync items --since 2025-08-01 --limit 5 --dry-run
python stsync.py sync pos   --since 2025-08-01 --limit 3 --dry-run
python stsync.py sync jobs  --since 2025-08-01 --limit 3 --dry-run
```

## Run for Real (Items → POs → Jobs)

**Important:** Ensure master data (vendors, customers, locations, job types, campaigns) already exists in Integration before syncing POs/Jobs.

```bash
# Sync items first (no dependencies)
python stsync.py sync items --since 2025-08-01 --limit 100

# Then POs (depends on items + vendors)
python stsync.py sync pos   --since 2025-08-01 --limit 50

# Finally jobs (depends on customers, locations, job types, campaigns)
python stsync.py sync jobs  --since 2025-08-01 --limit 50
```

## Command Options

### sync command
- `--since`: ISO date filter (e.g., `2025-08-01`)
- `--limit`: Maximum records to process (0 = unlimited)
- `--dry-run`: Print payloads without creating records
- `--verbose`: Enable debug logging

## Configuration

### Environment Variables (.env)
```bash
# Auth endpoints
ST_AUTH_URL_PROD=https://auth.servicetitan.io/connect/token
ST_AUTH_URL_INT=https://auth-integration.servicetitan.io/connect/token

# API bases
ST_API_BASE_PROD=https://api.servicetitan.io
ST_API_BASE_INT=https://api-integration.servicetitan.io

# OAuth clients
ST_CLIENT_ID_PROD=your_prod_client_id
ST_CLIENT_SECRET_PROD=your_prod_client_secret
ST_CLIENT_ID_INT=your_int_client_id
ST_CLIENT_SECRET_INT=your_int_client_secret

# Required App Keys (v2 APIs)
ST_APP_KEY_PROD=your_prod_app_key
ST_APP_KEY_INT=your_int_app_key

# Database
STSYNC_DB=stsync.sqlite3

# Tuning
ST_PAGE_SIZE=200
ST_HTTP_TIMEOUT=30
```

### API Configuration (stsync.config.json)
Adjust paths and pagination keys to match your ServiceTitan tenant:

```json
{
  "entities": {
    "items": {
      "prod_list_path": "/pricebook/v2/tenant/{tenant}/materials",
      "int_create_path": "/pricebook/v2/tenant/{tenant}/materials",
      "list_params": { "page": 1, "pageSize": 200 },
      "list_data_key": "data",
      "next_page_key": "hasMore",
      "since_param": "modifiedSince"
    }
  }
}
```

Notes:
- Endpoints use the v2 shape: `/<domain>/v2/tenant/{tenant}/...` and the `{tenant}` placeholder is replaced from `.env`.
- The client now sends the required `ST-App-Key` header. Be sure to set `ST_APP_KEY_*`.

## ID Mapping

The tool maintains an SQLite database (`stsync.sqlite3`) that maps production IDs to integration IDs:

```sql
CREATE TABLE id_map (
    kind TEXT NOT NULL,      -- 'items', 'pos', 'jobs', etc.
    prod_id TEXT NOT NULL,   -- Production system ID
    int_id TEXT NOT NULL,    -- Integration system ID
    created_at REAL NOT NULL,-- Timestamp
    PRIMARY KEY(kind, prod_id)
);
```

## Dependencies

The order of syncing is important due to foreign key relationships:

1. **Items** (no dependencies)
2. **Purchase Orders** → requires Items + Vendors
3. **Jobs** → requires Customers + Locations + Job Types + Campaigns

## Error Handling

- **Rate limiting**: Automatic retries with exponential backoff
- **Network errors**: Robust retry logic with Tenacity
- **Data validation**: Pydantic models catch invalid payloads before API calls
- **Auth failures**: Clear error messages for credential issues
- **Missing dependencies**: Logs warnings for missing foreign key references

## Troubleshooting

### Common Issues

1. **"Missing environment variables"**
   - Copy `env.example` to `.env`
   - Fill in all OAuth credentials

2. **"Auth failed"**
   - Verify client IDs and secrets are correct
   - Check if apps have proper permissions in ServiceTitan

3. **"No valid lines found for PO"**
   - Ensure referenced items exist in Integration
   - Check item ID mappings in SQLite

4. **"Invalid job data"**
   - Verify customers, locations, job types exist in Integration
   - May need to sync master data first

### Debug Mode

Use `--verbose` flag for detailed logging:
```bash
python stsync.py sync items --verbose --limit 1
```

## Safety Features

- **Dry-run mode**: Test without creating records
- **Limit parameter**: Process small batches first
- **Idempotent**: Re-running won't create duplicates
- **Validation**: Pydantic catches data issues early
- **Structured logging**: Full audit trail of operations

## Extending the Tool

### Adding New Entity Types

1. Add configuration to `stsync.config.json`
2. Create Pydantic model in `stsync.py`
3. Implement mapper function
4. Add to CLI argument validation

### Custom Field Mapping

Edit the mapper functions in `stsync.py` to handle tenant-specific field requirements.

---

## Acceptance Checklist

- [ ] `verify` passes for both environments
- [ ] `sync items --dry-run` shows valid payloads
- [ ] Real `sync items` creates records and populates ID map
- [ ] `sync pos` and `sync jobs` succeed with correct FK translations
- [ ] Re-running is idempotent (no duplicates)
- [ ] Error handling works for missing dependencies
- [ ] Rate limiting is handled gracefully
