#!/usr/bin/env python3
import os
import sys

# Ensure repo root is on sys.path for `import stsync`
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from stsync import http_get, int_token  # noqa: E402
from stsync_settings import require_settings  # noqa: E402


def main() -> None:
    t = int_token()
    paths = [
        "/crm/v2/tenant/{tenant}/business-units",
        "/settings/v2/tenant/{tenant}/business-units",
    ]

    s = require_settings()
    for path in paths:
        try:
            d = http_get(s.API_BASE_INT, path, t, {"page": 1, "pageSize": 200})
            print(f"OK: {path}")
            items = d.get("data") or d.get("items") or []
            for bu in items:
                print(f"{bu.get('id')} - {bu.get('name')}")
            break
        except Exception as e:
            print(f"fail: {path} -> {e}")


if __name__ == "__main__":
    main()
