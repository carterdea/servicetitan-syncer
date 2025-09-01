from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class EntityConfig(BaseModel):
    prod_list_path: str
    int_create_path: str | None = None
    list_params: dict[str, Any] = Field(default_factory=dict)
    list_data_key: str = "items"
    next_page_key: str | None = "hasMore"
    since_param: str | None = None


class ConfigModel(BaseModel):
    entities: dict[str, EntityConfig]


def load_config(path: str | Path = "stsync.config.json") -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    model = ConfigModel.model_validate(data)
    # Return a plain dict for compatibility with existing code
    return model.model_dump()
