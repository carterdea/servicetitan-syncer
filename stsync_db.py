from __future__ import annotations

import sqlite3
import time

from stsync_settings import get_settings


class IDMapper:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or get_settings().DB_PATH
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                """CREATE TABLE IF NOT EXISTS id_map(
                kind TEXT NOT NULL,
                prod_id TEXT NOT NULL,
                int_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY(kind, prod_id)
            )"""
            )

    def get(self, kind: str, prod_id: str) -> str | None:
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

