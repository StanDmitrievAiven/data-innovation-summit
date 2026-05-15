#!/usr/bin/env python3
"""
Render the dbt models from lightdash-dbt into raw CREATE OR REPLACE VIEW
statements that ClickHouse can execute directly.

Use case: the views need to be materialised in Aiven for ClickHouse, but
Lightdash only compiles the dbt manifest — it doesn't `dbt run`. ClickHouse
itself is VPC-only, so paste the output of this script into the Aiven
Console's Query Editor (clickhouse-2a6274d2 -> Query editor) or pipe it
through any in-VPC client.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DBT_REPO = Path.home() / "Documents" / "lightdash-dbt"
TARGET_DB = "default"
MODELS = ["orders_enriched", "customers_clean", "products_catalog"]


def strip_jinja_call(sql: str, call_name: str) -> str:
    needle = "{{"
    i = 0
    while True:
        start = sql.find(needle, i)
        if start == -1:
            return sql
        j = start + 2
        while j < len(sql) and sql[j].isspace():
            j += 1
        if not sql[j:].startswith(call_name):
            i = start + 2
            continue
        depth = 0
        k = j
        while k < len(sql) - 1:
            if sql[k] == "(":
                depth += 1
            elif sql[k] == ")":
                depth -= 1
            elif sql[k : k + 2] == "}}" and depth == 0:
                k += 2
                break
            k += 1
        return sql[:start] + sql[k:]


def render(name: str) -> str:
    path = DBT_REPO / "models" / f"{name}.sql"
    sql = path.read_text()
    sql = strip_jinja_call(sql, "config")
    sql = re.sub(
        r"\{\{\s*source\(\s*'webstore'\s*,\s*'(\w+)'\s*\)\s*\}\}",
        rf"{TARGET_DB}.\1",
        sql,
    ).strip()
    return f"-- ===== {TARGET_DB}.{name} =====\nCREATE OR REPLACE VIEW {TARGET_DB}.{name} AS (\n{sql}\n);\n"


def main() -> None:
    if not DBT_REPO.exists():
        sys.exit(f"missing {DBT_REPO}")
    out = "\n".join(render(name) for name in MODELS)
    out_path = Path(__file__).with_name("lightdash_views.sql")
    out_path.write_text(out)
    print(f"wrote {out_path} ({len(out)} bytes, {len(MODELS)} statements)")
    print("\nfirst 30 lines:\n")
    print("\n".join(out.splitlines()[:30]))


if __name__ == "__main__":
    main()
