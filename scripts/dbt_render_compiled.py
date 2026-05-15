#!/usr/bin/env python3
"""Patch dbt manifest.json with manually rendered ``compiled_code``.

``dbt compile`` insists on a live warehouse connection to render Jinja, but our
ClickHouse service lives inside the Aiven project VPC and isn't reachable from
the developer's machine. For the DataHub dbt source to derive column-level
lineage we need ``manifest.nodes[*].compiled_code`` populated — without it the
sql-parser walks `None` and ``nodes_with_graph_columns`` lands at 0.

This script does the minimum rendering required:
- ``{{ config(...) }}``                          → stripped (it's a directive)
- ``{{ source('schema', 'table') }}``            → ``<schema>.<table>``
- ``{{ ref('model') }}``                         → ``<target_schema>.<model>``

Anything else (macros, vars, complex jinja) would need the full dbt compiler;
we don't use any of that in this demo's models. The script asserts every
``{{ ... }}`` placeholder gets rendered — if anything slips through, it raises
so you can see exactly which model still has unresolved jinja before pushing
into DataHub.

Usage:
    python scripts/dbt_render_compiled.py [path/to/manifest.json]
        (defaults to ~/Documents/lightdash-dbt/target/manifest.json)
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

MANIFEST = Path(
    sys.argv[1]
    if len(sys.argv) > 1
    else Path.home() / "Documents/lightdash-dbt/target/manifest.json"
)

# Models in this project all materialise to the ClickHouse ``default`` database.
TARGET_SCHEMA = "default"

# {{ source('foo', 'bar') }}   →   default.bar     (source schema lives in
# sources.yml; we hardcode the mapping rather than parse sources.yml since
# every source in this project is in ``default``).
RE_SOURCE = re.compile(
    r"""\{\{\s*source\(\s*['"](?P<src_schema>[^'"]+)['"]\s*,
                          \s*['"](?P<src_table>[^'"]+)['"]\s*\)\s*\}\}""",
    re.X,
)
RE_REF = re.compile(
    r"""\{\{\s*ref\(\s*['"](?P<ref_name>[^'"]+)['"]\s*\)\s*\}\}""",
    re.X,
)
RE_CONFIG = re.compile(r"\{\{\s*config\([^)]*\)\s*\}\}", re.DOTALL)
RE_ANY_JINJA = re.compile(r"\{\{[^}]*\}\}|\{%[^%]*%\}")


def render(raw_code: str, node_db: str | None) -> str:
    # ``database`` in ClickHouse dbt is the schema; if a node specifies it, use
    # it as the target schema for ref() resolution. Otherwise fall back to the
    # demo's default.
    target_schema = node_db or TARGET_SCHEMA

    sql = RE_CONFIG.sub("", raw_code)
    sql = RE_SOURCE.sub(
        lambda m: f"{m.group('src_schema')}.{m.group('src_table')}", sql
    )
    sql = RE_REF.sub(lambda m: f"{target_schema}.{m.group('ref_name')}", sql)

    # Each source's logical name (e.g. "webstore") differs from the
    # ClickHouse database that physically holds the data. Rewrite the
    # rendered `<source>.<table>` references to point at the real CH
    # database name so downstream sql-parsing (in DataHub) lines up
    # exactly with what ClickHouse executes.

    # webstore source: CDC-fed gold tables in the ClickHouse `default` db.
    for tbl in ("customers", "products", "orders", "order_items"):
        sql = sql.replace(f"webstore.{tbl}", f"{TARGET_SCHEMA}.{tbl}")

    # marketing source: federated database created by the Aiven
    # PostgreSQL → ClickHouse service integration. The name follows
    # the documented Aiven convention `service_<pg>_<db>_<schema>`.
    MARKETING_FEDERATED_DB = "service_pg-37c7de3b_defaultdb_marketing"
    sql = sql.replace(
        "marketing.campaigns", f"`{MARKETING_FEDERATED_DB}`.campaigns"
    )

    leftover = RE_ANY_JINJA.search(sql)
    if leftover:
        raise RuntimeError(
            f"unresolved jinja remaining after render: {leftover.group(0)!r}"
        )
    return sql.strip()


def main() -> int:
    if not MANIFEST.exists():
        sys.exit(f"manifest not found: {MANIFEST}")

    m = json.loads(MANIFEST.read_text())
    patched = 0
    for k, node in m["nodes"].items():
        if node["resource_type"] != "model":
            continue
        raw = node.get("raw_code") or ""
        if not raw:
            continue

        # The dbt-clickhouse adapter stores the warehouse db in `schema`;
        # `database` is empty. Use whichever is populated.
        node_db = node.get("schema") or node.get("database") or None
        rendered = render(raw, node_db)
        node["compiled_code"] = rendered
        node["compiled"] = True
        print(f"  rendered {node['name']:25s} ({len(rendered)} chars)")
        patched += 1

    MANIFEST.write_text(json.dumps(m))
    print(f"\npatched {patched} models in {MANIFEST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
