#!/usr/bin/env python3
"""Emit upstream lineage for ClickHouse views into Aiven DataHub.

The ClickHouse views ``default.orders_enriched``, ``default.customers_clean``,
and ``default.products_catalog`` are dbt models built on top of the CDC-fed
base tables (``orders``, ``order_items``, ``products``, ``customers``).
DataHub's stock ClickHouse source emits the views as plain datasets but does
not parse their ``CREATE VIEW ... AS SELECT ...`` bodies, so the warehouse
lineage graph is missing the most important hop — from raw CDC tables to the
analytical views the BI layer reads from.

This script closes the gap:

1. Pulls every view DDL out of ``system.tables.create_table_query``. We tunnel
   the query through Lightdash's ``/sqlQuery`` endpoint because the ClickHouse
   service lives inside the project VPC and Lightdash is the only thing in
   that VPC we can reach from outside.
2. Parses each DDL with sqlglot (ClickHouse dialect) and walks the AST to:
   - Collect every base ``schema.table`` referenced.
   - For every projected column in the outermost SELECT, trace back to which
     base column(s) feed it. Aliases through CTEs, ``LEFT JOIN``, and column
     renames are followed; anything we can't trace is reported and skipped at
     the column level (the table-level edge still lands).
3. Emits one ``UpstreamLineage`` aspect per view via DataHub's REST API, with
   both:
   - ``upstreams[]``: one ``Upstream`` per base table (TRANSFORMED type).
   - ``fineGrainedLineages[]``: one entry per ``(view column, base columns)``
     pair, in CL_FINE_GRAINED form so the column-impact view picks it up.

The script is idempotent: re-running it overwrites the lineage aspect.

Env vars
--------
LIGHTDASH_PAT        Lightdash API key for the SQL Runner tunnel
DATAHUB_GMS_URL      e.g. https://...8080.eur-1.aiven.app
DATAHUB_TOKEN        DataHub personal access token (needs write scope)

Project-specific defaults are baked in below; override via env if needed.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

import sqlglot
from sqlglot import exp, lineage

# ---------------------------------------------------------------------------
# Configuration — defaults match the demo environment.

LIGHTDASH_URL = os.environ.get(
    "LIGHTDASH_URL",
    "https://019e0171-79d5-775d-b84a-9b12dcd1ed50-8080.eur-1.aiven.app",
)
LIGHTDASH_PROJECT = os.environ.get(
    "LIGHTDASH_PROJECT_UUID",
    "ce1188f7-2543-4176-a05e-489b0eb84713",
)
LIGHTDASH_PAT = os.environ.get("LIGHTDASH_PAT", "")
DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "").rstrip("/")
DATAHUB_TOKEN = os.environ.get("DATAHUB_TOKEN", "")

CH_PLATFORM = "clickhouse"
CH_DATABASE = "default"
ENV = "PROD"

VIEWS_TO_PROCESS = ["orders_enriched", "customers_clean", "products_catalog"]


def fail(msg: str) -> None:
    sys.stderr.write(f"error: {msg}\n")
    sys.exit(1)


# ---------------------------------------------------------------------------
# 1. Pull view DDLs from ClickHouse via the Lightdash SQL Runner tunnel.


def _lightdash_sql(sql: str) -> list[dict]:
    if not LIGHTDASH_PAT:
        fail("LIGHTDASH_PAT not set")
    url = f"{LIGHTDASH_URL}/api/v1/projects/{LIGHTDASH_PROJECT}/sqlQuery"
    body = json.dumps({"sql": sql}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"ApiKey {LIGHTDASH_PAT}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        fail(f"Lightdash SQL Runner {e.code}: {e.read().decode()[:300]}")
    if payload.get("status") != "ok":
        fail(f"Lightdash SQL Runner returned: {payload}")
    return payload["results"]["rows"]


def fetch_view_ddls() -> dict[str, str]:
    rows = _lightdash_sql(
        "SELECT name, create_table_query FROM system.tables "
        f"WHERE database='{CH_DATABASE}' AND engine='View'"
    )
    return {row["name"]: row["create_table_query"] for row in rows}


def fetch_warehouse_schema() -> dict[str, dict[str, dict[str, str]]]:
    """Return a nested ``{schema: {table: {column: type}}}`` from ``system.columns``.

    sqlglot.lineage needs real column lists to resolve references through CTEs.
    Without it, the walk bottoms out at ``Placeholder`` leaves and we get no
    column-level lineage. We only need *base tables* (engine=View entries are
    excluded so we don't try to walk through them).
    """
    rows = _lightdash_sql(
        "SELECT database, table, name, type FROM system.columns "
        f"WHERE database='{CH_DATABASE}' "
        "AND table NOT IN (SELECT name FROM system.tables "
        f"WHERE database='{CH_DATABASE}' AND engine='View')"
    )
    schema: dict[str, dict[str, dict[str, str]]] = {}
    for row in rows:
        schema.setdefault(row["database"], {}).setdefault(row["table"], {})[
            row["name"]
        ] = row["type"]
    return schema


# ---------------------------------------------------------------------------
# 2. Parse the DDL and build per-column lineage with sqlglot.


@dataclass
class ColumnLineage:
    """The set of ``(schema, table, column)`` triples that feed one view column."""

    downstream_column: str
    upstreams: list[tuple[str, str, str]]


@dataclass
class ViewLineage:
    view_schema: str
    view_name: str
    upstream_tables: set[tuple[str, str]]  # set of (schema, table)
    column_lineage: list[ColumnLineage]


def _view_name_and_body(parsed: exp.Expression) -> tuple[str, str, exp.Expression]:
    """Return (view_schema, view_name, SELECT body) from a CREATE VIEW AST."""
    if not isinstance(parsed, exp.Create) or parsed.args.get("kind") != "VIEW":
        raise ValueError("not a CREATE VIEW statement")

    this = parsed.this  # Schema/Table
    if isinstance(this, exp.Schema):  # CREATE VIEW name (col1, col2, ...)
        target = this.this
    else:
        target = this
    if not isinstance(target, exp.Table):
        raise ValueError("unexpected CREATE VIEW target shape")

    view_schema = target.db or "default"
    view_name = target.name

    body = parsed.expression  # the SELECT
    if body is None:
        raise ValueError("CREATE VIEW has no body")
    return view_schema, view_name, body


def _projection_alias(proj: exp.Expression) -> str:
    """Best-effort column name for a SELECT projection."""
    if isinstance(proj, exp.Alias):
        return proj.alias_or_name
    # Bare column reference — preserve its name.
    if isinstance(proj, exp.Column):
        return proj.name
    return proj.alias_or_name


def analyze_view(
    view_name: str,
    ddl: str,
    warehouse_schema: dict[str, dict[str, dict[str, str]]],
) -> ViewLineage:
    parsed = sqlglot.parse_one(ddl, dialect="clickhouse")
    view_schema, view_name_parsed, body = _view_name_and_body(parsed)
    assert view_name_parsed == view_name, (view_name_parsed, view_name)

    # --- Upstream tables: walk every table reference in the body. CTE aliases
    # are NOT base tables; sqlglot tracks those via ``CTE`` expressions and we
    # exclude them by name.
    cte_aliases: set[str] = set()
    for cte in body.find_all(exp.CTE):
        cte_aliases.add(cte.alias_or_name)

    upstream_tables: set[tuple[str, str]] = set()
    for tbl in body.find_all(exp.Table):
        if tbl.name in cte_aliases and not tbl.db:
            continue  # reference to a CTE, not a base table
        schema = tbl.db or "default"
        upstream_tables.add((schema, tbl.name))

    column_results: list[ColumnLineage] = []
    select_outer = body if isinstance(body, exp.Select) else body.find(exp.Select)
    if select_outer is None:
        return ViewLineage(view_schema, view_name, upstream_tables, column_results)

    # Construct a SELECT we can feed to sqlglot.lineage. Wrap the original CREATE
    # VIEW body's SELECT in a SQL string — sqlglot.lineage takes a SQL string +
    # column name and walks back.
    select_sql = body.sql(dialect="clickhouse")

    for proj in select_outer.expressions:
        col_name = _projection_alias(proj)
        try:
            node = lineage.lineage(
                column=col_name,
                sql=select_sql,
                schema=warehouse_schema,
                dialect="clickhouse",
            )
        except Exception as e:
            print(
                f"  ! {view_name}.{col_name}: lineage walk failed ({e!s}); "
                "skipping column-level lineage",
                file=sys.stderr,
            )
            continue

        # ``node`` is a Node; its leaves are the ultimate sources. Each leaf
        # has ``downstream=[]``. For column lineage we only care about leaves
        # whose ``expression`` is a base ``Table`` — leaves backed by ``Alias``
        # around ``count()``/``coalesce(literal)`` etc. are aggregates or
        # constants with no column-level traceability and get skipped.
        # ``leaf.name`` is the fully-qualified column at the leaf, e.g.
        # ``customers.id``; we split that to recover the (table, column) pair.
        upstreams: list[tuple[str, str, str]] = []
        for leaf in node.walk():
            if leaf.downstream:
                continue
            if not isinstance(leaf.expression, exp.Table):
                continue
            # The leaf's Table expression carries the resolved base table name
            # (after alias resolution) — e.g. ``default.order_items AS oi``
            # gives us ``name='order_items'`` even though the leaf.name might
            # say ``oi.quantity``. The column is always the last segment of
            # ``leaf.name``.
            src_col = leaf.name.split(".")[-1]
            src_table = leaf.expression.name
            src_schema = leaf.expression.db or "default"
            upstreams.append((src_schema, src_table, src_col))

        if upstreams:
            column_results.append(
                ColumnLineage(downstream_column=col_name, upstreams=upstreams)
            )

    return ViewLineage(view_schema, view_name, upstream_tables, column_results)


# ---------------------------------------------------------------------------
# 3. Emit the UpstreamLineage aspect to DataHub.


def make_dataset_urn(schema: str, table: str) -> str:
    return (
        f"urn:li:dataset:(urn:li:dataPlatform:{CH_PLATFORM},"
        f"{schema}.{table},{ENV})"
    )


def make_schema_field_urn(schema: str, table: str, column: str) -> str:
    return f"urn:li:schemaField:({make_dataset_urn(schema, table)},{column})"


def emit_lineage(v: ViewLineage) -> None:
    if not DATAHUB_GMS_URL or not DATAHUB_TOKEN:
        fail("DATAHUB_GMS_URL and DATAHUB_TOKEN must be set")

    view_urn = make_dataset_urn(v.view_schema, v.view_name)
    upstreams = [
        {
            "auditStamp": {"time": 0, "actor": "urn:li:corpuser:datahub"},
            "dataset": make_dataset_urn(schema, table),
            "type": "TRANSFORMED",
        }
        for (schema, table) in sorted(v.upstream_tables)
    ]
    fine_grained = []
    for cl in v.column_lineage:
        # Deduplicate the upstream column list while preserving order.
        seen: set[tuple[str, str, str]] = set()
        unique_upstreams = []
        for u in cl.upstreams:
            if u in seen:
                continue
            seen.add(u)
            unique_upstreams.append(u)
        if not unique_upstreams:
            continue
        fine_grained.append(
            {
                "upstreamType": "FIELD_SET",
                "upstreams": [
                    make_schema_field_urn(s, t, c) for (s, t, c) in unique_upstreams
                ],
                "downstreamType": "FIELD",
                "downstreams": [
                    make_schema_field_urn(
                        v.view_schema, v.view_name, cl.downstream_column
                    )
                ],
                "confidenceScore": 1.0,
            }
        )

    payload = {
        "entityType": "dataset",
        "entityUrn": view_urn,
        "aspectName": "upstreamLineage",
        "changeType": "UPSERT",
        "aspect": {
            "contentType": "application/json",
            "value": json.dumps(
                {
                    "upstreams": upstreams,
                    "fineGrainedLineages": fine_grained,
                }
            ),
        },
    }

    body = json.dumps([payload]).encode()
    req = urllib.request.Request(
        f"{DATAHUB_GMS_URL}/aspects?action=ingestProposalBatch",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {DATAHUB_TOKEN}",
            "Content-Type": "application/json",
            "X-RestLi-Protocol-Version": "2.0.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        # /aspects?action=ingestProposalBatch may not be enabled on this GMS;
        # fall back to per-proposal /aspects?action=ingestProposal.
        if e.code in (404, 400):
            single = {**payload}
            single_req = urllib.request.Request(
                f"{DATAHUB_GMS_URL}/aspects?action=ingestProposal",
                data=json.dumps({"proposal": single}).encode(),
                method="POST",
                headers={
                    "Authorization": f"Bearer {DATAHUB_TOKEN}",
                    "Content-Type": "application/json",
                    "X-RestLi-Protocol-Version": "2.0.0",
                },
            )
            try:
                with urllib.request.urlopen(single_req, timeout=30) as resp:
                    resp.read()
            except urllib.error.HTTPError as e2:
                fail(
                    f"DataHub /aspects {e2.code}: "
                    f"{e2.read().decode(errors='replace')[:400]}"
                )
        else:
            fail(
                f"DataHub /aspects {e.code}: "
                f"{e.read().decode(errors='replace')[:400]}"
            )

    print(
        f"  -> emitted {v.view_name}: {len(upstreams)} upstream tables, "
        f"{len(fine_grained)} column-level edges"
    )


# ---------------------------------------------------------------------------


def main() -> int:
    print("Fetching view DDLs and base-table schema via Lightdash SQL Runner...")
    ddls = fetch_view_ddls()
    warehouse_schema = fetch_warehouse_schema()
    base_tables = sum(len(t) for t in warehouse_schema.values())
    print(f"  loaded {base_tables} base tables across {len(warehouse_schema)} schemas")

    for view in VIEWS_TO_PROCESS:
        if view not in ddls:
            print(f"  ! {view}: not present in ClickHouse, skipping")
            continue
        print(f"\nAnalyzing {view}...")
        try:
            v = analyze_view(view, ddls[view], warehouse_schema)
        except Exception as e:
            print(f"  ! {view}: parse failed ({e!s})", file=sys.stderr)
            continue

        print(f"  upstream tables: {sorted(v.upstream_tables)}")
        print(f"  columns with traced lineage: {len(v.column_lineage)}")
        for cl in v.column_lineage[:3]:
            upstream_strs = [f"{s}.{t}.{c}" for (s, t, c) in cl.upstreams]
            print(f"    {cl.downstream_column} <- {upstream_strs}")

        emit_lineage(v)

    return 0


if __name__ == "__main__":
    sys.exit(main())
