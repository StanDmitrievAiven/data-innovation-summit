#!/usr/bin/env python3
"""
Materialise the lightdash-dbt models into Aiven for ClickHouse.

Lightdash's "Refresh" only re-parses the dbt manifest — it does not execute
`dbt run`. For our self-hosted Lightdash there is no scheduled dbt runner,
so the views referenced by the manifest are never physically created in the
warehouse, and every chart on the Webstore Live Insights dashboard fails
with `Unknown table expression identifier 'default.orders_enriched'`.

This script reads each model SQL file from the local clone of the
lightdash-dbt repo, expands the `{{ source('webstore', 'X') }}` macros into
real `default.X` references, wraps the body in `CREATE OR REPLACE VIEW
default.<model> AS (...)`, and POSTs each statement to the Aiven for
ClickHouse HTTPS interface directly. We go straight to ClickHouse (and not
through Lightdash's `sqlQuery` endpoint) because Lightdash's SQL Runner
auto-appends `LIMIT 5000 FORMAT JSONCompactEachRowWithNamesAndTypes` to
every query, which is a syntax error on a DDL statement.

Required env vars (or fall back to the demo defaults baked in below):
    AIVEN_TOKEN              Aiven API token, used to resolve the CH password
    AIVEN_PROJECT            Aiven project name (defaults to data-innovation-summit)
    AIVEN_CH_SERVICE         CH service name (defaults to clickhouse-2a6274d2)
    DBT_REPO_PATH            Local path to the cloned lightdash-dbt repo

Idempotent (CREATE OR REPLACE). Re-run after every push to lightdash-dbt
to keep the warehouse-side views in sync with the source SQL.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

AIVEN_TOKEN = os.environ.get("AIVEN_TOKEN", "").strip()
AIVEN_PROJECT = os.environ.get("AIVEN_PROJECT", "data-innovation-summit")
AIVEN_CH_SERVICE = os.environ.get("AIVEN_CH_SERVICE", "clickhouse-2a6274d2")
TARGET_DB = os.environ.get("CH_DATABASE", "default")

DBT_REPO = Path(
    os.environ.get("DBT_REPO_PATH", str(Path.home() / "Documents" / "lightdash-dbt"))
)

# Order matters topologically. None of our models depend on each other today
# (they all read from sources), but ordering is cheap insurance.
MODELS_IN_ORDER = [
    "orders_enriched",
    "customers_clean",
    "products_catalog",
]


# ---------------------------------------------------------------------------
# Aiven API: resolve the ClickHouse HTTPS endpoint + password


def aiven_get_service() -> dict:
    if not AIVEN_TOKEN:
        sys.exit(
            "AIVEN_TOKEN not set — pass an Aiven API token so we can fetch the "
            "ClickHouse password. Generate one at "
            "https://console.aiven.io/account/tokens."
        )
    req = urllib.request.Request(
        f"https://api.aiven.io/v1/project/{AIVEN_PROJECT}/service/{AIVEN_CH_SERVICE}",
        headers={"Authorization": f"aivenv1 {AIVEN_TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())["service"]
    except urllib.error.HTTPError as e:
        sys.exit(f"aiven API {e.code}: {e.read().decode(errors='replace')[:300]}")


def resolve_ch_endpoint() -> tuple[str, int, str, str]:
    svc = aiven_get_service()
    p = svc["service_uri_params"]
    user, password = p["user"], p["password"]
    host = p["host"]
    https_port = next(
        c["port"] for c in svc["components"] if c["component"] == "clickhouse_https"
    )
    return host, https_port, user, password


# ---------------------------------------------------------------------------
# ClickHouse over HTTPS — POST body is the raw SQL, response is plain text.


class CHClient:
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self.url = f"https://{host}:{port}/"
        self.user = user
        self.password = password
        self.ctx = ssl.create_default_context()  # Aiven uses a publicly-trusted CA

    def execute(self, sql: str, *, database: str | None = None) -> str:
        params = {"user": self.user, "password": self.password}
        if database:
            params["database"] = database
        url = self.url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            data=sql.encode(),
            method="POST",
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        try:
            with urllib.request.urlopen(req, timeout=120, context=self.ctx) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            raise RuntimeError(f"CH HTTP {e.code}: {body[:600]}") from None


# ---------------------------------------------------------------------------
# Rendering: strip jinja {{ config(...) }} block and resolve source() macros.


def render_model_sql(name: str) -> str:
    path = DBT_REPO / "models" / f"{name}.sql"
    if not path.exists():
        sys.exit(f"missing model file: {path}")
    sql = path.read_text()

    # Strip `{{ config(...) }}` (possibly multi-line). We use a balanced-paren
    # scan because regex alone is fragile for multi-line jinja args.
    sql = strip_jinja_call(sql, "config")

    # Resolve {{ source('webstore', 'X') }} to <db>.X.
    sql = re.sub(
        r"\{\{\s*source\(\s*'webstore'\s*,\s*'(\w+)'\s*\)\s*\}\}",
        rf"{TARGET_DB}.\1",
        sql,
    )
    return sql.strip()


def strip_jinja_call(sql: str, call_name: str) -> str:
    """Remove `{{ <call_name>(...) }}` even if it spans multiple lines."""
    needle = "{{"
    i = 0
    while True:
        start = sql.find(needle, i)
        if start == -1:
            return sql
        # Skip whitespace after {{
        j = start + 2
        while j < len(sql) and sql[j].isspace():
            j += 1
        if not sql[j:].startswith(call_name):
            i = start + 2
            continue
        # Find the matching `}}` that closes the jinja expression. Track paren
        # depth so we don't get confused by `}` inside the function args.
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


# ---------------------------------------------------------------------------
# Orchestration


def main() -> None:
    print(f"aiven project: {AIVEN_PROJECT}")
    print(f"ch service:    {AIVEN_CH_SERVICE}")
    print(f"target db:     {TARGET_DB}")
    print(f"dbt repo:      {DBT_REPO}")
    if not DBT_REPO.exists():
        sys.exit(f"missing dbt repo at {DBT_REPO} — clone lightdash-dbt first")

    print("\nresolving ClickHouse endpoint via Aiven API...")
    host, port, user, password = resolve_ch_endpoint()
    print(f"  endpoint: https://{user}@{host}:{port}")
    ch = CHClient(host, port, user, password)

    print("\n[1/4] sanity check — current databases:")
    out = ch.execute("SHOW DATABASES")
    for line in out.strip().splitlines():
        print(f"     - {line}")
    has_orphan = "default_default" in out

    print(f"\n[2/4] dropping orphan default_default database (has_orphan={has_orphan})...")
    if has_orphan:
        for name in MODELS_IN_ORDER:
            ch.execute(f"DROP VIEW IF EXISTS default_default.{name}")
            print(f"     - DROP VIEW IF EXISTS default_default.{name}")
        ch.execute("DROP DATABASE IF EXISTS default_default")
        print(f"     - DROP DATABASE IF EXISTS default_default")

    print(f"\n[3/4] creating {len(MODELS_IN_ORDER)} views in {TARGET_DB}.*...")
    for name in MODELS_IN_ORDER:
        body = render_model_sql(name)
        stmt = f"CREATE OR REPLACE VIEW {TARGET_DB}.{name} AS (\n{body}\n)"
        ch.execute(stmt)
        print(f"     + {TARGET_DB}.{name}")

    print("\n[4/4] smoke-testing views (count rows):")
    for name in MODELS_IN_ORDER:
        out = ch.execute(f"SELECT count() FROM {TARGET_DB}.{name}").strip()
        print(f"     {TARGET_DB}.{name}: {out} rows")

    print(
        "\n[done] views materialised. Hit the Lightdash dashboard now — every "
        "tile should load on the next render."
    )


if __name__ == "__main__":
    main()
