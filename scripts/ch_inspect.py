#!/usr/bin/env python3
"""Run ad-hoc SQL against the Aiven for ClickHouse service over HTTPS.

A small utility for verifying ClickHouse state from the laptop — listing
databases, peeking at the federated PostgreSQL integration, debugging the
output of newly-materialised dbt models. ClickHouse's HTTPS interface is
publicly reachable (unlike PG which is VPC-locked), so we can hit it
directly once the Aiven token resolves the password.

Usage:
    AIVEN_TOKEN=... python scripts/ch_inspect.py "SHOW DATABASES"
    AIVEN_TOKEN=... python scripts/ch_inspect.py "SELECT * FROM service_pg_37c7de3b.campaigns LIMIT 5"

Default query (when none passed) prints SHOW DATABASES + a peek at every
database whose name starts with ``service_`` (the convention Aiven uses
when exposing a PostgreSQL schema through the clickhouse_postgresql
integration).
"""

from __future__ import annotations

import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

AIVEN_TOKEN = os.environ.get("AIVEN_TOKEN", "").strip()
AIVEN_PROJECT = os.environ.get("AIVEN_PROJECT", "data-innovation-summit")
AIVEN_CH_SERVICE = os.environ.get("AIVEN_CH_SERVICE", "clickhouse-2a6274d2")


def fail(msg: str) -> None:
    sys.stderr.write(f"error: {msg}\n")
    sys.exit(1)


def resolve_ch_endpoint() -> tuple[str, int, str, str]:
    if not AIVEN_TOKEN:
        fail("AIVEN_TOKEN not set")
    req = urllib.request.Request(
        f"https://api.aiven.io/v1/project/{AIVEN_PROJECT}/service/{AIVEN_CH_SERVICE}",
        headers={"Authorization": f"aivenv1 {AIVEN_TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            svc = json.loads(resp.read().decode())["service"]
    except urllib.error.HTTPError as e:
        fail(f"aiven API {e.code}: {e.read().decode(errors='replace')[:200]}")
    p = svc["service_uri_params"]
    https_port = next(
        c["port"] for c in svc["components"] if c["component"] == "clickhouse_https"
    )
    return p["host"], https_port, p["user"], p["password"]


class CHClient:
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self.url = f"https://{host}:{port}/"
        self.user = user
        self.password = password
        self.ctx = ssl.create_default_context()

    def execute(self, sql: str) -> str:
        params = {"user": self.user, "password": self.password}
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


def main() -> None:
    host, port, user, password = resolve_ch_endpoint()
    ch = CHClient(host, port, user, password)
    if len(sys.argv) > 1:
        sql = " ".join(sys.argv[1:])
        print(ch.execute(sql))
        return

    print("=== SHOW DATABASES ===")
    out = ch.execute("SHOW DATABASES")
    print(out)
    for db in [d.strip() for d in out.splitlines() if d.startswith("service_")]:
        print(f"\n=== SHOW TABLES FROM {db} ===")
        print(ch.execute(f"SHOW TABLES FROM `{db}`"))


if __name__ == "__main__":
    main()
