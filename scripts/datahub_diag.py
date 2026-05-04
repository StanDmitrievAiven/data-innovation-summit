#!/usr/bin/env python3
"""Diagnose lineage + schema state for the demo entities in DataHub."""
import json
import os
import sys
import urllib.request

GMS = os.environ["DH_GMS"].rstrip("/")
TOKEN = os.environ["DH_TOKEN"]


def gql(query: str) -> dict:
    req = urllib.request.Request(
        f"{GMS}/api/graphql",
        method="POST",
        data=json.dumps({"query": query}).encode(),
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


def list_datasets() -> list[str]:
    data = gql(
        'query { search(input: {type: DATASET, query: "*", count: 100}) '
        "{ searchResults { entity { urn } } } }"
    )
    return [r["entity"]["urn"] for r in data["data"]["search"]["searchResults"]]


def lineage(urn: str, direction: str) -> list[str]:
    q = (
        'query { dataset(urn: "%s") { lineage(input: {direction: %s, start: 0, count: 50}) '
        "{ total relationships { entity { urn } } } } }"
    ) % (urn, direction)
    data = gql(q)
    ds = data.get("data", {}).get("dataset")
    if not ds or not ds.get("lineage"):
        return []
    return [r["entity"]["urn"] for r in ds["lineage"].get("relationships", [])]


def schema_field_count(urn: str) -> int:
    q = (
        'query { dataset(urn: "%s") { schemaMetadata { fields { fieldPath } } } }'
    ) % urn
    data = gql(q)
    ds = data.get("data", {}).get("dataset")
    if not ds or not ds.get("schemaMetadata"):
        return 0
    return len(ds["schemaMetadata"].get("fields") or [])


urns = list_datasets()
print(f"=== {len(urns)} datasets ===")

# Group by platform
groups: dict[str, list[str]] = {}
for u in urns:
    plat = u.split("dataPlatform:")[1].split(",")[0]
    groups.setdefault(plat, []).append(u)
for plat, items in sorted(groups.items()):
    print(f"  {plat}: {len(items)}")

print()
print("=== Lineage + schema status (UP = upstream count, DOWN = downstream, SCHEMA = field count) ===")
print(f"  {'PLATFORM':<11} {'NAME':<55} {'UP':>4} {'DOWN':>4} {'SCHEMA':>7}")
for urn in urns:
    plat = urn.split("dataPlatform:")[1].split(",")[0]
    name = urn.split(",")[1]
    up = len(lineage(urn, "UPSTREAM"))
    dn = len(lineage(urn, "DOWNSTREAM"))
    sc = schema_field_count(urn)
    print(f"  {plat:<11} {name:<55} {up:>4} {dn:>4} {sc:>7}")
