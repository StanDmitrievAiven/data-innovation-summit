#!/usr/bin/env python3
"""
Patch DataHub with the two things the Aiven Connector Wizard ingestion
recipes don't emit on their own for this demo:

  1. Lineage edges:
        kafka.webshop.public.X
          -> clickhouse.service_kafka-1b5cb1e7.X   (Kafka Engine table consumes the topic)
          -> clickhouse.default.X_mv               (Materialized View reads the engine table)
          -> clickhouse.default.X                  (MV writes into ReplacingMergeTree)

  2. schemaMetadata for the four Kafka topics — derived from the known Debezium
     payload (we use the JSON converter + ExtractNewRecordState SMT, so there's
     no Schema Registry entry the Kafka source could pick up automatically).

Idempotent: every aspect is UPSERTed with the same key, so re-runs just
overwrite. Intended to be re-run after every Connector Wizard ingestion if
that wipes our edges.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

GMS = os.environ["DH_GMS"].rstrip("/")
TOKEN = os.environ["DH_TOKEN"]

PG_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,defaultdb.public.{name},PROD)"
KAFKA_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,webshop.public.{name},PROD)"
CH_KE_URN = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,service_kafka-1b5cb1e7.{name},PROD)"
CH_MV_URN = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.{name}_mv,PROD)"
CH_TBL_URN = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.{name},PROD)"

TABLES = ("customers", "products", "orders", "order_items")

# Source-of-truth column names per table for column-level lineage.
# Kafka / CH copies of each table carry these columns plus the three
# Debezium metadata fields (__op, __source_ts_ms, __deleted).
PG_COLUMNS: dict[str, list[str]] = {
    "customers":    ["id", "email", "name", "region", "country", "created_at", "updated_at"],
    "products":     ["id", "sku", "name", "category", "price", "inventory", "created_at", "updated_at"],
    "orders":       ["id", "customer_id", "status", "total", "region", "created_at", "updated_at"],
    "order_items":  ["id", "order_id", "product_id", "quantity", "unit_price"],
}
DEBEZIUM_META_COLUMNS = ["__op", "__source_ts_ms", "__deleted"]


def schema_field_urn(dataset_urn: str, field: str) -> str:
    return f"urn:li:schemaField:({dataset_urn},{field})"


def field_set_edges(upstream_urn: str, downstream_urn: str, columns: list[str]) -> list[dict]:
    """Identity column-level edges for a 1:1 column-name-preserving hop."""
    return [
        {
            "upstreamType": "FIELD_SET",
            "upstreams": [schema_field_urn(upstream_urn, c)],
            "downstreamType": "FIELD",
            "downstreams": [schema_field_urn(downstream_urn, c)],
            "transformOperation": "IDENTITY",
            "confidenceScore": 1.0,
        }
        for c in columns
    ]


def http(method: str, path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{GMS}{path}",
        method=method,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "X-RestLi-Protocol-Version": "2.0.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        print(f"  ! {method} {path} -> {e.code} {e.reason}: {e.read().decode()[:300]}", file=sys.stderr)
        raise


def post(path: str, body: dict) -> dict:
    return http("POST", path, body)


def ingest_aspect(entity_urn: str, aspect_name: str, aspect_value: dict) -> None:
    """Upsert a single aspect via the MetadataChangeProposal endpoint."""
    body = {
        "proposal": {
            "entityType": "dataset",
            "entityUrn": entity_urn,
            "aspectName": aspect_name,
            "changeType": "UPSERT",
            "aspect": {
                "value": json.dumps(aspect_value),
                "contentType": "application/json",
            },
        }
    }
    post("/aspects?action=ingestProposal", body)


# --- 1. Dataset + column lineage on each downstream dataset ----------------

now_ms = int(time.time() * 1000)
actor = "urn:li:corpuser:datahub"

# Per-downstream description: (downstream URN, [(upstream URN, list of columns to map)])
# Listing column maps per hop lets us emit fineGrainedLineages so DataHub renders
# field-to-field arrows when the user clicks a column.
lineage_plan: list[tuple[str, list[tuple[str, list[str]]]]] = []
for name in TABLES:
    pg = PG_URN.format(name=name)
    kf = KAFKA_URN.format(name=name)
    ke = CH_KE_URN.format(name=name)
    mv = CH_MV_URN.format(name=name)
    tb = CH_TBL_URN.format(name=name)
    pg_cols = PG_COLUMNS[name]
    full_cols = pg_cols + DEBEZIUM_META_COLUMNS  # Kafka / CH datasets carry both

    # Hop 1: PG -> Kafka topic (Debezium adds the 3 metadata fields with no PG ancestor)
    lineage_plan.append((kf, [(pg, pg_cols)]))
    # Hop 2: Kafka topic -> CH Kafka engine table
    lineage_plan.append((ke, [(kf, full_cols)]))
    # Hop 3: CH Kafka engine table -> CH materialized view (identity SELECT)
    lineage_plan.append((mv, [(ke, full_cols)]))
    # Hop 4: CH materialized view -> CH ReplacingMergeTree storage table
    lineage_plan.append((tb, [(mv, full_cols)]))

print(f"=== Lineage: writing upstreamLineage (with fineGrainedLineages) for {len(lineage_plan)} datasets ===")
for downstream, upstream_specs in lineage_plan:
    fine_grained: list[dict] = []
    upstream_records: list[dict] = []
    for upstream_urn, cols in upstream_specs:
        upstream_records.append(
            {
                "auditStamp": {"time": now_ms, "actor": actor},
                "dataset": upstream_urn,
                "type": "TRANSFORMED",
            }
        )
        fine_grained.extend(field_set_edges(upstream_urn, downstream, cols))

    aspect = {
        "upstreams": upstream_records,
        "fineGrainedLineages": fine_grained,
    }
    print(f"  {downstream}")
    for upstream_urn, cols in upstream_specs:
        print(f"    <- {upstream_urn}  ({len(cols)} column edges)")
    ingest_aspect(downstream, "upstreamLineage", aspect)


# --- 2. schemaMetadata aspect for the four Kafka topics --------------------
#
# Schema reflects the actual Debezium-unwrapped JSON payload landing in each
# topic. For "Decimal(10,2)" columns we declare the native type but mark them
# as STRING because Debezium emits them stringified by default with the JSON
# converter we configured.

DEBEZIUM_FIELDS = [
    ("__op", "String (Debezium op: c/u/d/r)", "String", "Debezium metadata field"),
    ("__source_ts_ms", "Int64 (Debezium source timestamp, ms)", "Number", "Debezium metadata field"),
    ("__deleted", "String (Debezium tombstone flag, 'true'/'false')", "String", "Debezium metadata field"),
]

# (fieldPath, nativeDataType, schemaType, description)
KAFKA_FIELDS: dict[str, list[tuple[str, str, str, str]]] = {
    "customers": [
        ("id", "Int64 (PK)", "Number", "Source column"),
        ("email", "String (UNIQUE)", "String", "Source column"),
        ("name", "String", "String", "Source column"),
        ("region", "String", "String", "Source column — used for EMEA filter"),
        ("country", "String", "String", "Source column"),
        ("created_at", "Timestamp (ISO-8601 string)", "String", "Source column"),
        ("updated_at", "Timestamp (ISO-8601 string)", "String", "Source column"),
    ] + DEBEZIUM_FIELDS,
    "products": [
        ("id", "Int64 (PK)", "Number", "Source column"),
        ("sku", "String (UNIQUE)", "String", "Source column"),
        ("name", "String", "String", "Source column"),
        ("category", "String", "String", "Source column"),
        ("price", "Decimal(10,2) (stringified by JSON converter)", "String", "Source column"),
        ("inventory", "Int32", "Number", "Source column"),
        ("created_at", "Timestamp (ISO-8601 string)", "String", "Source column"),
        ("updated_at", "Timestamp (ISO-8601 string)", "String", "Source column"),
    ] + DEBEZIUM_FIELDS,
    "orders": [
        ("id", "Int64 (PK)", "Number", "Source column"),
        ("customer_id", "Int64 (FK customers.id)", "Number", "Source column"),
        ("status", "String (pending/processing/shipped/delivered/cancelled)", "String", "Source column"),
        ("total", "Decimal(10,2) (stringified)", "String", "Source column"),
        ("region", "String", "String", "Source column — EMEA-weighted"),
        ("created_at", "Timestamp (ISO-8601 string)", "String", "Source column"),
        ("updated_at", "Timestamp (ISO-8601 string)", "String", "Source column"),
    ] + DEBEZIUM_FIELDS,
    "order_items": [
        ("id", "Int64 (PK)", "Number", "Source column"),
        ("order_id", "Int64 (FK orders.id)", "Number", "Source column"),
        ("product_id", "Int64 (FK products.id)", "Number", "Source column"),
        ("quantity", "Int32", "Number", "Source column"),
        ("unit_price", "Decimal(10,2) (stringified)", "String", "Source column"),
    ] + DEBEZIUM_FIELDS,
}

print(f"\n=== Schemas: writing schemaMetadata aspects for {len(KAFKA_FIELDS)} Kafka topics ===")
for table, fields in KAFKA_FIELDS.items():
    urn = KAFKA_URN.format(name=table)
    schema = {
        "schemaName": f"webshop.public.{table}",
        "platform": "urn:li:dataPlatform:kafka",
        "version": 0,
        "created": {"time": now_ms, "actor": actor},
        "lastModified": {"time": now_ms, "actor": actor},
        "hash": "",
        "platformSchema": {
            "com.linkedin.schema.OtherSchema": {
                "rawSchema": "Debezium PostgreSQL connector, JSON converter, "
                "ExtractNewRecordState SMT (no Schema Registry registration)."
            }
        },
        "fields": [
            {
                "fieldPath": fp,
                "nativeDataType": ndt,
                "type": {"type": {f"com.linkedin.schema.{tt}Type": {}}},
                "description": desc,
                "nullable": fp.startswith("__"),
            }
            for (fp, ndt, tt, desc) in fields
        ],
    }
    print(f"  -> {urn}  ({len(fields)} fields)")
    ingest_aspect(urn, "schemaMetadata", schema)

print("\nAll aspects written. Re-run scripts/datahub_diag.py to verify.")
