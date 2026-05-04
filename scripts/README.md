# `scripts/`

Maintenance helpers for the DataHub side of the demo. They paper over two gaps in the recipes that the Aiven DataHub Connector Wizard runs by default.

## What the wizard ingestion already gives us

| Source | Datasets | Schemas | Lineage to next hop |
| --- | --- | --- | --- |
| PostgreSQL (`pg-37c7de3b`) | ✅ all 4 webstore tables | ✅ | ✅ to Kafka topics (Debezium-aware) |
| Kafka (`kafka-1b5cb1e7`)   | ✅ all 4 topics            | ❌ (no Schema Registry record because we use Debezium with the JSON converter) | n/a |
| ClickHouse (`clickhouse-2a6274d2`) | ✅ all 12 tables (storage + MVs + Kafka-engine tables) | ✅ (from `system.tables`) | ❌ no Kafka-engine awareness, no MV SQL parsing |

## What the scripts do

### `datahub_fix.py`

Idempotent. Pushes two things every run:

1. `upstreamLineage` aspects on the 12 ClickHouse datasets so DataHub renders the full chain:

   ```
   pg.public.X  ->  kafka.webshop.public.X  ->  ch.service_kafka-1b5cb1e7.X  ->  ch.default.X_mv  ->  ch.default.X
   ```

2. `schemaMetadata` aspects on the 4 Kafka topics with the actual Debezium-unwrapped JSON payload (source columns + the three `__op`/`__source_ts_ms`/`__deleted` Debezium metadata fields).

Re-run it after every Connector Wizard ingestion (or whenever the lineage / schemas drop again).

### `datahub_diag.py`

Read-only sanity check. Prints one row per dataset with `UP / DOWN / SCHEMA` counts so you can verify the wiring at a glance.

### `datahub_glossary.py`

Idempotent. Pushes three things every run:

1. **Glossary** — 1 root node (`Webstore`) with 4 sub-nodes (Business Entities, Attributes, Privacy, CDC Metadata) and 15 terms (Customer, Product, Order, OrderItem, Email, PersonalName, Region, Revenue, OrderStatus, SKU, Quantity, PII, DebeziumOp, SourceTimestamp, DeletionFlag).
2. **Dataset descriptions + dataset-level term links** — for all 20 PG/Kafka/CH datasets, e.g. all 5 customer datasets carry the `Customer` term plus a friendly description.
3. **Column descriptions + column-level term links** — every column on every dataset gets a description and, where applicable, term tags (e.g. `customers.email -> Email + PII`, `orders.total -> Revenue`, `__op -> DebeziumOp`).

## Usage

```bash
export DH_GMS='https://<your-gms-host>:8080'   # the *-gms Aiven app
export DH_TOKEN='<datahub PAT>'                # DataHub UI -> Settings -> Access Tokens

python3 scripts/datahub_fix.py        # lineage + Kafka topic schemas
python3 scripts/datahub_glossary.py   # descriptions + glossary
python3 scripts/datahub_diag.py       # verify
```

All three scripts use only the Python stdlib, so no `pip install` needed.
