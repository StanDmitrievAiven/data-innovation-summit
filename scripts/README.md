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

### `dbt_render_compiled.py`

Patches `target/manifest.json` (in `~/Documents/lightdash-dbt`) with manually-rendered `compiled_code` for each model. Needed because `dbt compile` requires a live ClickHouse connection (our service is VPC-bound), and the DataHub dbt source needs compiled SQL to derive column-level lineage from the model bodies.

Pipeline:

```bash
cd ~/Documents/lightdash-dbt
. .venv-dbt/bin/activate   # dbt-core + dbt-clickhouse
DBT_PROFILES_DIR=$PWD CLICKHOUSE_HOST=dummy CLICKHOUSE_PASSWORD=dummy dbt parse
python3 /Users/stan.dmitriev/Documents/DataInnovationSummitDemo/scripts/dbt_render_compiled.py
# Then ingest into DataHub:
cd /Users/stan.dmitriev/Documents/DataInnovationSummitDemo/datahub-lightdash-source
. .venv/bin/activate
datahub ingest -c recipes/dbt_aiven_demo.yml
```

After this lands you'll have:
- 4 dbt source entities (sibling-linked to the CH base tables) with descriptions from `sources.yml`.
- 3 dbt model entities (sibling-linked to the CH views) with column-level lineage to each source.
- Roughly 20 `fineGrainedLineages` entries per non-trivial model — visible in the lineage explorer's column view.

### `datahub_emit_view_lineage.py`

Superseded by the dbt ingestion above (which gives richer, more accurate lineage). Kept as a fallback for environments where dbt isn't available — it pulls each view's DDL from ClickHouse via the Lightdash SQL Runner tunnel, parses with sqlglot, and emits `upstreamLineage` directly on the ClickHouse-platform URNs.

## Usage

```bash
export DH_GMS='https://<your-gms-host>:8080'   # the *-gms Aiven app
export DH_TOKEN='<datahub PAT>'                # DataHub UI -> Settings -> Access Tokens

python3 scripts/datahub_fix.py        # lineage + Kafka topic schemas
python3 scripts/datahub_glossary.py   # descriptions + glossary
python3 scripts/datahub_diag.py       # verify
```

All three scripts use only the Python stdlib, so no `pip install` needed.
