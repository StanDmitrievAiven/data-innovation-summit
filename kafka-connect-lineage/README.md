# Column-Level Data Lineage for Aiven Kafka Connect

- [Summary](#summary)
- [Context](#context)
- [Approach](#approach)
- [Repository structure](#repository-structure)
- [POC Results](#poc-results)
- [Running the local demo](#running-the-local-demo)
- [Running against Aiven DataHub](#running-against-aiven-datahub)
- [Auth](#auth)
- [Alternatives](#alternatives)
- [Status](#status)
- [Limitations](#limitations)

## Summary

Aiven customers with both Kafka Connect and DataHub see no data flow information in DataHub. The lineage view is empty. This POC adds a Kafka Connect plugin (SMT) that reports data flows to DataHub automatically, including column-level detail. No per-connector configuration is needed.

## Context

### What lineage is

A company stores customer data in PostgreSQL. A Debezium connector streams changes from the `customers` table to a Kafka topic. A JDBC Sink connector writes from that topic to a second database.

Without lineage, DataHub knows these datasets exist individually but has no idea they are connected. They show up as separate entries.

With lineage, DataHub shows the full path from the source table through the Kafka topic to the target table.

### Dataset-level vs column-level

**Dataset-level** shows which datasets connect. "Table A feeds Topic B."

**Column-level** shows which fields connect. "The `email` column in the source maps to the `email` column in the target."

### Current gap

Aiven offers DataHub and Kafka Connect as managed services. They can exist in the same project but do not communicate. DataHub does not know Kafka Connect exists. The customer gets a working pipeline and an empty DataHub lineage view.

The options available to customers today:
- Draw lineage by hand in the DataHub UI
- Write DataHub ingestion recipes manually
- Skip lineage and use DataHub only as a schema catalog

## Approach

### What the SMT does

The SMT (Single Message Transform) is a Kafka Connect plugin that attaches to each connector. It reads the schema of every record flowing through (field names and types), determines the source and destination datasets, and sends a structured event to DataHub.

It produces:
- Datasets (source tables, Kafka topics, sink tables)
- Lineage relationships between them
- Column-level mappings (which input field maps to which output field)
- Schema metadata (field names and types)

It does not modify, delay, or drop records.

### Connector support

The SMT reads `ConnectRecord.valueSchema()`, which is present on any connector that uses Kafka Connect's schema system. This includes Debezium, JDBC Sink, S3, GCS, OpenSearch, ClickHouse, HTTP, and others.

## Repository structure

```
.
|-- smt-kotlin/          SMT source code (Kotlin)
|   |-- src/main/        Source files
|   |-- src/test/        Test files
|
|-- smt-java/            Same SMT in Java (reference implementation)
|
|-- poc/                  Docker Compose demos
    |-- docker-compose-datahub.yml         Local DataHub demo
    |-- docker-compose-aiven-datahub.yml   Local pipeline, remote Aiven DataHub
    |-- debezium/                          Source connector configs
    |-- jdbc-sink/                         Sink connector configs
    |-- postgres/                          Database schema and seed data
    |-- scripts/
        |-- demo.sh                        Local demo (start/before/after/cleanup)
        |-- demo-aiven-datahub.sh          Aiven DataHub demo
```

## POC Results

### Start: pipeline running, DataHub empty

The Debezium source and JDBC Sink connectors are running. Data flows from PostgreSQL through Kafka topics back into PostgreSQL. DataHub is up but has no awareness of any of this.

> <img width="1054" height="365" alt="Screenshot 2026-03-23 at 15 27 04" src="https://github.com/user-attachments/assets/1d6fc012-676d-49a3-a354-e166aadf242e" />
>
> *DataHub shows 0 datasets. The pipeline is running but DataHub has no awareness of it.*

### Before: datasets discovered, no lineage

Datasets are registered in DataHub (simulating what an ingestion recipe or the connector wizard discovery would produce). DataHub now knows the datasets exist, but has no idea they are connected to each other. Each one shows up as an isolated entry.

> <img width="1054" height="365" alt="Screenshot 2026-03-23 at 15 32 45" src="https://github.com/user-attachments/assets/d5ad7535-bdb4-4b89-804b-38efa7b0c13d" />
>
> *6 datasets visible in DataHub. The pipeline is running, but nothing connects them.*

> <img width="1054" height="365" alt="Screenshot 2026-03-23 at 15 33 29" src="https://github.com/user-attachments/assets/2e2dd2c6-cf63-48da-a916-3371e8545d54" />
>
> *The lineage graph for any dataset is empty. DataHub sees the tables and topics but not the data flow between them.*

### After: lineage connects the datasets

The connectors are re-registered with the OpenLineage SMT attached. Same pipeline, same data, same datasets. The SMT observes the record schemas and reports the relationships to DataHub.

> <img width="1054" height="365" alt="Screenshot 2026-03-23 at 15 42 32" src="https://github.com/user-attachments/assets/d5fbfc20-27f2-4bf8-b548-ad49ae0d97b8" />
>
> *The lineage graph now shows the path from the source table through the Kafka topic to the target table. Connector jobs are visible. The previously isolated datasets are connected.*

> <img width="1054" height="365" alt="Screenshot 2026-03-23 at 15 43 06" src="https://github.com/user-attachments/assets/16213106-bd46-4c05-9e58-66d289e6ad7a" />
>
> *Column mappings showing `id` to `id`, `name` to `name`, `email` to `email`, `age` to `age`.*

## Running the local demo

Prerequisites are Docker and about 4 GB RAM.

```bash
# Build the SMT (Kotlin version, or use smt-java/ instead)
docker run --rm -v "$(pwd)/smt-kotlin:/project" -w /project \
  gradle:8.5-jdk17 gradle clean build

cd poc

# DataHub UI: http://localhost:9002 (credentials: datahub / datahub)

# Bring up DataHub, Kafka, Kafka Connect, PostgreSQL.
# Registers Debezium source + JDBC Sink connectors (without the SMT).
# Inserts seed data so CDC is flowing. DataHub is empty at this point.
./scripts/demo.sh start

# Register 6 datasets in DataHub via the GMS API (simulates ingestion
# recipe discovery). Datasets appear as isolated entries with no lineage.
./scripts/demo.sh before

# Remove connectors, re-register with the OpenLineage SMT attached.
# Insert more data. The SMT sends lineage events to DataHub.
# The isolated datasets are now connected with lineage edges,
# data jobs, column mappings, and schema metadata.
./scripts/demo.sh after

./scripts/demo.sh cleanup
```

## Running against Aiven DataHub

You need an Aiven DataHub instance and a DataHub personal access token.

```bash
cd poc

# Set your DataHub GMS URL and token
vim openlineage-aiven-datahub.yml

# Same start/before/after flow
./scripts/demo-aiven-datahub.sh before
./scripts/demo-aiven-datahub.sh after
./scripts/demo-aiven-datahub.sh cleanup
```

## Auth

The SMT supports token-based auth for DataHub instances that require it:

```yaml
transport:
  type: http
  url: https://datahub-gms.example.com
  endpoint: openapi/openlineage/api/v1/lineage
  auth:
    type: api_key
    api_key: your-token
```

## Alternatives

| Approach | Dataset lineage | Column lineage | Real-time | All connectors | Setup |
|----------|:---:|:---:|:---:|:---:|---|
| This SMT | Yes | Yes | Yes | Yes | One service toggle |
| Debezium built-in OpenLineage | Yes | No | Yes | Debezium only | Per-connector config |
| DataHub ingestion recipe | Yes | No | No (scheduled) | Yes | Write YAML, run CLI |
| Manual UI editing | Yes | Partial | No | N/A | Draw each arrow |

### Debezium built-in OpenLineage

Debezium (since v2.3) can emit OpenLineage events natively. It produces dataset-level lineage only (no columns), covers only Debezium connectors (not sinks or other connectors), and requires per-connector configuration. It is not enabled in Aiven today.

The SMT covers everything Debezium's built-in support does, plus column-level lineage, plus all connector types. Running both produces duplicate dataset-level events with no added benefit.

### DataHub ingestion recipes

DataHub can query the Kafka Connect REST API on a schedule and infer lineage from connector configs. This produces dataset-level lineage only. It cannot see actual record schemas, so no column detail. Inference can be wrong for unusual configs.

Ingestion recipes and the SMT complement each other. Recipes provide baseline dataset discovery. The SMT adds column-level detail.

### Manual editing

Drawing lineage in the DataHub UI by hand is an option but requires manual maintenance.

## Status

### Complete

| Component | Status |
|-----------|--------|
| SMT plugin (Kotlin + Java reference) | Done, tests passing |
| Local DataHub tests | Done |
| Aiven DataHub tests | Pending |

### Out of scope

- Lineage for non-Kafka-Connect paths (e.g., ClickHouse native Kafka engine)
- Column lineage for connectors without structured schemas

## Limitations

### Manual edits and automated lineage

By default, DataHub uses "last write wins" per metadata aspect, so an SMT event could overwrite a manual edit. DataHub supports an `incremental_lineage` flag that emits lineage as patches instead of full replacements, preserving manual edits. The SMT could adopt PATCH operations to support this.

### Auth mechanism unverified

The system client secret is used as a Bearer token. This needs testing against a real Aiven DataHub instance. A personal access token may be needed instead.

### Schema-less connectors

Connectors producing records without structured schemas get dataset-level lineage but no column detail.

