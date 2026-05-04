# dashboard-api

Tiny Express + TypeScript service that exposes the Data Innovation Summit demo's analytical questions as JSON REST endpoints, backed by **Aiven for ClickHouse** (`clickhouse-2a6274d2`).

It reads from the materialized `default.orders / order_items / products / customers` tables (which are continuously fed by the PG → Kafka (Debezium) → ClickHouse pipeline).

## Endpoints

| Method | Path                       | Notes                                                                                   |
| ------ | -------------------------- | --------------------------------------------------------------------------------------- |
| GET    | `/health`                  | Liveness for Aiven Apps.                                                                |
| GET    | `/`                        | Service info + total row counts (sanity).                                               |
| GET    | `/top-products`            | Top N products by units sold. Params: `region` (default `EMEA`), `days` (30), `limit` (10). |
| GET    | `/revenue-leaders`         | Same shape, ordered by revenue.                                                         |
| GET    | `/timeseries/orders`       | Orders per time bucket. Params: `hours` (24), `bucket_seconds` (60), `region` (""), `by_region` (`1` to split by region). |
| GET    | `/regions/summary`         | Per-region orders / revenue / unique customers / avg order value. Param: `hours` (24).  |
| GET    | `/categories/summary`      | Per-category orders / units / revenue. Params: `region`, `days`.                        |
| GET    | `/orders/status`           | Status breakdown (pending/processing/shipped/delivered/cancelled). Param: `hours`.      |
| GET    | `/live`                    | Most recent N orders with customer + country. Param: `limit` (25).                      |

### Region filtering convention

`region` is a "starts-with" filter:

- `region=EMEA` → matches every `EMEA-*` region.
- `region=EMEA-DACH` → exact regional bucket.
- `region=` (empty) → no region filter.

## Environment variables

| Name                    | Default            | Notes                                                                |
| ----------------------- | ------------------ | -------------------------------------------------------------------- |
| `PORT`                  | `3000`             | HTTP port (matches Dockerfile `EXPOSE`).                             |
| `CLICKHOUSE_HOST`       | —                  | E.g. `clickhouse-2a6274d2-data-innovation-summit.c.aivencloud.com`.  |
| `CLICKHOUSE_PORT`       | `14209`            | HTTPS port.                                                          |
| `CLICKHOUSE_USER`       | —                  | Typically `avnadmin`.                                                |
| `CLICKHOUSE_PASSWORD`   | —                  | Set via Aiven Console (Variables) — **never commit**.                |
| `CLICKHOUSE_DATABASE`   | `default`          |                                                                      |

## Why parameterised queries

All queries use ClickHouse's `{name:Type}` parameter binding (no string concatenation), so request inputs cannot inject SQL.

## Why `FINAL`

`default.*` tables are `ReplacingMergeTree(__source_ts_ms)`. `FINAL` ensures we read the latest row per primary key at query time. Cheap at demo scale; for production analytics you'd typically do this in a scheduled `OPTIMIZE TABLE FINAL` or use `argMax()` aggregations.
