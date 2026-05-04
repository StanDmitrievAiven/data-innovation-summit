# Data Innovation Summit Demo

End-to-end Aiven Platform demo built on the [`data-innovation-summit`](https://console.aiven.io/) project.

Pitch: **OLTP and OLAP on one platform**, with **lineage and governance** built in, plus an easy workflow for **building applications on top of your data** — all in Aiven, on any cloud.

## Architecture

```
+---------------------+   writes    +-------------------------+
| webshop-simulator   |------------>| Aiven for PostgreSQL    |
| (Aiven Apps)        |             | pg-37c7de3b             |
+---------------------+             +-------------------------+
                                                |
                                          Debezium CDC
                                       (Aiven Kafka Connect:
                                        kafkaconnect-30e121dd)
                                                v
                                    +-------------------------+
                                    | Aiven for Apache Kafka  |
                                    | kafka-1b5cb1e7          |
                                    +-------------------------+
                                                |
                                    clickhouse_kafka integration
                                       (Kafka engine tables +
                                        Materialized Views into
                                        ReplacingMergeTree)
                                                v
                                    +-------------------------+
                                    | Aiven for ClickHouse    |
                                    | clickhouse-2a6274d2     |
                                    +-------------------------+
                                                ^
                                                | SQL over HTTPS
                                                |
+----------------------+        /api/*  +-------------------------+
| dashboard-web        |--------------> | dashboard-api           |
| React + Recharts     | (Express proxy)| Express + @clickhouse/  |
| (Aiven Apps)         |                | client (Aiven Apps)     |
+----------------------+                +-------------------------+

         lineage / metadata
         <-------------------------------+
+-------------------------+              |
| Aiven for DataHub       |  ingests metadata from PG, Kafka, CH
| datahub-1c8ec127        |  via the Aiven DataHub Connector Wizard
+-------------------------+
```

## Components in this repo

- [`webshop-simulator/`](./webshop-simulator) — Node.js/TypeScript app deployed on **Aiven Apps**. Continuously simulates CRUD activity (orders, customers, restocks, repricings) on the PostgreSQL service. EMEA-weighted regions so the headline demo query ("most popular product, last 30 days, EMEA") makes sense.
- [`dashboard-api/`](./dashboard-api) — Tiny Express + `@clickhouse/client` service that exposes the demo's analytical questions as JSON endpoints (top products by units / revenue, orders timeseries, regions / categories breakdown, live order feed). Backed by `clickhouse-2a6274d2`.
- [`dashboard-web/`](./dashboard-web) — React + Vite + Recharts SPA that visualises the data live. The Express server it ships with serves the static SPA and proxies `/api/*` to `dashboard-api` (URL injected by Aiven Apps).

## Aiven services backing the demo

| Service | Type | Plan | Role |
| --- | --- | --- | --- |
| `pg-37c7de3b` | PostgreSQL 17 | startup-4 | Webstore source-of-truth, written to by the simulator |
| `kafkaconnect-30e121dd` | Kafka Connect | startup-4 | Hosts the Debezium PG source connector |
| `kafka-1b5cb1e7` | Kafka 4.1 (3 brokers) | startup-4 | CDC stream backbone |
| `clickhouse-2a6274d2` | ClickHouse 25.3 | startup-8 | Analytics warehouse, read by dashboard-api |
| `datahub-1c8ec127` (+ stack) | DataHub | startup-1 | Catalog + lineage across PG, Kafka, CH |
| `webshop-simulator` | Aiven App | startup-50 | Generates live transactions |
| `dashboard-api` | Aiven App | startup-50 | REST over CH analytics tables |
| `dashboard-web` | Aiven App | startup-50 | React SPA, proxy in front of dashboard-api |

## Demo storyline (3-minute build-history version)

1. PostgreSQL holds the live webstore — written to continuously by `webshop-simulator`.
2. CDC via Debezium streams every change into Kafka topics (`webshop.public.{customers,products,orders,order_items}`).
3. ClickHouse consumes those topics natively (Kafka Engine tables → Materialized Views → `ReplacingMergeTree` storage), giving low-latency analytics on the same data.
4. DataHub picks up metadata from PG, Kafka and ClickHouse via the **DataHub Connector Wizard**, so lineage from source column to dashboard chart is automatic.
5. The dashboard is "just an app on the platform": `dashboard-api` (REST over CH) + `dashboard-web` (React + Recharts), both deployed via **Aiven Apps**.

## Where each piece lives

| Concern | Lives in |
| --- | --- |
| Schema + simulator code | [`webshop-simulator/`](./webshop-simulator) |
| ClickHouse storage tables + materialized views | Defined manually in CH (see DDL in chat history) |
| Analytical endpoints (`/top-products`, `/timeseries/orders`, …) | [`dashboard-api/`](./dashboard-api) |
| Charts, KPIs, live feed | [`dashboard-web/`](./dashboard-web) |
| DataHub source connectors | Aiven Console → DataHub Connector Wizard |
