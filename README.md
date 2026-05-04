# Data Innovation Summit Demo

End-to-end Aiven Platform demo built on the [`data-innovation-summit`](https://console.aiven.io/) project.

Pitch: **OLTP and OLAP on one platform**, with **lineage and governance** built in, plus an easy workflow for **building applications on top of your data** — all in Aiven, on any cloud.

## Architecture

```
                       writes
+---------------------+   |   +-------------------------+
| webshop-simulator   |---+-->| Aiven for PostgreSQL    |
| (Aiven Apps)        |       | pg-37c7de3b             |
+---------------------+       +-------------------------+
                                          |
                                  Debezium CDC
                                          v
                              +-------------------------+
                              | Aiven for Apache Kafka  |
                              | kafka-1b5cb1e7          |
                              +-------------------------+
                                          |
                              clickhouse_kafka integration
                                          v
                              +-------------------------+
                              | Aiven for ClickHouse    |
                              | clickhouse-2a6274d2     |
                              +-------------------------+

         lineage / metadata
         <----------------------
+-------------------------+
| Aiven for DataHub       |
| datahub-1c8ec127        |
+-------------------------+
```

## Components in this repo

- [`webshop-simulator/`](./webshop-simulator) — Node.js/TypeScript app deployed on **Aiven Apps**. Continuously simulates CRUD activity (orders, customers, restocks, repricings) on the PostgreSQL service. EMEA-weighted regions so the headline demo query ("most popular product, last 30 days, EMEA") makes sense.

More apps will be added (e.g. `dashboard-api`, `dashboard-web`) as the demo grows.

## Aiven services backing the demo

| Service | Type | Plan |
| --- | --- | --- |
| `pg-37c7de3b` | PostgreSQL 17 | startup-4 |
| `kafka-1b5cb1e7` | Kafka 4.1 (3 brokers) | startup-4 |
| `kafkaconnect-30e121dd` | Kafka Connect | startup-4 |
| `clickhouse-2a6274d2` | ClickHouse 25.3 | startup-8 |
| `datahub-1c8ec127` (+ stack) | DataHub | startup-1 |
