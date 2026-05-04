# webshop-simulator

A small Node.js + TypeScript service that simulates a live webstore on Aiven for PostgreSQL.

It is the **source** of the demo pipeline:

```
webshop-simulator (this app)  --writes-->  Aiven PostgreSQL
                                                 |
                                            (Debezium CDC)
                                                 v
                                          Aiven for Kafka
                                                 |
                                       clickhouse_kafka integration
                                                 v
                                          Aiven for ClickHouse
```

DataHub ingests metadata from PG, Kafka, and ClickHouse to render the lineage graph.

## What it does

On startup:
1. Creates the schema (`customers`, `products`, `orders`, `order_items`) if missing.
2. Sets `REPLICA IDENTITY FULL` on each table so Debezium captures full before-images.
3. Seeds the product catalog (~50 products) and a pool of customers (default 300).
4. Backfills ~90 days of historical orders (idempotent: only if `orders` is empty).

Then continuously generates weighted random CRUD:

| Action               | Default weight |
| -------------------- | -------------- |
| `create_order`       | 70%            |
| `create_customer`    | 10%            |
| `update_order_status`| 10%            |
| `restock_product`    | 5%             |
| `reprice_product`    | 5%             |

Customer regions are weighted EMEA-heavy so the "most popular product, last 30 days, EMEA" demo query is meaningful.

## HTTP control surface

- `GET  /health` – liveness for Aiven Apps.
- `GET  /` – stats: simulator state, table counts, last-5-minute orders per region.
- `POST /pause` – stop generating new operations.
- `POST /resume` – resume generation.
- `POST /rate` – change rate. Accepts JSON body `{"ops_per_minute": 120}` or query param `?ops_per_minute=120`.

## Environment variables

| Name                       | Default | Notes                                                                |
| -------------------------- | ------- | -------------------------------------------------------------------- |
| `PORT`                     | `3000`  | HTTP port (matches `EXPOSE` in the Dockerfile).                      |
| `DATABASE_URL`             | —       | Auto-injected by Aiven Apps PG integration.                          |
| `PROJECT_CA_CERT`          | —       | Auto-injected by Aiven Apps PG integration (base64 PEM).             |
| `ORDERS_PER_MINUTE`        | `60`    | Initial generation rate. Overridable at runtime via `POST /rate`.    |
| `BACKFILL_DAYS`            | `90`    | How many days of historical orders to seed on first boot.            |
| `BACKFILL_ORDERS_PER_DAY`  | `50`    | Average historical orders per day for the backfill.                  |
| `SEED_CUSTOMERS`           | `300`   | Target customer pool size after seeding (existing rows count).       |

## Local development

```bash
npm install
DATABASE_URL='postgres://user:pass@host:5432/db?sslmode=require' npm run build
npm start
```

For Aiven, no env wiring is needed beyond the Apps PG integration — credentials and the CA certificate are injected automatically.
