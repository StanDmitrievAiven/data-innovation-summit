import { pool } from "./db";

const STATEMENTS: string[] = [
  `CREATE TABLE IF NOT EXISTS customers (
     id          BIGSERIAL PRIMARY KEY,
     email       TEXT UNIQUE NOT NULL,
     name        TEXT NOT NULL,
     region      TEXT NOT NULL,
     country     TEXT NOT NULL,
     created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE TABLE IF NOT EXISTS products (
     id          BIGSERIAL PRIMARY KEY,
     sku         TEXT UNIQUE NOT NULL,
     name        TEXT NOT NULL,
     category    TEXT NOT NULL,
     price       NUMERIC(10,2) NOT NULL,
     inventory   INTEGER NOT NULL DEFAULT 0,
     created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE TABLE IF NOT EXISTS orders (
     id          BIGSERIAL PRIMARY KEY,
     customer_id BIGINT NOT NULL REFERENCES customers(id),
     status      TEXT NOT NULL DEFAULT 'pending',
     total       NUMERIC(12,2) NOT NULL DEFAULT 0,
     region      TEXT NOT NULL,
     created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE TABLE IF NOT EXISTS order_items (
     id          BIGSERIAL PRIMARY KEY,
     order_id    BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
     product_id  BIGINT NOT NULL REFERENCES products(id),
     quantity    INTEGER NOT NULL,
     unit_price  NUMERIC(10,2) NOT NULL
   )`,
  `CREATE INDEX IF NOT EXISTS idx_orders_created_at         ON orders(created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_orders_region_created_at  ON orders(region, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_orders_customer_id        ON orders(customer_id)`,
  `CREATE INDEX IF NOT EXISTS idx_order_items_order_id      ON order_items(order_id)`,
  `CREATE INDEX IF NOT EXISTS idx_order_items_product_id    ON order_items(product_id)`,
  // REPLICA IDENTITY FULL gives Debezium a full before-image on UPDATE/DELETE.
  // Useful for richer CDC payloads downstream in Kafka and ClickHouse.
  `ALTER TABLE customers   REPLICA IDENTITY FULL`,
  `ALTER TABLE products    REPLICA IDENTITY FULL`,
  `ALTER TABLE orders      REPLICA IDENTITY FULL`,
  `ALTER TABLE order_items REPLICA IDENTITY FULL`,
];

export async function ensureSchema(): Promise<void> {
  const client = await pool.connect();
  try {
    for (const sql of STATEMENTS) {
      await client.query(sql);
    }
  } finally {
    client.release();
  }
}
