import { pool } from "./db";
import {
  PRODUCT_CATALOG,
  pickRandom,
  pickWeighted,
  randomBetween,
  randomCustomer,
  REGIONS,
} from "./catalog";

const BACKFILL_DAYS = parseInt(process.env.BACKFILL_DAYS ?? "90", 10);
const BACKFILL_ORDERS_PER_DAY = parseInt(
  process.env.BACKFILL_ORDERS_PER_DAY ?? "50",
  10,
);
const SEED_CUSTOMERS = parseInt(process.env.SEED_CUSTOMERS ?? "300", 10);

interface CustomerRow {
  id: number;
  region: string;
}

interface ProductRow {
  id: number;
  price: number;
}

async function seedProducts(): Promise<void> {
  const { rows } = await pool.query<{ count: string }>(
    "SELECT count(*)::text AS count FROM products",
  );
  if (Number(rows[0].count) > 0) return;

  console.log(`[backfill] seeding ${PRODUCT_CATALOG.length} products`);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const p of PRODUCT_CATALOG) {
      await client.query(
        `INSERT INTO products (sku, name, category, price, inventory)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (sku) DO NOTHING`,
        [p.sku, p.name, p.category, p.price, p.inventory],
      );
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function seedCustomers(targetCount: number): Promise<CustomerRow[]> {
  const { rows: existingRows } = await pool.query<{ count: string }>(
    "SELECT count(*)::text AS count FROM customers",
  );
  const existing = Number(existingRows[0].count);

  if (existing < targetCount) {
    const toCreate = targetCount - existing;
    console.log(`[backfill] seeding ${toCreate} customers (existing=${existing})`);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (let i = 0; i < toCreate; i++) {
        const c = randomCustomer();
        await client.query(
          `INSERT INTO customers (email, name, region, country)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (email) DO NOTHING`,
          [c.email, c.name, c.region, c.country],
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  const { rows } = await pool.query<CustomerRow>(
    "SELECT id, region FROM customers",
  );
  return rows;
}

async function loadProducts(): Promise<ProductRow[]> {
  const { rows } = await pool.query<{ id: number; price: string }>(
    "SELECT id, price FROM products",
  );
  return rows.map((r) => ({ id: r.id, price: Number(r.price) }));
}

function randomTimestampInPast(maxDays: number): Date {
  const now = Date.now();
  const offsetMs = Math.random() * maxDays * 24 * 60 * 60 * 1000;
  return new Date(now - offsetMs);
}

const STATUSES_FOR_BACKFILL = [
  { status: "delivered", weight: 70 },
  { status: "shipped",   weight: 15 },
  { status: "processing", weight: 10 },
  { status: "cancelled", weight:  5 },
];

async function backfillOrders(
  customers: CustomerRow[],
  products: ProductRow[],
): Promise<void> {
  const { rows } = await pool.query<{ count: string }>(
    "SELECT count(*)::text AS count FROM orders",
  );
  if (Number(rows[0].count) > 0) {
    console.log(
      `[backfill] orders already populated (${rows[0].count} rows), skipping`,
    );
    return;
  }

  const totalOrders = BACKFILL_DAYS * BACKFILL_ORDERS_PER_DAY;
  console.log(
    `[backfill] generating ~${totalOrders} historical orders over ${BACKFILL_DAYS} days`,
  );

  const client = await pool.connect();
  try {
    let written = 0;
    for (let i = 0; i < totalOrders; i++) {
      const customer = pickRandom(customers);
      const ts = randomTimestampInPast(BACKFILL_DAYS);
      const status = pickWeighted(STATUSES_FOR_BACKFILL).status;
      const itemCount = randomBetween(1, 4);

      await client.query("BEGIN");
      try {
        const orderRes = await client.query<{ id: number }>(
          `INSERT INTO orders (customer_id, status, total, region, created_at, updated_at)
           VALUES ($1, $2, 0, $3, $4, $4)
           RETURNING id`,
          [customer.id, status, customer.region, ts],
        );
        const orderId = orderRes.rows[0].id;

        let total = 0;
        const chosen = new Set<number>();
        for (let k = 0; k < itemCount; k++) {
          const product = pickRandom(products);
          if (chosen.has(product.id)) continue;
          chosen.add(product.id);
          const qty = randomBetween(1, 3);
          const lineTotal = qty * product.price;
          total += lineTotal;
          await client.query(
            `INSERT INTO order_items (order_id, product_id, quantity, unit_price)
             VALUES ($1, $2, $3, $4)`,
            [orderId, product.id, qty, product.price],
          );
        }

        await client.query(
          `UPDATE orders SET total = $1, updated_at = $2 WHERE id = $3`,
          [total, ts, orderId],
        );

        await client.query("COMMIT");
        written++;
        if (written % 500 === 0) {
          console.log(`[backfill] inserted ${written}/${totalOrders} orders`);
        }
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    }
    console.log(`[backfill] done: ${written} orders inserted`);
  } finally {
    client.release();
  }
}

export async function runBackfill(): Promise<void> {
  await seedProducts();
  const customers = await seedCustomers(SEED_CUSTOMERS);
  const products = await loadProducts();
  await backfillOrders(customers, products);
}
