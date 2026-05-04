import { pool } from "./db";
import {
  pickRandom,
  pickWeighted,
  randomBetween,
  randomCustomer,
} from "./catalog";

interface CustomerRef {
  id: number;
  region: string;
}

interface ProductRef {
  id: number;
  price: number;
}

interface OrderRef {
  id: number;
  status: string;
}

export interface SimulatorState {
  paused: boolean;
  ratePerMinute: number;
  ops: {
    create_order: number;
    create_customer: number;
    update_order_status: number;
    restock_product: number;
    reprice_product: number;
    errors: number;
  };
  startedAt: string;
  lastOpAt?: string;
  lastOp?: string;
}

const ACTION_WEIGHTS = [
  { action: "create_order",        weight: 70 },
  { action: "create_customer",     weight: 10 },
  { action: "update_order_status", weight: 10 },
  { action: "restock_product",     weight:  5 },
  { action: "reprice_product",     weight:  5 },
] as const;

type ActionName = (typeof ACTION_WEIGHTS)[number]["action"];

const STATUS_NEXT: Record<string, string | null> = {
  pending: "processing",
  processing: "shipped",
  shipped: "delivered",
  delivered: null,
  cancelled: null,
};

export class Simulator {
  state: SimulatorState;
  private timer: NodeJS.Timeout | null = null;

  constructor(initialRatePerMinute: number) {
    this.state = {
      paused: false,
      ratePerMinute: initialRatePerMinute,
      ops: {
        create_order: 0,
        create_customer: 0,
        update_order_status: 0,
        restock_product: 0,
        reprice_product: 0,
        errors: 0,
      },
      startedAt: new Date().toISOString(),
    };
  }

  start(): void {
    this.scheduleNext();
  }

  stop(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  pause(): void {
    this.state.paused = true;
  }

  resume(): void {
    this.state.paused = false;
  }

  setRate(opsPerMinute: number): void {
    if (!Number.isFinite(opsPerMinute) || opsPerMinute < 0) return;
    // Cap to avoid runaway demos hammering the DB.
    this.state.ratePerMinute = Math.min(opsPerMinute, 6000);
  }

  private scheduleNext(): void {
    const rate = Math.max(this.state.ratePerMinute, 1);
    // Poisson-ish jitter around the target interval keeps the stream natural.
    const meanIntervalMs = (60_000 / rate) * (0.5 + Math.random());
    this.timer = setTimeout(() => {
      this.tick().finally(() => this.scheduleNext());
    }, meanIntervalMs);
  }

  private async tick(): Promise<void> {
    if (this.state.paused) return;
    const action = pickWeighted([...ACTION_WEIGHTS]).action as ActionName;
    try {
      switch (action) {
        case "create_order":         await this.createOrder(); break;
        case "create_customer":      await this.createCustomer(); break;
        case "update_order_status":  await this.updateOrderStatus(); break;
        case "restock_product":      await this.restockProduct(); break;
        case "reprice_product":      await this.repriceProduct(); break;
      }
      this.state.ops[action]++;
      this.state.lastOp = action;
      this.state.lastOpAt = new Date().toISOString();
    } catch (err) {
      this.state.ops.errors++;
      console.error(`[sim] ${action} failed:`, (err as Error).message);
    }
  }

  private async pickRandomCustomer(): Promise<CustomerRef | null> {
    const { rows } = await pool.query<CustomerRef>(
      `SELECT id, region FROM customers
       ORDER BY random()
       LIMIT 1`,
    );
    return rows[0] ?? null;
  }

  private async pickRandomProducts(n: number): Promise<ProductRef[]> {
    const { rows } = await pool.query<{ id: number; price: string }>(
      `SELECT id, price FROM products
       ORDER BY random()
       LIMIT $1`,
      [n],
    );
    return rows.map((r) => ({ id: r.id, price: Number(r.price) }));
  }

  private async createOrder(): Promise<void> {
    const customer = await this.pickRandomCustomer();
    if (!customer) return;

    const itemCount = randomBetween(1, 4);
    const products = await this.pickRandomProducts(itemCount);
    if (products.length === 0) return;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const orderRes = await client.query<{ id: number }>(
        `INSERT INTO orders (customer_id, status, total, region)
         VALUES ($1, 'pending', 0, $2)
         RETURNING id`,
        [customer.id, customer.region],
      );
      const orderId = orderRes.rows[0].id;

      let total = 0;
      for (const product of products) {
        const qty = randomBetween(1, 3);
        const lineTotal = qty * product.price;
        total += lineTotal;
        await client.query(
          `INSERT INTO order_items (order_id, product_id, quantity, unit_price)
           VALUES ($1, $2, $3, $4)`,
          [orderId, product.id, qty, product.price],
        );
        await client.query(
          `UPDATE products
              SET inventory = GREATEST(inventory - $1, 0),
                  updated_at = now()
            WHERE id = $2`,
          [qty, product.id],
        );
      }

      await client.query(
        `UPDATE orders
            SET total = $1, updated_at = now()
          WHERE id = $2`,
        [total, orderId],
      );

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  private async createCustomer(): Promise<void> {
    const c = randomCustomer();
    await pool.query(
      `INSERT INTO customers (email, name, region, country)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (email) DO NOTHING`,
      [c.email, c.name, c.region, c.country],
    );
  }

  private async updateOrderStatus(): Promise<void> {
    const { rows } = await pool.query<OrderRef>(
      `SELECT id, status FROM orders
        WHERE status IN ('pending', 'processing', 'shipped')
        ORDER BY random()
        LIMIT 1`,
    );
    const order = rows[0];
    if (!order) return;

    const next = STATUS_NEXT[order.status];
    if (!next) return;

    await pool.query(
      `UPDATE orders SET status = $1, updated_at = now() WHERE id = $2`,
      [next, order.id],
    );
  }

  private async restockProduct(): Promise<void> {
    const restock = randomBetween(50, 500);
    await pool.query(
      `UPDATE products
          SET inventory = inventory + $1,
              updated_at = now()
        WHERE id = (SELECT id FROM products ORDER BY random() LIMIT 1)`,
      [restock],
    );
  }

  private async repriceProduct(): Promise<void> {
    // Small +/- 5% nudge on price for an existing product.
    const factor = 1 + (Math.random() * 0.1 - 0.05);
    await pool.query(
      `UPDATE products
          SET price = ROUND(price * $1::numeric, 2),
              updated_at = now()
        WHERE id = (SELECT id FROM products ORDER BY random() LIMIT 1)`,
      [factor.toFixed(4)],
    );
  }
}
