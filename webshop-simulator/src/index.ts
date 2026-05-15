import express, { Request, Response } from "express";
import { pool, withRetry } from "./db";
import { ensureSchema } from "./schema";
import { ensureMarketing } from "./marketing";
import { runBackfill } from "./backfill";
import { Simulator } from "./simulator";

const PORT = parseInt(process.env.PORT ?? "3000", 10);
const RATE_PER_MINUTE = parseInt(process.env.ORDERS_PER_MINUTE ?? "60", 10);

async function main(): Promise<void> {
  console.log(`[boot] starting webshop-simulator on port ${PORT}`);

  await withRetry(() => ensureSchema());
  console.log("[boot] schema ready");

  // Marketing reference data lives in PG but does not flow through CDC — it's
  // read live from ClickHouse via the PG→CH service integration. The simulator
  // bootstraps the schema + an evergreen seed once, then stays out of its way.
  await withRetry(() => ensureMarketing());
  console.log("[boot] marketing reference data ready");

  await runBackfill();
  console.log("[boot] backfill complete");

  const sim = new Simulator(RATE_PER_MINUTE);
  sim.start();
  console.log(`[boot] simulator started at ${RATE_PER_MINUTE} ops/minute`);

  const app = express();
  app.use(express.json());

  app.get("/health", (_req: Request, res: Response) => {
    res.json({ ok: true });
  });

  app.get("/", async (_req: Request, res: Response) => {
    try {
      const counts = await pool.query<{
        customers: string;
        products: string;
        orders: string;
        order_items: string;
      }>(`
        SELECT
          (SELECT count(*)::text FROM customers)   AS customers,
          (SELECT count(*)::text FROM products)    AS products,
          (SELECT count(*)::text FROM orders)      AS orders,
          (SELECT count(*)::text FROM order_items) AS order_items
      `);

      const recent = await pool.query<{ region: string; orders: string }>(`
        SELECT region, count(*)::text AS orders
          FROM orders
         WHERE created_at > now() - interval '5 minutes'
         GROUP BY region
         ORDER BY count(*) DESC
      `);

      res.json({
        service: "webshop-simulator",
        state: sim.state,
        totals: counts.rows[0],
        last_5min_by_region: recent.rows,
      });
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.post("/pause", (_req: Request, res: Response) => {
    sim.pause();
    res.json({ paused: true });
  });

  app.post("/resume", (_req: Request, res: Response) => {
    sim.resume();
    res.json({ paused: false });
  });

  app.post("/rate", (req: Request, res: Response) => {
    const value =
      typeof req.body?.ops_per_minute === "number"
        ? req.body.ops_per_minute
        : Number(req.query.ops_per_minute);
    if (!Number.isFinite(value) || value < 0) {
      res.status(400).json({ error: "ops_per_minute must be a non-negative number" });
      return;
    }
    sim.setRate(value);
    res.json({ ratePerMinute: sim.state.ratePerMinute });
  });

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`[boot] listening on 0.0.0.0:${PORT}`);
  });

  const shutdown = async (signal: string) => {
    console.log(`[shutdown] received ${signal}`);
    sim.stop();
    server.close(() => {
      pool.end().finally(() => process.exit(0));
    });
    setTimeout(() => process.exit(1), 10_000).unref();
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  console.error("[fatal]", err);
  process.exit(1);
});
