import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import { query } from "./db";
import { ensureWarehouseViews } from "./views";
import {
  CATEGORY_REVENUE,
  LIVE_ORDERS,
  ORDERS_TIMESERIES,
  ORDERS_TIMESERIES_BY_REGION,
  REGIONS_SUMMARY,
  STATUS_BREAKDOWN,
  TOP_PRODUCTS_BY_REVENUE,
  TOP_PRODUCTS_BY_UNITS,
  TOTALS,
} from "./queries";

const PORT = parseInt(process.env.PORT ?? "3000", 10);

function asInt(v: unknown, fallback: number, min: number, max: number): number {
  const n = typeof v === "string" ? parseInt(v, 10) : NaN;
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

function asString(v: unknown, fallback: string): string {
  return typeof v === "string" ? v : fallback;
}

async function main() {
  // Bootstrap derived ClickHouse views before serving traffic. dashboard-api
  // is the only thing in the VPC that has both ClickHouse credentials and
  // its own startup hook, so it doubles as the warehouse view materialiser:
  // the dbt project (lightdash-dbt) is the source of truth for definitions,
  // and this step keeps the live CH views in sync. Idempotent — re-runs every
  // deploy and skips any view that's already up to date.
  await ensureWarehouseViews();

  const app = express();
  app.use(cors());
  app.use(express.json());

  app.get("/health", (_req, res) => res.json({ ok: true }));

  app.get("/", async (_req: Request, res: Response, next: NextFunction) => {
    try {
      const rows = await query<{
        customers: string;
        products: string;
        orders: string;
        order_items: string;
      }>(TOTALS);
      res.json({
        service: "dashboard-api",
        clickhouse: "connected",
        totals: rows[0],
      });
    } catch (err) {
      next(err);
    }
  });

  app.get("/top-products", async (req, res, next) => {
    try {
      const days = asInt(req.query.days, 30, 1, 365);
      const limit = asInt(req.query.limit, 10, 1, 100);
      const region = asString(req.query.region, "EMEA");
      const rows = await query(TOP_PRODUCTS_BY_UNITS, { days, limit, region });
      res.json({ region, days, products: rows });
    } catch (err) {
      next(err);
    }
  });

  app.get("/revenue-leaders", async (req, res, next) => {
    try {
      const days = asInt(req.query.days, 30, 1, 365);
      const limit = asInt(req.query.limit, 10, 1, 100);
      const region = asString(req.query.region, "EMEA");
      const rows = await query(TOP_PRODUCTS_BY_REVENUE, { days, limit, region });
      res.json({ region, days, products: rows });
    } catch (err) {
      next(err);
    }
  });

  app.get("/timeseries/orders", async (req, res, next) => {
    try {
      const hours = asInt(req.query.hours, 24, 1, 720);
      const bucket_seconds = asInt(req.query.bucket_seconds, 60, 10, 3600);
      const region = asString(req.query.region, "");
      const byRegion = req.query.by_region === "1";
      const rows = byRegion
        ? await query(ORDERS_TIMESERIES_BY_REGION, { hours, bucket_seconds })
        : await query(ORDERS_TIMESERIES, { hours, bucket_seconds, region });
      res.json({ hours, bucket_seconds, region: byRegion ? null : region, points: rows });
    } catch (err) {
      next(err);
    }
  });

  app.get("/regions/summary", async (req, res, next) => {
    try {
      const hours = asInt(req.query.hours, 24, 1, 720);
      const rows = await query(REGIONS_SUMMARY, { hours });
      res.json({ hours, regions: rows });
    } catch (err) {
      next(err);
    }
  });

  app.get("/categories/summary", async (req, res, next) => {
    try {
      const days = asInt(req.query.days, 30, 1, 365);
      const region = asString(req.query.region, "EMEA");
      const rows = await query(CATEGORY_REVENUE, { days, region });
      res.json({ region, days, categories: rows });
    } catch (err) {
      next(err);
    }
  });

  app.get("/orders/status", async (req, res, next) => {
    try {
      const hours = asInt(req.query.hours, 24, 1, 720);
      const rows = await query(STATUS_BREAKDOWN, { hours });
      res.json({ hours, breakdown: rows });
    } catch (err) {
      next(err);
    }
  });

  app.get("/live", async (req, res, next) => {
    try {
      const limit = asInt(req.query.limit, 25, 1, 200);
      const rows = await query(LIVE_ORDERS, { limit });
      res.json({ limit, orders: rows });
    } catch (err) {
      next(err);
    }
  });

  app.use(
    (err: Error, _req: Request, res: Response, _next: NextFunction) => {
      console.error("[error]", err.message);
      res.status(500).json({ error: err.message });
    },
  );

  app.listen(PORT, "0.0.0.0", () =>
    console.log(`[boot] dashboard-api listening on 0.0.0.0:${PORT}`),
  );
}

main().catch((err) => {
  console.error("[fatal]", err);
  process.exit(1);
});
