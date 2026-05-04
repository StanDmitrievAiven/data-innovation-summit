import React, { useEffect, useState } from "react";
import { api } from "./api";
import type {
  LiveOrder,
  ProductRow,
  RegionRow,
  TimeseriesPoint,
  Totals,
} from "./api";
import { Kpi } from "./components/Kpi";
import { TopProducts } from "./components/TopProducts";
import { OrdersChart } from "./components/OrdersChart";
import { RegionChart } from "./components/RegionChart";
import { LiveFeed } from "./components/LiveFeed";

const REFRESH_MS = 5000;
const REGIONS = ["EMEA", "AMER", "APAC", ""];

function fmt(n: string | number): string {
  return Intl.NumberFormat("en").format(Number(n));
}

export function App() {
  const [region, setRegion] = useState("EMEA");
  const [days, setDays] = useState(30);

  const [totals, setTotals] = useState<Totals | null>(null);
  const [topByUnits, setTopByUnits] = useState<ProductRow[]>([]);
  const [topByRevenue, setTopByRevenue] = useState<ProductRow[]>([]);
  const [timeseries, setTimeseries] = useState<TimeseriesPoint[]>([]);
  const [regions, setRegions] = useState<RegionRow[]>([]);
  const [live, setLive] = useState<LiveOrder[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [refreshedAt, setRefreshedAt] = useState<Date | null>(null);

  async function refresh() {
    try {
      const [t, units, revenue, ts, regs, lv] = await Promise.all([
        api.totals(),
        api.topProducts(region, days, 10),
        api.revenueLeaders(region, days, 10),
        api.timeseries(24, 60, ""),
        api.regionsSummary(24),
        api.live(25),
      ]);
      setTotals(t.totals);
      setTopByUnits(units.products);
      setTopByRevenue(revenue.products);
      setTimeseries(ts.points);
      setRegions(regs.regions);
      setLive(lv.orders);
      setError(null);
      setRefreshedAt(new Date());
    } catch (e) {
      setError((e as Error).message);
    }
  }

  useEffect(() => {
    void refresh();
    const id = setInterval(refresh, REFRESH_MS);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [region, days]);

  const totalRevenue24h = regions.reduce(
    (s, r) => s + Number(r.revenue_eur),
    0,
  );
  const totalOrders24h = regions.reduce((s, r) => s + Number(r.orders), 0);

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>Webstore Live Insights</h1>
          <div className="subtitle">
            PostgreSQL → Debezium → Kafka → ClickHouse, on Aiven
          </div>
        </div>
        <div className="live">
          <span className="dot" />
          {refreshedAt
            ? `live • last refresh ${refreshedAt.toLocaleTimeString()}`
            : "connecting…"}
        </div>
      </header>

      {error ? <div className="error">⚠ {error}</div> : null}

      <div className="controls">
        <label>
          Region:
          <select value={region} onChange={(e) => setRegion(e.target.value)}>
            {REGIONS.map((r) => (
              <option key={r || "all"} value={r}>
                {r || "All"}
              </option>
            ))}
          </select>
        </label>
        <label>
          Window:
          <select value={days} onChange={(e) => setDays(Number(e.target.value))}>
            <option value={1}>last 1 day</option>
            <option value={7}>last 7 days</option>
            <option value={30}>last 30 days</option>
            <option value={90}>last 90 days</option>
          </select>
        </label>
      </div>

      <div className="kpis">
        <Kpi label="Customers" value={totals ? fmt(totals.customers) : "—"} />
        <Kpi label="Products" value={totals ? fmt(totals.products) : "—"} />
        <Kpi label="Orders (total)" value={totals ? fmt(totals.orders) : "—"} />
        <Kpi
          label="Order items (total)"
          value={totals ? fmt(totals.order_items) : "—"}
        />
        <Kpi
          label="Orders (24h)"
          value={fmt(totalOrders24h)}
          hint={`across ${regions.length} regions`}
        />
        <Kpi
          label="Revenue (24h)"
          value={`€${fmt(Math.round(totalRevenue24h))}`}
          hint="all regions"
        />
      </div>

      <div className="grid">
        <OrdersChart
          title="Orders per minute (last 24h, all regions)"
          points={timeseries}
        />
        <RegionChart title="Orders by region (last 24h)" rows={regions} />
      </div>

      <div className="grid">
        <TopProducts
          title={`Top products by units — ${region || "All"}, last ${days} days`}
          rows={topByUnits}
          metric="units_sold"
        />
        <TopProducts
          title={`Top products by revenue — ${region || "All"}, last ${days} days`}
          rows={topByRevenue}
          metric="revenue_eur"
          unit="eur"
        />
      </div>

      <LiveFeed orders={live} />
    </div>
  );
}
