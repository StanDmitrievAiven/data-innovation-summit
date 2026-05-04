// All requests go to /api/* on this same origin; the Express server proxies them
// to the dashboard-api Aiven App via the API_URL env var.

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { accept: "application/json" },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return (await res.json()) as T;
}

export interface Totals {
  customers: string;
  products: string;
  orders: string;
  order_items: string;
}

export interface ProductRow {
  product: string;
  category: string;
  units_sold: number | string;
  revenue_eur: number | string;
}

export interface RegionRow {
  region: string;
  orders: number | string;
  revenue_eur: number | string;
  unique_customers: number | string;
  avg_order_value: number | string;
}

export interface TimeseriesPoint {
  bucket_at: string;
  orders: number | string;
  revenue_eur: number | string;
  region?: string;
}

export interface CategoryRow {
  category: string;
  orders: number | string;
  units_sold: number | string;
  revenue_eur: number | string;
}

export interface StatusRow {
  status: string;
  orders: number | string;
}

export interface LiveOrder {
  order_id: number | string;
  region: string;
  status: string;
  total: number | string;
  created_at: string;
  customer_name: string | null;
  country: string | null;
}

export const api = {
  totals: () => getJSON<{ totals: Totals }>(`/`),
  topProducts: (region: string, days: number, limit = 10) =>
    getJSON<{ products: ProductRow[] }>(
      `/top-products?region=${encodeURIComponent(region)}&days=${days}&limit=${limit}`,
    ),
  revenueLeaders: (region: string, days: number, limit = 10) =>
    getJSON<{ products: ProductRow[] }>(
      `/revenue-leaders?region=${encodeURIComponent(region)}&days=${days}&limit=${limit}`,
    ),
  timeseries: (hours: number, bucketSeconds: number, region = "") =>
    getJSON<{ points: TimeseriesPoint[] }>(
      `/timeseries/orders?hours=${hours}&bucket_seconds=${bucketSeconds}&region=${encodeURIComponent(region)}`,
    ),
  regionsSummary: (hours: number) =>
    getJSON<{ regions: RegionRow[] }>(`/regions/summary?hours=${hours}`),
  categories: (region: string, days: number) =>
    getJSON<{ categories: CategoryRow[] }>(
      `/categories/summary?region=${encodeURIComponent(region)}&days=${days}`,
    ),
  statusBreakdown: (hours: number) =>
    getJSON<{ breakdown: StatusRow[] }>(`/orders/status?hours=${hours}`),
  live: (limit = 25) => getJSON<{ orders: LiveOrder[] }>(`/live?limit=${limit}`),
};
