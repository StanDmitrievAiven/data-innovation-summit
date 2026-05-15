#!/usr/bin/env python3
"""
Build the "Webstore Live Insights" Lightdash dashboard via the REST API.

Mirrors the bespoke React dashboard (`dashboard-web` + `dashboard-api`) so the
demo shows the same data plane (Aiven for ClickHouse, default.* gold tables)
explored through Lightdash's drag-and-drop UI on top of dbt-defined dimensions
and metrics.

Idempotent: every chart and the dashboard itself are matched by name and
deleted before being re-created, so running this script multiple times leaves
exactly one copy of each object.

Env vars (or override at the top of __main__):
    LIGHTDASH_URL    Lightdash base URL, e.g. https://...aiven.app
    LIGHTDASH_TOKEN  Personal Access Token (ldpat_...).
"""

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# HTTP helpers


class LightdashClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        *,
        expected: Iterable[int] = (200, 201),
    ) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"ApiKey {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read().decode() or "{}"
                if resp.status not in expected:
                    raise RuntimeError(
                        f"{method} {path} -> {resp.status}: {raw[:500]}"
                    )
                return json.loads(raw).get("results", json.loads(raw))
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode(errors="replace")
            raise RuntimeError(
                f"{method} {path} -> {e.code}: {body_txt[:800]}"
            ) from None

    def get(self, path: str) -> Any:
        return self._request("GET", path)

    def post(self, path: str, body: dict) -> Any:
        return self._request("POST", path, body)

    def patch(self, path: str, body: dict) -> Any:
        return self._request("PATCH", path, body)

    def delete(self, path: str) -> Any:
        return self._request("DELETE", path, expected=(200, 204))


# ---------------------------------------------------------------------------
# Chart and dashboard definitions
#
# We model every chart as a small dict that the build functions below turn
# into Lightdash API payloads. The metric and dimension IDs use Lightdash's
# convention <table>_<column>, matching what `GET /explores/{name}` returned.


ORDERS = "orders_enriched"
CUSTOMERS = "customers_clean"
PRODUCTS = "products_catalog"


def f(table: str, name: str) -> str:
    """Lightdash field id = `<tableName>_<column>`."""
    return f"{table}_{name}"


# Big-number KPI tiles (mirror /api/totals in the React dashboard)
KPI_TILES = [
    {
        "name": "Total customers",
        "description": "Distinct customers in the OLTP source-of-truth (Aiven for PostgreSQL), streamed through Debezium + Kafka + ClickHouse.",
        "table": CUSTOMERS,
        "metric": f(CUSTOMERS, "customer_count"),
    },
    {
        "name": "Total products",
        "description": "Distinct active products in the catalog.",
        "table": PRODUCTS,
        "metric": f(PRODUCTS, "product_count"),
    },
    {
        "name": "Total orders",
        "description": "Distinct orders across every region.",
        "table": ORDERS,
        "metric": f(ORDERS, "order_count"),
    },
    {
        "name": "Total revenue",
        "description": "GMV across every region, every status. Sum of line totals.",
        "table": ORDERS,
        "metric": f(ORDERS, "total_revenue"),
    },
]


# ---------------------------------------------------------------------------
# Chart payload builders
#
# Lightdash's chart payload has three main blocks:
#   - metricQuery (dimensions + metrics + filters + sorts + limit)
#   - chartConfig.type + chartConfig.config (per chart-type schema)
#   - tableConfig.columnOrder (the order columns appear in the table view)


def base_metric_query(
    dimensions: list[str],
    metrics: list[str],
    sorts: list[dict] | None = None,
    limit: int = 500,
    filters: dict | None = None,
) -> dict:
    return {
        "exploreName": None,
        "dimensions": dimensions,
        "metrics": metrics,
        "filters": filters or {},
        "sorts": sorts or [],
        "limit": limit,
        "tableCalculations": [],
        "additionalMetrics": [],
    }


def big_number_chart(*, name: str, description: str, table: str, metric: str) -> dict:
    return {
        "name": name,
        "description": description,
        "tableName": table,
        "metricQuery": base_metric_query(
            dimensions=[],
            metrics=[metric],
            limit=1,
        ),
        "chartConfig": {
            "type": "big_number",
            "config": {
                "label": name,
                "style": "thousands",
                "selectedField": metric,
            },
        },
        "tableConfig": {"columnOrder": [metric]},
    }


def cartesian_chart(
    *,
    name: str,
    description: str,
    table: str,
    x: str,
    ys: list[str],
    chart_type: str = "bar",
    stack: bool = False,
    pivot: str | None = None,
    sorts: list[dict] | None = None,
    limit: int = 500,
    extra_dims: list[str] | None = None,
) -> dict:
    """Cartesian (bar / line / area / column) chart."""
    dims = [x] + (extra_dims or [])
    if pivot and pivot not in dims:
        dims.append(pivot)

    series = []
    for y in ys:
        s = {
            "encode": {
                "xRef": {"field": x},
                "yRef": {"field": y},
            },
            "type": chart_type,
        }
        if stack:
            s["stack"] = "total"
        series.append(s)

    return {
        "name": name,
        "description": description,
        "tableName": table,
        "metricQuery": base_metric_query(
            dimensions=dims,
            metrics=ys,
            sorts=sorts or [{"fieldId": x, "descending": False}],
            limit=limit,
        ),
        "chartConfig": {
            "type": "cartesian",
            "config": {
                "layout": {
                    "xField": x,
                    "yField": ys,
                    "flipAxes": False,
                    "showGridX": False,
                    "showGridY": True,
                },
                "eChartsConfig": {
                    "series": series,
                    "legend": {"show": True},
                    "tooltip": {"trigger": "axis"},
                },
                "pivotedDimensions": [pivot] if pivot else [],
            },
        },
        "tableConfig": {"columnOrder": dims + ys},
        "pivotConfig": {"columns": [pivot]} if pivot else None,
    }


def horizontal_bar_chart(
    *,
    name: str,
    description: str,
    table: str,
    category: str,
    metric: str,
    extra_dims: list[str] | None = None,
    limit: int = 10,
) -> dict:
    dims = [category] + (extra_dims or [])
    return {
        "name": name,
        "description": description,
        "tableName": table,
        "metricQuery": base_metric_query(
            dimensions=dims,
            metrics=[metric],
            sorts=[{"fieldId": metric, "descending": True}],
            limit=limit,
        ),
        "chartConfig": {
            "type": "cartesian",
            "config": {
                "layout": {
                    "xField": metric,
                    "yField": [category],
                    "flipAxes": True,
                    "showGridX": True,
                    "showGridY": False,
                },
                "eChartsConfig": {
                    "series": [
                        {
                            "encode": {
                                "xRef": {"field": metric},
                                "yRef": {"field": category},
                            },
                            "type": "bar",
                        }
                    ],
                    "legend": {"show": False},
                    "tooltip": {"trigger": "axis"},
                },
            },
        },
        "tableConfig": {"columnOrder": dims + [metric]},
    }


def pie_chart(
    *,
    name: str,
    description: str,
    table: str,
    group: str,
    metric: str,
    limit: int = 50,
) -> dict:
    return {
        "name": name,
        "description": description,
        "tableName": table,
        "metricQuery": base_metric_query(
            dimensions=[group],
            metrics=[metric],
            sorts=[{"fieldId": metric, "descending": True}],
            limit=limit,
        ),
        "chartConfig": {
            "type": "pie",
            "config": {
                "groupFieldIds": [group],
                "metricId": metric,
                "isDonut": True,
                "showLegend": True,
                "showLabels": True,
                "showPercentage": True,
            },
        },
        "tableConfig": {"columnOrder": [group, metric]},
    }


def table_chart(
    *,
    name: str,
    description: str,
    table: str,
    dimensions: list[str],
    metrics: list[str],
    sorts: list[dict] | None = None,
    limit: int = 50,
) -> dict:
    return {
        "name": name,
        "description": description,
        "tableName": table,
        "metricQuery": base_metric_query(
            dimensions=dimensions,
            metrics=metrics,
            sorts=sorts or [],
            limit=limit,
        ),
        "chartConfig": {
            "type": "table",
            "config": {
                "showColumnCalculation": False,
                "showRowCalculation": False,
                "showTableNames": False,
                "showResultsTotal": True,
                "hideRowNumbers": False,
                "metricsAsRows": False,
            },
        },
        "tableConfig": {"columnOrder": dimensions + metrics},
    }


# ---------------------------------------------------------------------------
# Build all the charts for the dashboard


def all_chart_payloads() -> list[dict]:
    """Returns the list of saved-chart payloads to push to Lightdash."""
    charts: list[dict] = []

    for k in KPI_TILES:
        charts.append(big_number_chart(**k))

    charts.append(
        cartesian_chart(
            name="Orders timeseries (last 30 days)",
            description="Daily order count and revenue across every region. Mirrors /api/orders/timeseries in the React dashboard.",
            table=ORDERS,
            x=f(ORDERS, "order_created_date_day"),
            ys=[f(ORDERS, "order_count"), f(ORDERS, "total_revenue")],
            chart_type="line",
            sorts=[{"fieldId": f(ORDERS, "order_created_date_day"), "descending": False}],
            limit=500,
        )
    )

    charts.append(
        cartesian_chart(
            name="Orders by region (timeseries)",
            description="Daily orders per region. Pivoted on order_region so each region gets its own coloured line. Mirrors /api/orders/timeseries-by-region.",
            table=ORDERS,
            x=f(ORDERS, "order_created_date_day"),
            ys=[f(ORDERS, "order_count")],
            chart_type="line",
            pivot=f(ORDERS, "order_region"),
            sorts=[{"fieldId": f(ORDERS, "order_created_date_day"), "descending": False}],
            limit=2000,
        )
    )

    charts.append(
        cartesian_chart(
            name="Revenue by region",
            description="Total GMV per region. Mirrors /api/regions/summary.",
            table=ORDERS,
            x=f(ORDERS, "order_region"),
            ys=[f(ORDERS, "total_revenue"), f(ORDERS, "order_count"), f(ORDERS, "unique_customers")],
            chart_type="bar",
            sorts=[{"fieldId": f(ORDERS, "total_revenue"), "descending": True}],
            limit=20,
        )
    )

    charts.append(
        horizontal_bar_chart(
            name="Top 10 products by units sold",
            description="Top-sellers by lifetime units. Mirrors /api/products/top-by-units.",
            table=ORDERS,
            category=f(ORDERS, "product_name"),
            metric=f(ORDERS, "units_sold"),
            extra_dims=[f(ORDERS, "product_category")],
            limit=10,
        )
    )

    charts.append(
        horizontal_bar_chart(
            name="Top 10 products by revenue",
            description="Top-revenue products. Mirrors /api/products/top-by-revenue.",
            table=ORDERS,
            category=f(ORDERS, "product_name"),
            metric=f(ORDERS, "total_revenue"),
            extra_dims=[f(ORDERS, "product_category")],
            limit=10,
        )
    )

    charts.append(
        pie_chart(
            name="Orders by status",
            description="Order lifecycle distribution. Mirrors /api/orders/by-status.",
            table=ORDERS,
            group=f(ORDERS, "order_status"),
            metric=f(ORDERS, "order_count"),
        )
    )

    charts.append(
        cartesian_chart(
            name="Revenue by category",
            description="Revenue and units per product category. Mirrors /api/products/by-category.",
            table=ORDERS,
            x=f(ORDERS, "product_category"),
            ys=[f(ORDERS, "total_revenue"), f(ORDERS, "units_sold"), f(ORDERS, "order_count")],
            chart_type="bar",
            sorts=[{"fieldId": f(ORDERS, "total_revenue"), "descending": True}],
            limit=50,
        )
    )

    charts.append(
        table_chart(
            name="Region summary",
            description="Per-region metrics: orders, revenue, unique customers, AOV. Mirrors /api/regions/summary as a sortable table.",
            table=ORDERS,
            dimensions=[f(ORDERS, "order_region")],
            metrics=[
                f(ORDERS, "order_count"),
                f(ORDERS, "total_revenue"),
                f(ORDERS, "unique_customers"),
                f(ORDERS, "average_order_value"),
            ],
            sorts=[{"fieldId": f(ORDERS, "total_revenue"), "descending": True}],
            limit=20,
        )
    )

    charts.append(
        table_chart(
            name="Live orders",
            description="Most-recent orders, customer, country, total. Mirrors /api/orders/live, but sortable + groupable.",
            table=ORDERS,
            dimensions=[
                f(ORDERS, "order_created_at"),
                f(ORDERS, "order_id"),
                f(ORDERS, "order_region"),
                f(ORDERS, "customer_country"),
                f(ORDERS, "customer_name"),
                f(ORDERS, "order_status"),
                f(ORDERS, "order_total_eur"),
            ],
            metrics=[],
            sorts=[{"fieldId": f(ORDERS, "order_created_at"), "descending": True}],
            limit=25,
        )
    )

    return charts


# ---------------------------------------------------------------------------
# Dashboard layout (12-column grid, h rows in 60px increments)


def build_dashboard_payload(
    *, name: str, description: str, chart_uuids: dict[str, str]
) -> dict:
    """Maps chart names -> tile placement on a 12-col / 5-row grid."""

    # (chart_name, x, y, w, h)
    layout: list[tuple[str, int, int, int, int]] = [
        # Row 1 — KPI tiles
        ("Total customers", 0, 0, 3, 3),
        ("Total products", 3, 0, 3, 3),
        ("Total orders", 6, 0, 3, 3),
        ("Total revenue", 9, 0, 3, 3),
        # Row 2 — wide timeseries side by side
        ("Orders timeseries (last 30 days)", 0, 3, 6, 5),
        ("Orders by region (timeseries)", 6, 3, 6, 5),
        # Row 3 — regional + status
        ("Revenue by region", 0, 8, 6, 5),
        ("Orders by status", 6, 8, 3, 5),
        ("Revenue by category", 9, 8, 3, 5),
        # Row 4 — top products
        ("Top 10 products by units sold", 0, 13, 6, 5),
        ("Top 10 products by revenue", 6, 13, 6, 5),
        # Row 5 — tables
        ("Region summary", 0, 18, 6, 5),
        ("Live orders", 6, 18, 6, 5),
    ]

    tiles = []
    for chart_name, x, y, w, h in layout:
        uuid = chart_uuids.get(chart_name)
        if not uuid:
            print(f"  ! skipping tile (chart not created): {chart_name}", file=sys.stderr)
            continue
        tiles.append(
            {
                "type": "saved_chart",
                "x": x,
                "y": y,
                "w": w,
                "h": h,
                "properties": {
                    "savedChartUuid": uuid,
                    "belongsToDashboard": False,
                },
            }
        )

    return {
        "name": name,
        "description": description,
        "tiles": tiles,
        "filters": {"dimensions": [], "metrics": [], "tableCalculations": []},
        "tabs": [],
    }


# ---------------------------------------------------------------------------
# Orchestration


def find_project_uuid(client: LightdashClient) -> str:
    projects = client.get("/api/v1/org/projects")
    if not projects:
        raise RuntimeError("No projects in this organization")
    p = projects[0]
    print(f"using project: {p['name']!r} ({p['projectUuid']}) | warehouse={p['warehouseType']}")
    return p["projectUuid"]


def find_default_space_uuid(client: LightdashClient, project_uuid: str) -> str:
    spaces = client.get(f"/api/v1/projects/{project_uuid}/spaces")
    if not spaces:
        raise RuntimeError("No spaces in this project")
    s = spaces[0]
    print(f"using space:   {s['name']!r} ({s['uuid']})")
    return s["uuid"]


def delete_existing_charts_by_name(
    client: LightdashClient, project_uuid: str, names: set[str]
) -> None:
    """Remove every saved chart in the project whose name is one we're about to create."""
    existing = client.get(f"/api/v1/projects/{project_uuid}/spaces")
    seen: set[str] = set()
    for space in existing:
        for q in space.get("queries", []) or []:
            if q["name"] in names and q["uuid"] not in seen:
                print(f"  - deleting existing chart {q['name']!r} ({q['uuid']})")
                client.delete(f"/api/v1/saved/{q['uuid']}")
                seen.add(q["uuid"])


def delete_existing_dashboards_by_name(
    client: LightdashClient, project_uuid: str, name: str
) -> None:
    """Remove every dashboard in the project whose name matches."""
    existing = client.get(f"/api/v1/projects/{project_uuid}/dashboards")
    for d in existing:
        if d["name"] == name:
            print(f"  - deleting existing dashboard {d['name']!r} ({d['uuid']})")
            client.delete(f"/api/v1/dashboards/{d['uuid']}")


def create_chart(
    client: LightdashClient, project_uuid: str, space_uuid: str, payload: dict
) -> str:
    """POST a saved chart and return its uuid."""
    payload = {**payload, "spaceUuid": space_uuid}
    res = client.post(f"/api/v1/projects/{project_uuid}/saved", payload)
    return res["uuid"]


def main() -> None:
    base_url = os.environ.get("LIGHTDASH_URL") or "https://019e0171-79d5-775d-b84a-9b12dcd1ed50-8080.eur-1.aiven.app"
    token = os.environ.get("LIGHTDASH_TOKEN")
    if not base_url or not token:
        sys.exit("set LIGHTDASH_URL and LIGHTDASH_TOKEN env vars")

    client = LightdashClient(base_url, token)
    project_uuid = find_project_uuid(client)
    space_uuid = find_default_space_uuid(client, project_uuid)

    charts = all_chart_payloads()
    chart_names = {c["name"] for c in charts}

    dashboard_name = "Webstore Live Insights"
    dashboard_desc = (
        "Live mirror of the bespoke React webstore dashboard (dashboard-web + "
        "dashboard-api) on top of the same Aiven for ClickHouse gold layer "
        "(default.customers, default.products, default.orders, "
        "default.order_items). Powered by the lightdash-dbt project."
    )

    print("\n[1/3] cleaning up old charts/dashboard with the same names...")
    delete_existing_dashboards_by_name(client, project_uuid, dashboard_name)
    delete_existing_charts_by_name(client, project_uuid, chart_names)

    print(f"\n[2/3] creating {len(charts)} saved charts...")
    chart_uuids: dict[str, str] = {}
    for c in charts:
        try:
            uuid = create_chart(client, project_uuid, space_uuid, c)
            chart_uuids[c["name"]] = uuid
            print(f"  + {c['name']:42s} -> {uuid}")
        except RuntimeError as e:
            print(f"  ! failed to create {c['name']}: {e}", file=sys.stderr)

    print(f"\n[3/3] creating dashboard {dashboard_name!r}...")
    dash_payload = build_dashboard_payload(
        name=dashboard_name,
        description=dashboard_desc,
        chart_uuids=chart_uuids,
    )
    dash_payload["spaceUuid"] = space_uuid
    res = client.post(f"/api/v1/projects/{project_uuid}/dashboards", dash_payload)
    dash_uuid = res["uuid"]
    print(f"  + dashboard uuid: {dash_uuid}")
    print(f"\n[✓] {base_url}/projects/{project_uuid}/dashboards/{dash_uuid}/view")


if __name__ == "__main__":
    main()
