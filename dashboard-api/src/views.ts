/**
 * Warehouse view bootstrap.
 *
 * dashboard-api is the only piece of the demo that sits inside the project
 * VPC, holds ClickHouse credentials, and has its own startup hook. That
 * makes it the natural place to materialise the derived ClickHouse views
 * the dashboard depends on — including the `orders_with_campaign` view that
 * joins our CDC-fed `orders_enriched` view against the federated
 * PostgreSQL→ClickHouse `marketing.campaigns` table.
 *
 * The dbt project (lightdash-dbt) stays the source of truth for these view
 * definitions — that's what DataHub ingests for column-level lineage. This
 * file mirrors those definitions verbatim so the warehouse stays in sync
 * with the dbt manifest on every redeploy. Whenever a model changes in
 * lightdash-dbt, copy the SQL here, push, redeploy — and the live view
 * follows.
 *
 * Why mirror instead of compile dbt at runtime? Two reasons:
 *   1. We don't want the app container to ship a full dbt + Python toolchain
 *      just to run CREATE OR REPLACE VIEW. The SQL is short and stable.
 *   2. Going through dbt here would require credentials and lockstep with
 *      the lightdash-dbt repo layout; mirroring keeps the runtime path
 *      simple and auditable.
 *
 * All statements run via `CREATE OR REPLACE VIEW` so the boot path is fully
 * idempotent. If the federated PG→CH integration database isn't ready yet
 * (the integration provisions a new PG node and that can take a few minutes
 * after creation), the view DDL still succeeds because CH only resolves the
 * upstream PostgreSQL table at query time, not at view creation.
 */

import { ch } from "./db";

/**
 * The federated database that Aiven's PostgreSQL→ClickHouse service
 * integration exposes. Aiven names it
 * `service_<pg-service>_<pg-database>_<pg-schema>`; for this demo that
 * resolves to the constant below.
 *
 * Reference: https://aiven.io/docs/products/clickhouse/howto/integrate-postgresql
 */
const PG_FEDERATED_DB = "service_pg-37c7de3b_defaultdb_marketing";

interface ViewDef {
  name: string;
  sql: string;
}

const VIEWS: ViewDef[] = [
  {
    name: "default.orders_with_campaign",
    sql: `
      CREATE OR REPLACE VIEW default.orders_with_campaign AS
      SELECT
          o.id                                       AS order_id,
          o.created_at                               AS order_created_at,
          o.region                                   AS order_region,
          o.status                                   AS order_status,
          o.total                                    AS order_total,
          o.customer_id                              AS customer_id,
          c.id                                       AS campaign_id,
          c.name                                     AS campaign_name,
          c.channel                                  AS campaign_channel,
          c.region                                   AS campaign_region,
          c.country                                  AS campaign_country,
          c.discount_pct                             AS campaign_discount_pct,
          c.budget_eur                               AS campaign_budget_eur,
          c.target_revenue_eur                       AS campaign_target_revenue_eur,
          c.status                                   AS campaign_status,
          c.start_date                               AS campaign_start_date,
          c.end_date                                 AS campaign_end_date,
          (c.id IS NOT NULL)                         AS in_campaign
      FROM default.orders_enriched AS o
      LEFT JOIN \`${PG_FEDERATED_DB}\`.campaigns AS c
        ON (
              -- Region-targeted campaign (e.g. EMEA, AMER) — match the region
              -- prefix that orders carry (e.g. EMEA-FR matches an EMEA campaign).
              o.region LIKE concat(c.region, '%')
              -- ...or a GLOBAL campaign matches every order regardless of region.
              OR c.region = 'GLOBAL'
           )
       AND toDate(o.created_at) BETWEEN c.start_date AND c.end_date
    `.trim(),
  },
];

export async function ensureWarehouseViews(): Promise<void> {
  for (const v of VIEWS) {
    try {
      await ch.exec({ query: v.sql });
      console.log(`[views] ensured ${v.name}`);
    } catch (err) {
      // We never want a failed view create to keep the API from booting —
      // the dashboard's other endpoints still work fine without the new
      // view, and the underlying issue (federated DB not ready, ddl typo)
      // is surfaced in logs for a follow-up.
      console.error(
        `[views] failed to ensure ${v.name}:`,
        (err as Error).message,
      );
    }
  }
}
