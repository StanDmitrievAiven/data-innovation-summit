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
    // Mirror of the orders_with_campaign dbt model in lightdash-dbt. Keep
    // the two in sync — same join shape, same projected columns.
    //
    // ClickHouse needs at least one equality predicate to pick join keys,
    // so we can't write `LIKE … OR region='GLOBAL'` directly. The
    // campaigns CTE explodes each row to one (campaign, match_region)
    // tuple per region it targets (GLOBAL fans out to all four), then we
    // equi-join the order's region prefix (EMEA-FR → EMEA) against
    // match_region. LEFT ANY JOIN keeps grain at one row per order line
    // even when several campaigns overlap.
    name: "default.orders_with_campaign",
    sql: `
      CREATE OR REPLACE VIEW default.orders_with_campaign AS
      WITH campaigns AS (
          SELECT
              id                              AS campaign_id,
              name                            AS campaign_name,
              channel                         AS campaign_channel,
              region                          AS campaign_region,
              country                         AS campaign_country,
              toFloat64(discount_pct)         AS campaign_discount_pct,
              toFloat64(budget_eur)           AS campaign_budget_eur,
              toFloat64(target_revenue_eur)   AS campaign_target_revenue_eur,
              status                          AS campaign_status,
              start_date                      AS campaign_start_date,
              end_date                        AS campaign_end_date,
              description                     AS campaign_description,
              arrayJoin(
                  if(region = 'GLOBAL', ['EMEA', 'AMER', 'APAC', 'LATAM'], [region])
              )                               AS match_region
          FROM \`${PG_FEDERATED_DB}\`.campaigns
      ),
      enriched AS (
          SELECT
              e.*,
              splitByChar('-', e.order_region)[1] AS order_region_prefix
          FROM default.orders_enriched AS e
      )
      SELECT
          e.* EXCEPT (order_region_prefix),
          c.campaign_id,
          c.campaign_name,
          c.campaign_channel,
          c.campaign_region,
          c.campaign_country,
          c.campaign_discount_pct,
          c.campaign_budget_eur,
          c.campaign_target_revenue_eur,
          c.campaign_status,
          c.campaign_start_date,
          c.campaign_end_date,
          c.campaign_description,
          (c.campaign_id IS NOT NULL) AS in_campaign
      FROM enriched AS e
      LEFT ANY JOIN campaigns AS c
        ON e.order_region_prefix = c.match_region
       AND e.order_created_date BETWEEN c.campaign_start_date AND c.campaign_end_date
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
