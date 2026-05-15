/**
 * Marketing reference data that lives in PostgreSQL but never flows through CDC.
 *
 * Why this module exists:
 *   The webshop tables in the `public` schema are continuously mutated by
 *   `simulator.ts` and streamed to ClickHouse via Debezium → Kafka → CH Kafka
 *   engine. That's the OLTP→OLAP story, and it's great for high-volume
 *   transactional data. But not every dataset wants that treatment — campaign
 *   metadata is small (~dozens of rows), slowly-changing, and managed by a
 *   marketing team, not by app code.
 *
 *   Instead of routing it through CDC, we keep it in PG and let ClickHouse
 *   read it live over the Aiven PostgreSQL→ClickHouse service integration.
 *   That gives the demo a second integration pattern alongside CDC — federated
 *   queries on slowly-changing reference data — and both pipelines end up
 *   visible in DataHub's lineage graph.
 *
 * This module is responsible for:
 *   - Creating the `marketing` schema and `marketing.campaigns` table.
 *   - Seeding the table on first boot with a curated set of past, active, and
 *     upcoming campaigns across all regions, so dashboards have something
 *     interesting to join against immediately.
 *
 * After the initial seed, the simulator never touches `marketing.campaigns`
 * again. It's a fire-and-forget bootstrap, idempotent via ON CONFLICT DO
 * NOTHING + IF NOT EXISTS.
 */

import { pool } from "./db";

const SCHEMA_STATEMENTS: string[] = [
  `CREATE SCHEMA IF NOT EXISTS marketing`,
  `CREATE TABLE IF NOT EXISTS marketing.campaigns (
     id                  BIGSERIAL PRIMARY KEY,
     name                TEXT NOT NULL UNIQUE,
     region              TEXT NOT NULL,
     country             TEXT,
     channel             TEXT NOT NULL,
     start_date          DATE NOT NULL,
     end_date            DATE NOT NULL,
     discount_pct        NUMERIC(5,2) NOT NULL DEFAULT 0,
     budget_eur          NUMERIC(12,2) NOT NULL DEFAULT 0,
     target_revenue_eur  NUMERIC(12,2) NOT NULL DEFAULT 0,
     status              TEXT NOT NULL DEFAULT 'planned',
     description         TEXT,
     created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
     updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS idx_campaigns_region_window
     ON marketing.campaigns (region, start_date, end_date)`,
  `CREATE INDEX IF NOT EXISTS idx_campaigns_status
     ON marketing.campaigns (status)`,
  // Debezium isn't watching this table — REPLICA IDENTITY left at default.
  // Marketing changes flow to ClickHouse via the PG→CH service integration,
  // not via CDC, so we don't need full before-images.
];

interface CampaignSeed {
  name: string;
  region: string;
  country: string | null;
  channel: string;
  start_offset_days: number; // relative to "now"; negative = past, positive = future
  duration_days: number;
  discount_pct: number;
  budget_eur: number;
  target_revenue_eur: number;
  status: "planned" | "active" | "paused" | "completed";
  description: string;
}

// Curated mix of past, active, and upcoming campaigns spanning all four sales
// regions. Dates are computed at insert-time relative to "today" so the seed
// stays evergreen — the dashboard always has fresh "active" campaigns to show.
const CAMPAIGN_SEED: CampaignSeed[] = [
  // ─── Currently active (started before today, end after today) ─────────────
  {
    name: "EMEA Spring Tech Push",
    region: "EMEA",
    country: null,
    channel: "paid_search",
    start_offset_days: -10,
    duration_days: 30,
    discount_pct: 15,
    budget_eur: 45_000,
    target_revenue_eur: 220_000,
    status: "active",
    description:
      "Two-week paid-search push on consumer electronics across EMEA. Targets returning customers in DE/FR/UK with a 15% discount on laptops and accessories.",
  },
  {
    name: "AMER Q2 Loyalty Push",
    region: "AMER",
    country: null,
    channel: "email",
    start_offset_days: -7,
    duration_days: 21,
    discount_pct: 10,
    budget_eur: 18_000,
    target_revenue_eur: 95_000,
    status: "active",
    description:
      "Lifecycle email series re-engaging customers who haven't ordered in 60+ days. Personalized 10% off site-wide for the top product category they previously bought.",
  },
  {
    name: "APAC Mid-Year Sale",
    region: "APAC",
    country: null,
    channel: "social",
    start_offset_days: -3,
    duration_days: 14,
    discount_pct: 20,
    budget_eur: 32_000,
    target_revenue_eur: 160_000,
    status: "active",
    description:
      "Mid-year sale across APAC with heavy social spend in JP/KR/SG. 20% off Home & Living plus free standard shipping over €50.",
  },
  {
    name: "LATAM Carnival Weeks",
    region: "LATAM",
    country: "BR",
    channel: "social",
    start_offset_days: -5,
    duration_days: 20,
    discount_pct: 25,
    budget_eur: 14_000,
    target_revenue_eur: 60_000,
    status: "active",
    description:
      "Brazil-focused Carnival campaign. Aggressive 25% discount with influencer partnerships on Instagram and TikTok.",
  },
  {
    name: "Global Flagship Anniversary",
    region: "GLOBAL",
    country: null,
    channel: "partner",
    start_offset_days: -1,
    duration_days: 7,
    discount_pct: 30,
    budget_eur: 80_000,
    target_revenue_eur: 500_000,
    status: "active",
    description:
      "Seven-day brand anniversary event across every region. Flagship products discounted 30%, headline placement with retail partners.",
  },

  // ─── Recently completed (good for retrospective dashboards) ───────────────
  {
    name: "EMEA Black Friday 2025",
    region: "EMEA",
    country: null,
    channel: "paid_search",
    start_offset_days: -180,
    duration_days: 5,
    discount_pct: 35,
    budget_eur: 120_000,
    target_revenue_eur: 850_000,
    status: "completed",
    description:
      "Five-day Black Friday blitz across EMEA. Heaviest spend of the year on paid search, retargeting, and social.",
  },
  {
    name: "AMER Cyber Monday 2025",
    region: "AMER",
    country: "US",
    channel: "email",
    start_offset_days: -175,
    duration_days: 1,
    discount_pct: 40,
    budget_eur: 40_000,
    target_revenue_eur: 320_000,
    status: "completed",
    description:
      "Single-day Cyber Monday email + push notification campaign with the deepest discount of the year on tech accessories.",
  },
  {
    name: "APAC Lunar New Year",
    region: "APAC",
    country: null,
    channel: "social",
    start_offset_days: -90,
    duration_days: 14,
    discount_pct: 18,
    budget_eur: 28_000,
    target_revenue_eur: 140_000,
    status: "completed",
    description:
      "Lunar New Year campaign with red-envelope themed creative. Strong performance in CN/HK/TW.",
  },
  {
    name: "EMEA Winter Sale 2025",
    region: "EMEA",
    country: null,
    channel: "email",
    start_offset_days: -60,
    duration_days: 30,
    discount_pct: 22,
    budget_eur: 35_000,
    target_revenue_eur: 180_000,
    status: "completed",
    description:
      "Post-holiday inventory clearance across EMEA. End-of-season fashion and home goods at 22% off.",
  },
  {
    name: "AMER Back-to-School 2025",
    region: "AMER",
    country: null,
    channel: "paid_search",
    start_offset_days: -270,
    duration_days: 21,
    discount_pct: 12,
    budget_eur: 25_000,
    target_revenue_eur: 110_000,
    status: "completed",
    description:
      "Back-to-school season targeting parents in US/CA with student-relevant categories: backpacks, laptops, headphones.",
  },

  // ─── Upcoming / planned (fills the funnel for "what's next" dashboards) ──
  {
    name: "EMEA Summer Sale 2026",
    region: "EMEA",
    country: null,
    channel: "paid_search",
    start_offset_days: 21,
    duration_days: 21,
    discount_pct: 18,
    budget_eur: 55_000,
    target_revenue_eur: 260_000,
    status: "planned",
    description:
      "Three-week summer sale across EMEA. Heavy seasonal merchandising on outdoor, travel, and apparel categories.",
  },
  {
    name: "APAC Singles Day 2026",
    region: "APAC",
    country: null,
    channel: "social",
    start_offset_days: 90,
    duration_days: 3,
    discount_pct: 45,
    budget_eur: 65_000,
    target_revenue_eur: 420_000,
    status: "planned",
    description:
      "Three-day Singles Day mega-sale. Highest single-day revenue target of the year for the APAC region.",
  },
  {
    name: "Global Holiday 2026",
    region: "GLOBAL",
    country: null,
    channel: "partner",
    start_offset_days: 180,
    duration_days: 28,
    discount_pct: 25,
    budget_eur: 150_000,
    target_revenue_eur: 1_200_000,
    status: "planned",
    description:
      "Month-long holiday campaign across every region. Phased rollout: gift guides, doorbusters, last-minute shipping.",
  },
  {
    name: "AMER Memorial Day",
    region: "AMER",
    country: "US",
    channel: "email",
    start_offset_days: 14,
    duration_days: 4,
    discount_pct: 20,
    budget_eur: 18_000,
    target_revenue_eur: 88_000,
    status: "planned",
    description:
      "Long-weekend Memorial Day sale. Email-led with a banner takeover on the homepage. Heavy on apparel and home goods.",
  },
  {
    name: "LATAM Mother's Day Brazil",
    region: "LATAM",
    country: "BR",
    channel: "social",
    start_offset_days: 7,
    duration_days: 10,
    discount_pct: 15,
    budget_eur: 8_000,
    target_revenue_eur: 38_000,
    status: "planned",
    description:
      "Mother's Day campaign in Brazil. Gifting categories: beauty, jewelry, home decor.",
  },
  {
    name: "EMEA UK-only Royal Anniversary",
    region: "EMEA",
    country: "GB",
    channel: "social",
    start_offset_days: 45,
    duration_days: 5,
    discount_pct: 10,
    budget_eur: 7_500,
    target_revenue_eur: 32_000,
    status: "planned",
    description:
      "UK-only short-burst campaign tied to a national event. Modest discount, lifestyle-led creative.",
  },
];

const INSERT_SQL = `
  INSERT INTO marketing.campaigns
    (name, region, country, channel, start_date, end_date,
     discount_pct, budget_eur, target_revenue_eur, status, description)
  VALUES
    ($1, $2, $3, $4, current_date + ($5 || ' days')::interval,
                     current_date + (($5 + $6) || ' days')::interval,
     $7, $8, $9, $10, $11)
  ON CONFLICT (name) DO NOTHING
  RETURNING id
`;

export async function ensureMarketing(): Promise<void> {
  const client = await pool.connect();
  try {
    for (const sql of SCHEMA_STATEMENTS) {
      await client.query(sql);
    }
    let inserted = 0;
    for (const c of CAMPAIGN_SEED) {
      const r = await client.query<{ id: string }>(INSERT_SQL, [
        c.name,
        c.region,
        c.country,
        c.channel,
        c.start_offset_days,
        c.duration_days,
        c.discount_pct,
        c.budget_eur,
        c.target_revenue_eur,
        c.status,
        c.description,
      ]);
      if (r.rowCount && r.rowCount > 0) inserted += 1;
    }
    if (inserted > 0) {
      console.log(`[marketing] seeded ${inserted} new campaign(s)`);
    } else {
      console.log("[marketing] campaigns table already seeded");
    }
  } finally {
    client.release();
  }
}
