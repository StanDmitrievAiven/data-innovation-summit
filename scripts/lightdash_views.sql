-- ===== default.orders_enriched =====
CREATE OR REPLACE VIEW default.orders_enriched AS (
-- Wide order-line fact built from the four CDC gold tables. One row per order line,
-- joined with its order header, the product, and the customer.
--
-- FINAL is required because every source is a ReplacingMergeTree on `id`, and CDC
-- updates produce multiple parts that need merging at read time. The __deleted='false'
-- filter strips Debezium tombstones (rows that have been DELETEd upstream in postgres).
--
-- The CH gold tables already store decimals as Decimal(10,2) (the Kafka
-- materialised views cast them on the way in). We promote to Float64 here
-- so Lightdash treats the columns as numbers everywhere it does sum/avg/
-- arithmetic. toFloat64 (not toFloat64OrNull, which only takes String).

with order_items as (
    select
        id,
        order_id,
        product_id,
        quantity,
        toFloat64(unit_price) as unit_price_eur
    from default.order_items final
    where __deleted = 'false'
),

orders as (
    select
        id,
        customer_id,
        status,
        toFloat64(total) as order_total_eur,
        region,
        parseDateTimeBestEffortOrNull(created_at) as created_at_ts
    from default.orders final
    where __deleted = 'false'
),

products as (
    select
        id,
        sku,
        name,
        category,
        toFloat64(price) as list_price_eur
    from default.products final
    where __deleted = 'false'
),

customers as (
    select
        id,
        email,
        name,
        region,
        country
    from default.customers final
    where __deleted = 'false'
)

select
    -- order-item grain
    oi.id                                  as order_item_id,
    oi.order_id                            as order_id,
    oi.product_id                          as product_id,
    oi.quantity                            as quantity,
    oi.unit_price_eur                      as unit_price_eur,
    oi.unit_price_eur * oi.quantity        as line_total_eur,

    -- order header
    o.customer_id                          as customer_id,
    o.status                               as order_status,
    o.region                               as order_region,
    o.order_total_eur                      as order_total_eur,
    o.created_at_ts                        as order_created_at,
    toDate(o.created_at_ts)                as order_created_date,

    -- product
    p.sku                                  as product_sku,
    p.name                                 as product_name,
    p.category                             as product_category,
    p.list_price_eur                       as product_list_price_eur,

    -- customer
    c.email                                as customer_email,
    c.name                                 as customer_name,
    c.region                               as customer_region,
    c.country                              as customer_country
from order_items as oi
left join orders    as o on o.id = oi.order_id
left join products  as p on p.id = oi.product_id
left join customers as c on c.id = o.customer_id
);

-- ===== default.customers_clean =====
CREATE OR REPLACE VIEW default.customers_clean AS (
-- Clean customer dimension: latest version of every active customer, joined with
-- their lifetime order metrics. Useful as a standalone Lightdash explore for
-- "top spenders", churn analysis, regional breakdowns, etc.

with customers as (
    select
        id,
        email,
        name,
        region,
        country,
        parseDateTimeBestEffortOrNull(created_at) as account_created_at
    from default.customers final
    where __deleted = 'false'
),

order_rollup as (
    select
        customer_id,
        count() as lifetime_orders,
        sum(toFloat64(total)) as lifetime_revenue_eur,
        min(parseDateTimeBestEffortOrNull(created_at)) as first_order_at,
        max(parseDateTimeBestEffortOrNull(created_at)) as latest_order_at
    from default.orders final
    where __deleted = 'false'
    group by customer_id
)

select
    c.id                                              as customer_id,
    c.email                                           as customer_email,
    c.name                                            as customer_name,
    c.region                                          as customer_region,
    c.country                                         as customer_country,
    c.account_created_at                              as account_created_at,
    toDate(c.account_created_at)                      as account_created_date,
    coalesce(r.lifetime_orders,       0)              as lifetime_orders,
    coalesce(r.lifetime_revenue_eur,  0.0)            as lifetime_revenue_eur,
    r.first_order_at                                  as first_order_at,
    r.latest_order_at                                 as latest_order_at,
    if(r.lifetime_orders > 0, 1, 0)                   as has_ordered
from customers as c
left join order_rollup as r on r.customer_id = c.id
);

-- ===== default.products_catalog =====
CREATE OR REPLACE VIEW default.products_catalog AS (
-- Product dimension joined with lifetime sell-through metrics.
-- Use this explore for "top sellers by category", "inventory vs sold", "price band analysis".

with products as (
    select
        id,
        sku,
        name,
        category,
        toFloat64(price) as list_price_eur,
        inventory
    from default.products final
    where __deleted = 'false'
),

sales_rollup as (
    select
        oi.product_id                                                  as product_id,
        sum(oi.quantity)                                               as units_sold,
        sum(oi.quantity * toFloat64(oi.unit_price))                    as gross_revenue_eur,
        count()                                                        as line_count
    from default.order_items as oi final
    where oi.__deleted = 'false'
    group by oi.product_id
)

select
    p.id                                              as product_id,
    p.sku                                             as product_sku,
    p.name                                            as product_name,
    p.category                                        as product_category,
    p.list_price_eur                                  as list_price_eur,
    p.inventory                                       as inventory_units,
    coalesce(s.units_sold,        0)                  as units_sold,
    coalesce(s.gross_revenue_eur, 0.0)                as gross_revenue_eur,
    coalesce(s.line_count,        0)                  as line_count
from products as p
left join sales_rollup as s on s.product_id = p.id
);
