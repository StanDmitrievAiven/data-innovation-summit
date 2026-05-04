// All queries use ClickHouse parameterised binding ({name:Type}) — no string concat.
// Region filtering is "starts-with" (e.g. region="EMEA" matches every EMEA-* region).
// Pass region="" to disable region filtering.

export const TOP_PRODUCTS_BY_UNITS = `
  SELECT
      p.name        AS product,
      p.category    AS category,
      sum(oi.quantity)                            AS units_sold,
      round(sum(oi.quantity * oi.unit_price), 2)  AS revenue_eur
  FROM default.orders      AS o  FINAL
  JOIN default.order_items AS oi FINAL ON oi.order_id = o.id
  JOIN default.products    AS p  FINAL ON p.id       = oi.product_id
  WHERE o.created_at >= now() - INTERVAL {days:UInt32} DAY
    AND ({region:String} = '' OR o.region LIKE concat({region:String}, '%'))
    AND o.__deleted = 'false'
  GROUP BY product, category
  ORDER BY units_sold DESC
  LIMIT {limit:UInt32}
`;

export const TOP_PRODUCTS_BY_REVENUE = `
  SELECT
      p.name        AS product,
      p.category    AS category,
      sum(oi.quantity)                            AS units_sold,
      round(sum(oi.quantity * oi.unit_price), 2)  AS revenue_eur
  FROM default.orders      AS o  FINAL
  JOIN default.order_items AS oi FINAL ON oi.order_id = o.id
  JOIN default.products    AS p  FINAL ON p.id       = oi.product_id
  WHERE o.created_at >= now() - INTERVAL {days:UInt32} DAY
    AND ({region:String} = '' OR o.region LIKE concat({region:String}, '%'))
    AND o.__deleted = 'false'
  GROUP BY product, category
  ORDER BY revenue_eur DESC
  LIMIT {limit:UInt32}
`;

export const ORDERS_TIMESERIES = `
  SELECT
      toStartOfInterval(created_at, INTERVAL {bucket_seconds:UInt32} SECOND) AS bucket_at,
      count()                       AS orders,
      round(sum(total), 2)          AS revenue_eur
  FROM default.orders FINAL
  WHERE created_at >= now() - INTERVAL {hours:UInt32} HOUR
    AND ({region:String} = '' OR region LIKE concat({region:String}, '%'))
    AND __deleted = 'false'
  GROUP BY bucket_at
  ORDER BY bucket_at ASC
`;

export const ORDERS_TIMESERIES_BY_REGION = `
  SELECT
      toStartOfInterval(created_at, INTERVAL {bucket_seconds:UInt32} SECOND) AS bucket_at,
      region,
      count()                       AS orders,
      round(sum(total), 2)          AS revenue_eur
  FROM default.orders FINAL
  WHERE created_at >= now() - INTERVAL {hours:UInt32} HOUR
    AND __deleted = 'false'
  GROUP BY bucket_at, region
  ORDER BY bucket_at ASC, region ASC
`;

export const REGIONS_SUMMARY = `
  SELECT
      region,
      count()                                  AS orders,
      round(sum(total), 2)                     AS revenue_eur,
      uniqExact(customer_id)                   AS unique_customers,
      round(avg(total), 2)                     AS avg_order_value
  FROM default.orders FINAL
  WHERE created_at >= now() - INTERVAL {hours:UInt32} HOUR
    AND __deleted = 'false'
  GROUP BY region
  ORDER BY orders DESC
`;

export const LIVE_ORDERS = `
  SELECT
      o.id          AS order_id,
      o.region,
      o.status,
      o.total,
      o.created_at,
      c.name        AS customer_name,
      c.country
  FROM default.orders    AS o FINAL
  LEFT JOIN default.customers AS c FINAL ON c.id = o.customer_id
  WHERE o.__deleted = 'false'
  ORDER BY o.created_at DESC
  LIMIT {limit:UInt32}
`;

export const TOTALS = `
  SELECT
    (SELECT count() FROM default.customers   FINAL WHERE __deleted = 'false') AS customers,
    (SELECT count() FROM default.products    FINAL WHERE __deleted = 'false') AS products,
    (SELECT count() FROM default.orders      FINAL WHERE __deleted = 'false') AS orders,
    (SELECT count() FROM default.order_items FINAL WHERE __deleted = 'false') AS order_items
`;

export const STATUS_BREAKDOWN = `
  SELECT
      status,
      count() AS orders
  FROM default.orders FINAL
  WHERE created_at >= now() - INTERVAL {hours:UInt32} HOUR
    AND __deleted = 'false'
  GROUP BY status
  ORDER BY orders DESC
`;

export const CATEGORY_REVENUE = `
  SELECT
      p.category    AS category,
      count(DISTINCT o.id)                        AS orders,
      sum(oi.quantity)                            AS units_sold,
      round(sum(oi.quantity * oi.unit_price), 2)  AS revenue_eur
  FROM default.orders      AS o  FINAL
  JOIN default.order_items AS oi FINAL ON oi.order_id = o.id
  JOIN default.products    AS p  FINAL ON p.id       = oi.product_id
  WHERE o.created_at >= now() - INTERVAL {days:UInt32} DAY
    AND ({region:String} = '' OR o.region LIKE concat({region:String}, '%'))
    AND o.__deleted = 'false'
  GROUP BY category
  ORDER BY revenue_eur DESC
`;
