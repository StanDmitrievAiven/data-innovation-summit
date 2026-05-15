#!/usr/bin/env bash
# Registers derived analytics views in DataHub with schemas, upstream lineage,
# and column-level lineage mappings.
#
# These simulate what dbt / Airflow / Spark would produce in a real deployment.
# Those tools also speak OpenLineage and report to DataHub automatically.
set -euo pipefail

DATAHUB_GMS_URL="${DATAHUB_GMS_URL:-http://localhost:8080}"

KAFKA_NS="urn:li:dataPlatform:kafka"
PG_NS="urn:li:dataPlatform:postgres"

# Schema field type shorthands
NT='{"type":{"com.linkedin.schema.NumberType":{}}}'
ST='{"type":{"com.linkedin.schema.StringType":{}}}'
BT='{"type":{"com.linkedin.schema.BooleanType":{}}}'
TT='{"type":{"com.linkedin.schema.TimeType":{}}}'

sf() { echo "urn:li:schemaField:(urn:li:dataset:(${1},${2},PROD),${3})"; }

ingest_dataset() {
  local URN="$1" NAME="$2" DESC="$3"
  curl -s -X POST "${DATAHUB_GMS_URL}/entities?action=ingest" \
    -H 'Content-Type: application/json' \
    -H 'X-RestLi-Protocol-Version: 2.0.0' \
    -d "{\"entity\":{\"value\":{\"com.linkedin.metadata.snapshot.DatasetSnapshot\":{\"urn\":\"${URN}\",\"aspects\":[{\"com.linkedin.common.Status\":{\"removed\":false}},{\"com.linkedin.dataset.DatasetProperties\":{\"name\":\"${NAME}\",\"description\":\"${DESC}\",\"qualifiedName\":\"${NAME}\"}}]}}}}" > /dev/null 2>&1
}

ingest_schema() {
  local URN="$1" FIELDS="$2"
  curl -s -X POST "${DATAHUB_GMS_URL}/entities?action=ingest" \
    -H 'Content-Type: application/json' \
    -H 'X-RestLi-Protocol-Version: 2.0.0' \
    -d "{\"entity\":{\"value\":{\"com.linkedin.metadata.snapshot.DatasetSnapshot\":{\"urn\":\"${URN}\",\"aspects\":[{\"com.linkedin.schema.SchemaMetadata\":{\"schemaName\":\"derived\",\"platform\":\"urn:li:dataPlatform:postgres\",\"version\":0,\"hash\":\"v1\",\"platformSchema\":{\"com.linkedin.schema.OtherSchema\":{\"rawSchema\":\"CREATE VIEW\"}},\"fields\":${FIELDS}}}]}}}}" > /dev/null 2>&1
}

ingest_lineage() {
  local URN="$1" UPSTREAMS="$2" FGL="$3"
  local ASPECT_JSON="{\"upstreams\":${UPSTREAMS},\"fineGrainedLineages\":${FGL}}"
  local ESCAPED
  ESCAPED=$(echo "$ASPECT_JSON" | jq -c . | jq -Rs .)
  curl -s -X POST "${DATAHUB_GMS_URL}/aspects?action=ingestProposal" \
    -H 'Content-Type: application/json' \
    -d "{\"proposal\":{\"entityType\":\"dataset\",\"entityUrn\":\"${URN}\",\"changeType\":\"UPSERT\",\"aspectName\":\"upstreamLineage\",\"aspect\":{\"contentType\":\"application/json\",\"value\":${ESCAPED}}}}" > /dev/null 2>&1
}

# Kafka topic URNs
KU="urn:li:dataset:(${KAFKA_NS},ecommerce.public.users,PROD)"
KP="urn:li:dataset:(${KAFKA_NS},ecommerce.public.products,PROD)"
KO="urn:li:dataset:(${KAFKA_NS},ecommerce.public.orders,PROD)"
KOI="urn:li:dataset:(${KAFKA_NS},ecommerce.public.order_items,PROD)"
KS="urn:li:dataset:(${KAFKA_NS},ecommerce.public.shipments,PROD)"

# ── customer_360 ──────────────────────────────────────────────────
C360="urn:li:dataset:(${PG_NS},analytics.customer_360,PROD)"
echo "  analytics.customer_360"
ingest_dataset "${C360}" "analytics.customer_360" "360-degree customer view joining users, orders, and order history"
ingest_schema "${C360}" "[
  {\"fieldPath\":\"user_id\",\"nativeDataType\":\"INT32\",\"type\":${NT}},
  {\"fieldPath\":\"first_name\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"last_name\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"email\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"country\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"is_active\",\"nativeDataType\":\"BOOLEAN\",\"type\":${BT}},
  {\"fieldPath\":\"total_orders\",\"nativeDataType\":\"INT64\",\"type\":${NT}},
  {\"fieldPath\":\"total_spent\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"avg_order_value\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"total_items\",\"nativeDataType\":\"INT64\",\"type\":${NT}}
]"
ingest_lineage "${C360}" \
  "[{\"dataset\":\"${KU}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${KO}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${KOI}\",\"type\":\"TRANSFORMED\"}]" \
  "[
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users user_id)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 user_id)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users first_name)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 first_name)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users last_name)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 last_name)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users email)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 email)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users country)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 country)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users is_active)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 is_active)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders order_id)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 total_orders)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders total_amount)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 total_spent)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders total_amount)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 avg_order_value)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.order_items quantity)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.customer_360 total_items)\"]}" \
  "]"

# ── product_performance ───────────────────────────────────────────
PP="urn:li:dataset:(${PG_NS},analytics.product_performance,PROD)"
echo "  analytics.product_performance"
ingest_dataset "${PP}" "analytics.product_performance" "Product performance metrics combining catalog and sales data"
ingest_schema "${PP}" "[
  {\"fieldPath\":\"product_id\",\"nativeDataType\":\"INT32\",\"type\":${NT}},
  {\"fieldPath\":\"sku\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"name\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"category\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"brand\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"unit_price\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"total_units_sold\",\"nativeDataType\":\"INT64\",\"type\":${NT}},
  {\"fieldPath\":\"total_revenue\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}}
]"
ingest_lineage "${PP}" \
  "[{\"dataset\":\"${KP}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${KOI}\",\"type\":\"TRANSFORMED\"}]" \
  "[
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.products product_id)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance product_id)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.products sku)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance sku)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.products name)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance name)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.products category)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance category)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.products brand)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance brand)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.products unit_price)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance unit_price)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.order_items quantity)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance total_units_sold)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.order_items unit_price)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.product_performance total_revenue)\"]}" \
  "]"

# ── fulfillment_metrics ───────────────────────────────────────────
FM="urn:li:dataset:(${PG_NS},analytics.fulfillment_metrics,PROD)"
echo "  analytics.fulfillment_metrics"
ingest_dataset "${FM}" "analytics.fulfillment_metrics" "Shipping and fulfillment KPIs from orders and shipment tracking"
ingest_schema "${FM}" "[
  {\"fieldPath\":\"order_id\",\"nativeDataType\":\"INT32\",\"type\":${NT}},
  {\"fieldPath\":\"order_status\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"shipping_country\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"carrier\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"tracking_number\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"shipped_at\",\"nativeDataType\":\"TIMESTAMP\",\"type\":${TT}},
  {\"fieldPath\":\"delivered_at\",\"nativeDataType\":\"TIMESTAMP\",\"type\":${TT}},
  {\"fieldPath\":\"delivery_days\",\"nativeDataType\":\"INT32\",\"type\":${NT}}
]"
ingest_lineage "${FM}" \
  "[{\"dataset\":\"${KO}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${KS}\",\"type\":\"TRANSFORMED\"}]" \
  "[
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders order_id)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics order_id)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders status)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics order_status)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders shipping_country)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics shipping_country)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.shipments carrier)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics carrier)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.shipments tracking_number)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics tracking_number)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.shipments shipped_at)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics shipped_at)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.shipments delivered_at)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics delivered_at)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.shipments shipped_at)\",\"$(sf ${KAFKA_NS} ecommerce.public.shipments delivered_at)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics delivery_days)\"]}" \
  "]"

# ── revenue_summary ───────────────────────────────────────────────
RS="urn:li:dataset:(${PG_NS},reporting.revenue_summary,PROD)"
echo "  reporting.revenue_summary"
ingest_dataset "${RS}" "reporting.revenue_summary" "Revenue report aggregating user spend and order totals"
ingest_schema "${RS}" "[
  {\"fieldPath\":\"country\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"total_customers\",\"nativeDataType\":\"INT64\",\"type\":${NT}},
  {\"fieldPath\":\"total_orders\",\"nativeDataType\":\"INT64\",\"type\":${NT}},
  {\"fieldPath\":\"total_revenue\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"avg_order_value\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}}
]"
ingest_lineage "${RS}" \
  "[{\"dataset\":\"${KU}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${KO}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${KOI}\",\"type\":\"TRANSFORMED\"}]" \
  "[
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users country)\"],\"downstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary country)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.users user_id)\"],\"downstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary total_customers)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders order_id)\"],\"downstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary total_orders)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders total_amount)\"],\"downstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary total_revenue)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${KAFKA_NS} ecommerce.public.orders total_amount)\"],\"downstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary avg_order_value)\"]}" \
  "]"

# ── executive_dashboard ───────────────────────────────────────────
ED="urn:li:dataset:(${PG_NS},analytics.executive_dashboard,PROD)"
echo "  analytics.executive_dashboard"
ingest_dataset "${ED}" "analytics.executive_dashboard" "Executive KPI dashboard aggregating all analytics views"
ingest_schema "${ED}" "[
  {\"fieldPath\":\"total_customers\",\"nativeDataType\":\"INT64\",\"type\":${NT}},
  {\"fieldPath\":\"total_revenue\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"avg_order_value\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"top_selling_category\",\"nativeDataType\":\"VARCHAR\",\"type\":${ST}},
  {\"fieldPath\":\"avg_delivery_days\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}},
  {\"fieldPath\":\"fulfillment_rate\",\"nativeDataType\":\"DECIMAL\",\"type\":${NT}}
]"
ingest_lineage "${ED}" \
  "[{\"dataset\":\"${C360}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${PP}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${FM}\",\"type\":\"TRANSFORMED\"},{\"dataset\":\"${RS}\",\"type\":\"TRANSFORMED\"}]" \
  "[
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${PG_NS} analytics.customer_360 user_id)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.executive_dashboard total_customers)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary total_revenue)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.executive_dashboard total_revenue)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"IDENTITY\",\"upstreams\":[\"$(sf ${PG_NS} reporting.revenue_summary avg_order_value)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.executive_dashboard avg_order_value)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${PG_NS} analytics.product_performance category)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.executive_dashboard top_selling_category)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics delivery_days)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.executive_dashboard avg_delivery_days)\"]},
    {\"upstreamType\":\"FIELD_SET\",\"downstreamType\":\"FIELD_SET\",\"transformOperation\":\"TRANSFORM\",\"upstreams\":[\"$(sf ${PG_NS} analytics.fulfillment_metrics delivered_at)\"],\"downstreams\":[\"$(sf ${PG_NS} analytics.executive_dashboard fulfillment_rate)\"]}" \
  "]"
