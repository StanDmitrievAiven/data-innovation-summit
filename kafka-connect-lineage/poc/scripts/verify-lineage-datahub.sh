#!/usr/bin/env bash
set -euo pipefail

DATAHUB_GMS_URL="${DATAHUB_GMS_URL:-http://localhost:8080}"

echo "============================================"
echo "  DataHub Lineage Verification"
echo "============================================"
echo ""

# Helper: run a GraphQL query against DataHub GMS
graphql_query() {
  local query="$1"
  curl -s "${DATAHUB_GMS_URL}/api/graphql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\"}"
}

# ── 1. Check OpenLineage events endpoint ─────────────────────────────
echo "1. Checking OpenLineage events endpoint..."
OL_EVENTS=$(curl -s "${DATAHUB_GMS_URL}/openapi/openlineage/api/v2/lineage/events" 2>/dev/null || echo "{}")
if echo "$OL_EVENTS" | jq -e '.' > /dev/null 2>&1; then
  EVENT_COUNT=$(echo "$OL_EVENTS" | jq 'if type == "array" then length else 0 end' 2>/dev/null || echo "0")
  echo "  OpenLineage events endpoint returned data (count: ${EVENT_COUNT})"
else
  echo "  OpenLineage events endpoint not available or returned non-JSON"
fi
echo ""

# ── 2. Search for datasets containing "inventory" ────────────────────
echo "2. Searching for datasets matching 'inventory'..."
SEARCH_RESULT=$(graphql_query "{ search(input: {type: DATASET, query: \\\"inventory\\\", start: 0, count: 20}) { total searchResults { entity { urn ... on Dataset { name properties { qualifiedName } } } } } }")

SEARCH_TOTAL=$(echo "$SEARCH_RESULT" | jq '.data.search.total // 0' 2>/dev/null)
echo "  Found ${SEARCH_TOTAL} datasets matching 'inventory'"
if [ "${SEARCH_TOTAL:-0}" -gt 0 ]; then
  echo "  Datasets:"
  echo "$SEARCH_RESULT" | jq -r '.data.search.searchResults[]? | "    - \(.entity.urn) (\(.entity.name // "unnamed"))"' 2>/dev/null
fi
echo ""

# ── 3. Search for datasets containing "customers" ────────────────────
echo "3. Searching for datasets matching 'customers'..."
CUSTOMERS_RESULT=$(graphql_query "{ search(input: {type: DATASET, query: \\\"customers\\\", start: 0, count: 10}) { total searchResults { entity { urn ... on Dataset { name properties { qualifiedName } schemaMetadata { fields { fieldPath nativeDataType } } } } } } }")

CUSTOMERS_TOTAL=$(echo "$CUSTOMERS_RESULT" | jq '.data.search.total // 0' 2>/dev/null)
echo "  Found ${CUSTOMERS_TOTAL} datasets matching 'customers'"
if [ "${CUSTOMERS_TOTAL:-0}" -gt 0 ]; then
  echo "  Schema fields:"
  echo "$CUSTOMERS_RESULT" | jq -r '
    .data.search.searchResults[]? |
    .entity | "  Dataset: \(.urn)",
    (.schemaMetadata.fields[]? | "    - \(.fieldPath): \(.nativeDataType // "unknown")")
  ' 2>/dev/null
fi
echo ""

# ── 4. Get upstream lineage for a customers dataset ──────────────────
echo "4. Checking upstream lineage for customers dataset..."
# First grab the URN of a customers dataset
CUSTOMERS_URN=$(echo "$CUSTOMERS_RESULT" | jq -r '.data.search.searchResults[0]?.entity.urn // empty' 2>/dev/null)

if [ -n "${CUSTOMERS_URN:-}" ]; then
  echo "  Using URN: ${CUSTOMERS_URN}"
  # Escape quotes for GraphQL
  ESCAPED_URN=$(echo "$CUSTOMERS_URN" | sed 's/"/\\"/g')
  UPSTREAM=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { urn upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 10}) { total relationships { entity { urn type } } } } }")
  UPSTREAM_COUNT=$(echo "$UPSTREAM" | jq '.data.dataset.upstream.total // 0' 2>/dev/null)
  echo "  Upstream entities: ${UPSTREAM_COUNT}"
  echo "$UPSTREAM" | jq -r '.data.dataset.upstream.relationships[]? | "    - [\(.entity.type)] \(.entity.urn)"' 2>/dev/null
else
  echo "  No customers dataset found to query lineage for"
fi
echo ""

# ── 5. Get downstream lineage for a customers dataset ────────────────
echo "5. Checking downstream lineage for customers dataset..."
if [ -n "${CUSTOMERS_URN:-}" ]; then
  DOWNSTREAM=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { urn downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 10}) { total relationships { entity { urn type } } } } }")
  DOWNSTREAM_COUNT=$(echo "$DOWNSTREAM" | jq '.data.dataset.downstream.total // 0' 2>/dev/null)
  echo "  Downstream entities: ${DOWNSTREAM_COUNT}"
  echo "$DOWNSTREAM" | jq -r '.data.dataset.downstream.relationships[]? | "    - [\(.entity.type)] \(.entity.urn)"' 2>/dev/null
else
  echo "  No customers dataset found to query lineage for"
fi
echo ""

# ── 6. Check fine-grained (column-level) lineage ─────────────────────
echo "6. Checking column-level lineage..."
if [ -n "${CUSTOMERS_URN:-}" ]; then
  FINE_GRAINED=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { urn fineGrainedLineages { upstreams { urn field } downstreams { urn field } } } }")

  FGL_COUNT=$(echo "$FINE_GRAINED" | jq '.data.dataset.fineGrainedLineages | if . == null then 0 elif type == "array" then length else 0 end' 2>/dev/null)
  if [ "${FGL_COUNT:-0}" -gt 0 ]; then
    echo "  Column-level lineage entries: ${FGL_COUNT}"
    echo "$FINE_GRAINED" | jq -r '
      .data.dataset.fineGrainedLineages[]? |
      "    upstream: \(.upstreams[]? | "\(.urn)#\(.field)") -> downstream: \(.downstreams[]? | "\(.urn)#\(.field)")"
    ' 2>/dev/null | head -20
  else
    echo "  No fine-grained lineage found (this may be expected if DataHub has not ingested column-level data yet)"
    echo "  Checking schemaMetadata for inputFields as fallback..."
    INPUT_FIELDS=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { schemaMetadata { fields { fieldPath } } inputFields { fields { schemaField { fieldPath } schemaFieldUrn } } } }")
    echo "$INPUT_FIELDS" | jq '.' 2>/dev/null | head -30
  fi
else
  echo "  No customers dataset found to query column-level lineage for"
fi
echo ""

# ── 7. List all datasets ─────────────────────────────────────────────
echo "7. All datasets in DataHub:"
ALL_DATASETS=$(graphql_query "{ search(input: {type: DATASET, query: \\\"*\\\", start: 0, count: 50}) { total searchResults { entity { urn ... on Dataset { name platform { name } } } } } }")
ALL_COUNT=$(echo "$ALL_DATASETS" | jq '.data.search.total // 0' 2>/dev/null)
echo "  Total datasets: ${ALL_COUNT}"
echo "$ALL_DATASETS" | jq -r '.data.search.searchResults[]? | "    - [\(.entity.platform.name // "?")] \(.entity.name // .entity.urn)"' 2>/dev/null
echo ""

echo "============================================"
echo "  DataHub Frontend UI: http://localhost:9002"
echo "  DataHub GMS API:     ${DATAHUB_GMS_URL}"
echo "============================================"
