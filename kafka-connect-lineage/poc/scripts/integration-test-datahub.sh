#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Integration Test: OpenLineage SMT Column-Level Lineage (DataHub variant)
#
# This script:
#   1. Builds the SMT JAR
#   2. Starts all Docker services (Postgres, Kafka, Connect, DataHub)
#   3. Registers Debezium source connector with the OpenLineage SMT
#   4. Waits for initial snapshot to complete
#   5. Inserts new data to trigger CDC events
#   6. Verifies column-level lineage is visible in DataHub API
#   7. Tears down services
#
# Prerequisites: docker, docker compose, java 11+, curl, jq
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATAHUB_GMS_URL="http://localhost:8080"
CONNECT_URL="http://localhost:8083"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose-datahub.yml"

PASS=0
FAIL=0
TESTS=()

pass() {
  local name="$1"
  PASS=$((PASS + 1))
  TESTS+=("PASS: $name")
  echo "  [PASS] $name"
}

fail() {
  local name="$1"
  local detail="${2:-}"
  FAIL=$((FAIL + 1))
  TESTS+=("FAIL: $name${detail:+ — $detail}")
  echo "  [FAIL] $name${detail:+ — $detail}"
}

cleanup() {
  echo ""
  echo "Cleaning up Docker services..."
  cd "$PROJECT_DIR"
  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

# Optionally keep services running for debugging
if [ "${KEEP_SERVICES:-}" != "true" ]; then
  trap cleanup EXIT
fi

echo "============================================"
echo "  OpenLineage SMT Integration Test (DataHub)"
echo "============================================"
echo ""

# ── Step 1: Build the SMT JAR ───────────────────────────────────────
echo "Step 1: Building SMT JAR..."
cd "${PROJECT_DIR}/../smt"
./gradlew shadowJar -q 2>&1
if [ -f build/libs/openlineage-smt-all.jar ]; then
  pass "SMT shadow JAR built"
else
  fail "SMT shadow JAR build" "JAR not found"
  exit 1
fi

# ── Step 2: Start Docker services ───────────────────────────────────
echo ""
echo "Step 2: Starting Docker services..."
cd "$PROJECT_DIR"
docker compose -f "$COMPOSE_FILE" up -d 2>&1

# Wait for services
echo "  Waiting for services to be ready..."
MAX_WAIT=300

wait_for_url() {
  local url="$1"
  local name="$2"
  local waited=0
  while [ $waited -lt $MAX_WAIT ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      return 0
    fi
    sleep 3
    waited=$((waited + 3))
  done
  return 1
}

if wait_for_url "${DATAHUB_GMS_URL}/health" "DataHub GMS"; then
  pass "DataHub GMS is healthy"
else
  fail "DataHub GMS health" "Not reachable after ${MAX_WAIT}s"
  echo "  Dumping datahub-gms logs:"
  docker compose -f "$COMPOSE_FILE" logs datahub-gms 2>&1 | tail -30
  exit 1
fi

if wait_for_url "${CONNECT_URL}/connectors" "Kafka Connect"; then
  pass "Kafka Connect is healthy"
else
  fail "Kafka Connect health" "Not reachable after ${MAX_WAIT}s"
  echo "  Dumping kafka-connect logs:"
  docker compose -f "$COMPOSE_FILE" logs kafka-connect 2>&1 | tail -30
  exit 1
fi

# ── Step 3: Register Debezium source connector ──────────────────────
echo ""
echo "Step 3: Registering Debezium source connector..."
HTTP_CODE=$(curl -s -o /tmp/register-source-datahub.txt -w "%{http_code}" \
  -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${PROJECT_DIR}/debezium/connector-config.json")

if [ "$HTTP_CODE" -eq 201 ] || [ "$HTTP_CODE" -eq 200 ]; then
  pass "Debezium source connector registered (HTTP ${HTTP_CODE})"
elif [ "$HTTP_CODE" -eq 409 ]; then
  pass "Debezium source connector already exists (HTTP 409)"
else
  fail "Debezium source connector registration" "HTTP ${HTTP_CODE}: $(cat /tmp/register-source-datahub.txt)"
fi

# Wait for connector to start and complete snapshot
echo "  Waiting for Debezium snapshot (up to 60s)..."
sleep 15

CONNECTOR_STATE=""
for i in $(seq 1 20); do
  STATUS=$(curl -s "${CONNECT_URL}/connectors/inventory-source/status" 2>/dev/null)
  CONNECTOR_STATE=$(echo "$STATUS" | jq -r '.connector.state' 2>/dev/null)
  TASK_STATE=$(echo "$STATUS" | jq -r '.tasks[0].state' 2>/dev/null)
  if [ "$CONNECTOR_STATE" = "RUNNING" ] && [ "$TASK_STATE" = "RUNNING" ]; then
    break
  fi
  sleep 3
done

if [ "$CONNECTOR_STATE" = "RUNNING" ]; then
  pass "Debezium connector is RUNNING"
else
  fail "Debezium connector state" "State: ${CONNECTOR_STATE}"
fi

# ── Step 4: Insert test data (triggers CDC) ─────────────────────────
echo ""
echo "Step 4: Inserting test data..."
docker compose -f "$COMPOSE_FILE" exec -T postgres \
  psql -U postgres -d lineage_poc <<'SQL'
INSERT INTO customers (name, email, age) VALUES ('Dave', 'dave@test.com', 28);
INSERT INTO orders (customer_id, product, amount, status) VALUES (1, 'Sprocket', 29.99, 'pending');
INSERT INTO payments (order_id, method, total) VALUES (3, 'paypal', 9.99);
UPDATE customers SET age = 31 WHERE name = 'Alice';
SQL

if [ $? -eq 0 ]; then
  pass "Test data inserted"
else
  fail "Test data insertion"
fi

echo "  Waiting for CDC events to propagate (30s)..."
sleep 30

# ── Step 5: Verify Kafka topics exist ───────────────────────────────
echo ""
echo "Step 5: Verifying Kafka topics..."
TOPICS=$(MSYS_NO_PATHCONV=1 docker compose -f "$COMPOSE_FILE" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null)

if echo "$TOPICS" | grep -q "inventory.public.customers"; then
  pass "Kafka topic 'inventory.public.customers' exists"
else
  fail "Kafka topic check" "inventory.public.customers not found"
fi

if echo "$TOPICS" | grep -q "inventory.public.orders"; then
  pass "Kafka topic 'inventory.public.orders' exists"
else
  fail "Kafka topic check" "inventory.public.orders not found"
fi

# ── Step 6: Verify OpenLineage events in DataHub ────────────────────
echo ""
echo "Step 6: Verifying OpenLineage events in DataHub..."

# Check the OpenLineage events endpoint
OL_EVENTS=$(curl -s "${DATAHUB_GMS_URL}/openapi/openlineage/api/v2/lineage/events" 2>/dev/null || echo "{}")
if echo "$OL_EVENTS" | jq -e '.' > /dev/null 2>&1; then
  OL_COUNT=$(echo "$OL_EVENTS" | jq 'if type == "array" then length else 0 end' 2>/dev/null || echo "0")
  if [ "${OL_COUNT:-0}" -gt 0 ]; then
    pass "OpenLineage events received by DataHub (${OL_COUNT} events)"
  else
    echo "  OpenLineage events endpoint returned 0 events (may use internal ingestion)"
  fi
else
  echo "  OpenLineage events endpoint not available (events may be ingested internally)"
fi

# Search for datasets via GraphQL
graphql_query() {
  local query="$1"
  curl -s "${DATAHUB_GMS_URL}/api/graphql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\"}"
}

SEARCH_RESULT=$(graphql_query "{ search(input: {type: DATASET, query: \\\"inventory\\\", start: 0, count: 20}) { total searchResults { entity { urn ... on Dataset { name } } } } }")
SEARCH_TOTAL=$(echo "$SEARCH_RESULT" | jq '.data.search.total // 0' 2>/dev/null)

if [ "${SEARCH_TOTAL:-0}" -gt 0 ]; then
  pass "Datasets found in DataHub (${SEARCH_TOTAL} datasets)"
  echo "  Datasets:"
  echo "$SEARCH_RESULT" | jq -r '.data.search.searchResults[]? | "    - \(.entity.name // .entity.urn)"' 2>/dev/null
else
  fail "Dataset check" "No datasets found in DataHub"
fi

# ── Step 7: Verify datasets have schema fields ─────────────────────
echo ""
echo "Step 7: Verifying dataset schema in DataHub..."

CUSTOMERS_RESULT=$(graphql_query "{ search(input: {type: DATASET, query: \\\"customers\\\", start: 0, count: 10}) { total searchResults { entity { urn ... on Dataset { name schemaMetadata { fields { fieldPath nativeDataType } } } } } } }")
CUSTOMERS_TOTAL=$(echo "$CUSTOMERS_RESULT" | jq '.data.search.total // 0' 2>/dev/null)

if [ "${CUSTOMERS_TOTAL:-0}" -gt 0 ]; then
  FIELD_COUNT=$(echo "$CUSTOMERS_RESULT" | jq '[.data.search.searchResults[]?.entity.schemaMetadata.fields[]?] | length' 2>/dev/null)
  if [ "${FIELD_COUNT:-0}" -gt 0 ]; then
    pass "Schema fields found for customers dataset (${FIELD_COUNT} fields)"
    echo "  Fields:"
    echo "$CUSTOMERS_RESULT" | jq -r '.data.search.searchResults[]?.entity.schemaMetadata.fields[]? | "    - \(.fieldPath): \(.nativeDataType // "?")"' 2>/dev/null
  else
    fail "Schema fields" "Customers dataset exists but has no schema fields"
  fi
else
  fail "Customers dataset" "No customers dataset found in DataHub"
fi

# ── Step 8: Verify lineage relationships ──────────────────────────
echo ""
echo "Step 8: Verifying lineage relationships..."

CUSTOMERS_URN=$(echo "$CUSTOMERS_RESULT" | jq -r '.data.search.searchResults[0]?.entity.urn // empty' 2>/dev/null)

if [ -n "${CUSTOMERS_URN:-}" ]; then
  ESCAPED_URN=$(echo "$CUSTOMERS_URN" | sed 's/"/\\"/g')

  # Check upstream lineage
  UPSTREAM=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 10}) { total relationships { entity { urn type } } } } }")
  UPSTREAM_COUNT=$(echo "$UPSTREAM" | jq '.data.dataset.upstream.total // 0' 2>/dev/null)

  if [ "${UPSTREAM_COUNT:-0}" -gt 0 ]; then
    pass "Upstream lineage found (${UPSTREAM_COUNT} entities)"
    echo "$UPSTREAM" | jq -r '.data.dataset.upstream.relationships[]? | "    - [\(.entity.type)] \(.entity.urn)"' 2>/dev/null
  else
    echo "  No upstream lineage found (may be expected depending on dataset direction)"
  fi

  # Check downstream lineage
  DOWNSTREAM=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 10}) { total relationships { entity { urn type } } } } }")
  DOWNSTREAM_COUNT=$(echo "$DOWNSTREAM" | jq '.data.dataset.downstream.total // 0' 2>/dev/null)

  if [ "${DOWNSTREAM_COUNT:-0}" -gt 0 ]; then
    pass "Downstream lineage found (${DOWNSTREAM_COUNT} entities)"
    echo "$DOWNSTREAM" | jq -r '.data.dataset.downstream.relationships[]? | "    - [\(.entity.type)] \(.entity.urn)"' 2>/dev/null
  else
    echo "  No downstream lineage found"
  fi

  # Check if any lineage exists at all
  if [ "${UPSTREAM_COUNT:-0}" -gt 0 ] || [ "${DOWNSTREAM_COUNT:-0}" -gt 0 ]; then
    pass "Lineage relationships exist in DataHub"
  else
    fail "Lineage relationships" "No upstream or downstream lineage found"
  fi
else
  fail "Lineage check" "No customers dataset URN to query lineage for"
fi

# ── Step 9: Check column-level lineage ──────────────────────────────
echo ""
echo "Step 9: Checking column-level lineage..."

if [ -n "${CUSTOMERS_URN:-}" ]; then
  FINE_GRAINED=$(graphql_query "{ dataset(urn: \\\"${ESCAPED_URN}\\\") { fineGrainedLineages { upstreams { urn field } downstreams { urn field } } } }")
  FGL_COUNT=$(echo "$FINE_GRAINED" | jq '.data.dataset.fineGrainedLineages | if . == null then 0 elif type == "array" then length else 0 end' 2>/dev/null)

  if [ "${FGL_COUNT:-0}" -gt 0 ]; then
    pass "Column-level (fine-grained) lineage found (${FGL_COUNT} entries)"
    echo "$FINE_GRAINED" | jq -r '
      .data.dataset.fineGrainedLineages[]? |
      "    upstream: \(.upstreams[]? | "\(.urn)#\(.field)") -> downstream: \(.downstreams[]? | "\(.urn)#\(.field)")"
    ' 2>/dev/null | head -20
  else
    fail "Column-level lineage" "No fine-grained lineage found in DataHub"
  fi
else
  fail "Column-level lineage" "No dataset URN available"
fi

# ── Results ─────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  RESULTS: ${PASS} passed, ${FAIL} failed"
echo "============================================"
for t in "${TESTS[@]}"; do
  echo "  $t"
done
echo ""

if [ "$FAIL" -gt 0 ]; then
  echo "Some tests failed. Check logs:"
  echo "  docker compose -f ${COMPOSE_FILE} logs kafka-connect 2>&1 | tail -50"
  echo "  docker compose -f ${COMPOSE_FILE} logs datahub-gms 2>&1 | tail -50"
  exit 1
else
  echo "All integration tests passed!"
  echo "  DataHub UI: http://localhost:9002"
fi
