#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose-aiven-datahub.yml"
CONNECT_URL="http://localhost:8083"

# ── Read DataHub GMS URL from the openlineage config ──────────────
OL_CONFIG="${PROJECT_DIR}/openlineage-aiven-datahub.yml"
DATAHUB_GMS_URL=$(grep 'url:' "$OL_CONFIG" | awk '{print $2}' | tr -d ' ')

if [ "$DATAHUB_GMS_URL" = "DATAHUB_GMS_URL" ] || [ -z "$DATAHUB_GMS_URL" ]; then
  echo "ERROR: You need to set the DataHub GMS URL first."
  echo ""
  echo "Edit: ${OL_CONFIG}"
  echo "Replace DATAHUB_GMS_URL with the actual URL, e.g.:"
  echo "  url: https://datahub-gms-my-service-myproject.avns.net:443"
  echo ""
  echo "Get it from your Aiven DataHub service:"
  echo "  avn service get <datahub-service> --json | jq '.service_uri'"
  echo "  Or check the Aiven Console under Service > Overview > Connection information"
  exit 1
fi

# Strip trailing slash
DATAHUB_GMS_URL="${DATAHUB_GMS_URL%/}"

echo "Using DataHub GMS at: ${DATAHUB_GMS_URL}"
echo ""

# ── Verify DataHub is reachable ───────────────────────────────────
echo "Checking DataHub connectivity..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${DATAHUB_GMS_URL}/health" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "000" ]; then
  echo "ERROR: Cannot reach ${DATAHUB_GMS_URL}/health"
  echo "  - Is the DataHub service running?"
  echo "  - Is public access enabled on the service?"
  echo "  - Are you on VPN if required?"
  exit 1
elif [ "$HTTP_CODE" = "401" ]; then
  echo "WARNING: DataHub returned 401 (Unauthorized)."
  echo "  You may need to add a token to ${OL_CONFIG}:"
  echo "    auth:"
  echo "      type: api_key"
  echo "      api_key: <your-personal-access-token>"
  echo ""
  echo "  Generate a token in DataHub UI: Settings > Access Tokens > Generate"
  echo "  Continuing anyway — the SMT might fail to send events."
  echo ""
else
  echo "  DataHub is reachable (HTTP ${HTTP_CODE})"
fi
echo ""

wait_for_url() {
  local url="$1" name="$2" max="${3:-60}" attempt=0
  printf "  Waiting for %-20s " "${name}..."
  while [ $attempt -lt $max ]; do
    if curl -sf "$url" > /dev/null 2>&1; then echo "ready"; return 0; fi
    attempt=$((attempt + 1)); sleep 3
  done
  echo "TIMEOUT"; return 1
}

case "${1:-}" in

  # ─────────────────────────────────────────────────────────────────
  # PHASE 1: "BEFORE" — No SMT, DataHub sees nothing from the connectors
  # ─────────────────────────────────────────────────────────────────
  before)
    echo "============================================"
    echo "  DEMO PHASE 1: Before (no lineage)"
    echo "  DataHub: ${DATAHUB_GMS_URL}"
    echo "============================================"
    echo ""

    echo "Starting local services (PostgreSQL + Kafka + Kafka Connect)..."
    cd "$PROJECT_DIR"
    docker compose -f "$COMPOSE_FILE" up -d
    echo ""

    echo "Waiting for services..."
    wait_for_url "${CONNECT_URL}/connectors" "Kafka Connect" 60
    echo ""

    # Register connectors WITHOUT the SMT
    echo "Registering connectors (WITHOUT lineage SMT)..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/debezium/connector-config-no-smt.json" | jq '{name, type}'
    sleep 5
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/jdbc-sink/connector-config-no-smt.json" | jq '{name, type}'
    echo ""

    echo "Waiting for connectors to start..."
    sleep 20
    echo "Inserting test data..."
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d lineage_poc <<'SQL'
INSERT INTO customers (name, email, age) VALUES ('Dave', 'dave@test.com', 28);
INSERT INTO orders (customer_id, product, amount, status) VALUES (1, 'Sprocket', 29.99, 'pending');
INSERT INTO payments (order_id, method, total) VALUES (3, 'paypal', 9.99);
SQL
    echo ""

    echo "Waiting 30s for data to propagate..."
    sleep 30

    # Check DataHub for the datasets
    echo ""
    echo "============================================"
    echo "  RESULT: Checking DataHub for lineage..."
    echo "============================================"
    DATASETS=$(curl -s "${DATAHUB_GMS_URL}/api/graphql" \
      -H 'Content-Type: application/json' \
      -d '{"query":"{ search(input: {type: DATASET, query: \"inventory\", start: 0, count: 50}) { total searchResults { entity { urn ... on Dataset { name platform { name } } } } } }"}' 2>/dev/null || echo '{"data":{"search":{"total":0}}}')
    TOTAL=$(echo "$DATASETS" | jq '.data.search.total // 0' 2>/dev/null || echo 0)
    echo "  Datasets matching 'inventory' in DataHub: ${TOTAL}"
    if [ "${TOTAL}" -gt 0 ]; then
      echo "$DATASETS" | jq -r '.data.search.searchResults[]? | "    \(.entity.platform.name // "?"):\(.entity.name // .entity.urn)"' 2>/dev/null
    fi
    echo ""
    echo "  DataHub UI: (check your Aiven Console for the DataHub Frontend URL)"
    echo "  >>> Take screenshots now for the 'before' slide <<<"
    echo "  >>> Then run: $0 after <<<"
    echo ""
    ;;

  # ─────────────────────────────────────────────────────────────────
  # PHASE 2: "AFTER" — With SMT, DataHub gets lineage
  # ─────────────────────────────────────────────────────────────────
  after)
    echo "============================================"
    echo "  DEMO PHASE 2: After (with lineage SMT)"
    echo "  DataHub: ${DATAHUB_GMS_URL}"
    echo "============================================"
    echo ""

    # Delete old connectors
    echo "Removing old connectors..."
    curl -s -X DELETE "${CONNECT_URL}/connectors/inventory-source" > /dev/null 2>&1 || true
    curl -s -X DELETE "${CONNECT_URL}/connectors/inventory-jdbc-sink" > /dev/null 2>&1 || true
    sleep 10
    echo ""

    # Register connectors WITH the SMT
    echo "Registering connectors (WITH lineage SMT)..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/debezium/connector-config.json" | jq '{name, type}'
    sleep 5
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/jdbc-sink/connector-config.json" | jq '{name, type}'
    echo ""

    echo "Waiting for connectors to start..."
    sleep 20
    echo "Inserting more test data..."
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d lineage_poc <<'SQL'
INSERT INTO customers (name, email, age) VALUES ('Eve', 'eve@test.com', 22);
INSERT INTO orders (customer_id, product, amount, status) VALUES (2, 'Thingamajig', 15.50, 'pending');
UPDATE customers SET age = 31 WHERE name = 'Alice';
SQL
    echo ""

    echo "Waiting 30s for lineage events to propagate to Aiven DataHub..."
    sleep 30

    # Check Kafka Connect logs for SMT activity
    echo ""
    echo "Checking Kafka Connect logs for OpenLineage events..."
    docker compose -f "$COMPOSE_FILE" logs kafka-connect 2>&1 | grep -i "openlineage" | tail -5
    echo ""

    # Check DataHub for the datasets
    echo ""
    echo "============================================"
    echo "  RESULT: Checking DataHub for lineage..."
    echo "============================================"

    DATASETS=$(curl -s "${DATAHUB_GMS_URL}/api/graphql" \
      -H 'Content-Type: application/json' \
      -d '{"query":"{ search(input: {type: DATASET, query: \"inventory\", start: 0, count: 50}) { total searchResults { entity { urn ... on Dataset { name platform { name } } } } } }"}' 2>/dev/null || echo '{"data":{"search":{"total":0}}}')
    TOTAL=$(echo "$DATASETS" | jq '.data.search.total // 0' 2>/dev/null || echo 0)
    echo "  Datasets matching 'inventory': ${TOTAL}"
    echo "$DATASETS" | jq -r '.data.search.searchResults[]? | "    \(.entity.platform.name // "?"):\(.entity.name // .entity.urn)"' 2>/dev/null
    echo ""

    # Check lineage
    LINEAGE=$(curl -s "${DATAHUB_GMS_URL}/api/graphql" \
      -H 'Content-Type: application/json' \
      -d '{"query":"{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:kafka,inventory.public.customers,PROD)\") { upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 10}) { total relationships { entity { urn } } } downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 10}) { total relationships { entity { urn } } } } }"}' 2>/dev/null || echo '{"data":null}')
    echo "  Lineage for kafka:inventory.public.customers:"
    echo "    Upstream:"
    echo "$LINEAGE" | jq -r '.data.dataset.upstream.relationships[]? | "      \(.entity.urn)"' 2>/dev/null
    echo "    Downstream:"
    echo "$LINEAGE" | jq -r '.data.dataset.downstream.relationships[]? | "      \(.entity.urn)"' 2>/dev/null
    echo ""

    # Check schema
    SCHEMA=$(curl -s "${DATAHUB_GMS_URL}/api/graphql" \
      -H 'Content-Type: application/json' \
      -d '{"query":"{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:kafka,inventory.public.customers,PROD)\") { schemaMetadata { fields { fieldPath nativeDataType } } } }"}' 2>/dev/null || echo '{"data":null}')
    echo "  Schema for kafka:inventory.public.customers:"
    echo "$SCHEMA" | jq -r '.data.dataset.schemaMetadata.fields[]? | "    \(.fieldPath): \(.nativeDataType)"' 2>/dev/null
    echo ""

    echo "  DataHub UI: (check your Aiven Console for the DataHub Frontend URL)"
    echo "  >>> Take screenshots now for the 'after' slide <<<"
    echo ""
    ;;

  # ─────────────────────────────────────────────────────────────────
  # CLEANUP
  # ─────────────────────────────────────────────────────────────────
  cleanup)
    echo "Tearing down local services..."
    cd "$PROJECT_DIR"
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    echo "Done. (Aiven DataHub is still running — managed by Aiven, not by this script.)"
    ;;

  *)
    echo "Usage: $0 {before|after|cleanup}"
    echo ""
    echo "  before  — Start local pipeline, register connectors WITHOUT lineage, check DataHub"
    echo "  after   — Replace connectors WITH lineage SMT, check DataHub for lineage"
    echo "  cleanup — Tear down local services (DataHub stays running on Aiven)"
    echo ""
    echo "Prerequisites:"
    echo "  1. Edit poc/openlineage-aiven-datahub.yml — set DATAHUB_GMS_URL"
    echo "  2. Build SMT: cd ../smt-kotlin && ./gradlew shadowJar (or use Docker)"
    echo "  3. Ensure DataHub is reachable (public access or VPN)"
    ;;
esac
