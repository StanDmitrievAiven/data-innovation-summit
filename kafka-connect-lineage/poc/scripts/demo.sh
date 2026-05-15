#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose-datahub.yml"
CONNECT_URL="http://localhost:8083"
DATAHUB_GMS_URL="http://localhost:8080"
DATAHUB_UI_URL="http://localhost:9002"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

header() {
  echo ""
  echo -e "${BOLD}================================================================${NC}"
  echo -e "${BOLD}  $1${NC}"
  echo -e "${BOLD}================================================================${NC}"
  echo ""
}

info()    { echo -e "  ${BLUE}$1${NC}"; }
success() { echo -e "  ${GREEN}$1${NC}"; }
warn()    { echo -e "  ${YELLOW}$1${NC}"; }

wait_for_url() {
  local url="$1" name="$2" max="${3:-60}" attempt=0
  printf "  Waiting for %-28s " "${name}..."
  while [ $attempt -lt $max ]; do
    if curl -sf "$url" > /dev/null 2>&1; then echo -e "${GREEN}ready${NC}"; return 0; fi
    attempt=$((attempt + 1)); sleep 3
  done
  echo -e "${YELLOW}TIMEOUT${NC}"; return 1
}

graphql() {
  curl -s "${DATAHUB_GMS_URL}/api/graphql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$1\"}" 2>/dev/null
}

setup_policies() {
  curl -s -X POST "${DATAHUB_GMS_URL}/api/graphql" -H 'Content-Type: application/json' -d '{
    "query": "mutation { createPolicy(input: {type: PLATFORM, name: \"Admin\", description: \"Admin\", state: ACTIVE, privileges: [\"MANAGE_POLICIES\",\"MANAGE_INGESTION\",\"MANAGE_SECRETS\",\"MANAGE_USERS_AND_GROUPS\",\"VIEW_ANALYTICS\",\"GENERATE_PERSONAL_ACCESS_TOKENS\",\"MANAGE_ACCESS_TOKENS\",\"MANAGE_DOMAINS\",\"MANAGE_GLOBAL_ANNOUNCEMENTS\",\"MANAGE_TESTS\",\"MANAGE_GLOSSARIES\",\"MANAGE_TAGS\",\"MANAGE_GLOBAL_VIEWS\",\"MANAGE_GLOBAL_OWNERSHIP_TYPES\",\"MANAGE_FEATURES\",\"MANAGE_DOCUMENTATION_FORMS\"], actors: {users: [\"urn:li:corpuser:datahub\"], groups: [], allUsers: false, allGroups: false, resourceOwners: false}}) }"
  }' > /dev/null 2>&1
  curl -s -X POST "${DATAHUB_GMS_URL}/api/graphql" -H 'Content-Type: application/json' -d '{
    "query": "mutation { createPolicy(input: {type: METADATA, name: \"Metadata\", description: \"Access\", state: ACTIVE, privileges: [\"VIEW_ENTITY_PAGE\",\"EDIT_ENTITY_TAGS\",\"EDIT_ENTITY_GLOSSARY_TERMS\",\"EDIT_ENTITY_OWNERS\",\"EDIT_ENTITY_DOCS\",\"EDIT_ENTITY_DOC_LINKS\",\"EDIT_ENTITY_STATUS\",\"EDIT_ENTITY_DOMAINS\",\"EDIT_ENTITY_DEPRECATION\",\"EDIT_ENTITY\",\"EDIT_LINEAGE\",\"SEARCH_PRIVILEGE\",\"GET_ENTITY_PRIVILEGE\",\"GET_COUNTS_PRIVILEGE\"], actors: {users: [\"urn:li:corpuser:datahub\"], groups: [], allUsers: false, allGroups: false, resourceOwners: false}, resources: {allResources: true, filter: {criteria: []}}}) }"
  }' > /dev/null 2>&1
}

show_connector_status() {
  local name="$1"
  local status conn_state task_state
  status=$(curl -s "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null)
  conn_state=$(echo "$status" | jq -r '.connector.state // "UNKNOWN"' 2>/dev/null)
  task_state=$(echo "$status" | jq -r '.tasks[0].state // "UNKNOWN"' 2>/dev/null)
  if [ "$conn_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
    success "  ${name}: RUNNING"
  else
    warn "  ${name}: connector=${conn_state}, task=${task_state}"
  fi
}

show_results() {
  local result total
  result=$(graphql "{ search(input: {type: DATASET, query: \\\"*\\\", start: 0, count: 100}) { total searchResults { entity { urn ... on Dataset { name platform { name } } } } } }")
  total=$(echo "$result" | jq '.data.search.total // 0' 2>/dev/null)

  if [ "${total:-0}" -eq 0 ]; then
    warn "No datasets found yet. DataHub may need more time to process."
    warn "Wait 30 seconds and refresh the DataHub UI."
    return
  fi

  success "${total} datasets discovered automatically"
  echo ""

  # Group by platform
  for platform in postgres kafka; do
    local label count
    case "$platform" in
      postgres) label="PostgreSQL" ;;
      kafka)    label="Kafka" ;;
    esac
    count=$(echo "$result" | jq "[.data.search.searchResults[]? | select(.entity.platform.name == \"${platform}\")] | length" 2>/dev/null)
    if [ "${count:-0}" -gt 0 ]; then
      info "${label} datasets (${count}):"
      echo "$result" | jq -r ".data.search.searchResults[]? | select(.entity.platform.name == \"${platform}\") | \"    \(.entity.name)\"" 2>/dev/null | sort
      echo ""
    fi
  done

  # Find a kafka users dataset and show its lineage + schema
  local kafka_urn
  kafka_urn=$(echo "$result" | jq -r '[.data.search.searchResults[]? | select(.entity.platform.name == "kafka") | select(.entity.name | test("users"))] | .[0].entity.urn // empty' 2>/dev/null)

  if [ -n "${kafka_urn:-}" ]; then
    local lineage up down
    lineage=$(graphql "{ dataset(urn: \\\"${kafka_urn}\\\") { upstream: lineage(input: {direction: UPSTREAM, start: 0, count: 10}) { total relationships { entity { urn ... on Dataset { name platform { name } } } } } downstream: lineage(input: {direction: DOWNSTREAM, start: 0, count: 10}) { total relationships { entity { urn ... on Dataset { name platform { name } } } } } schemaMetadata { fields { fieldPath nativeDataType } } } }")

    up=$(echo "$lineage" | jq '.data.dataset.upstream.total // 0' 2>/dev/null)
    down=$(echo "$lineage" | jq '.data.dataset.downstream.total // 0' 2>/dev/null)

    info "Lineage for Kafka users topic:"
    info "  Upstream (${up}):"
    echo "$lineage" | jq -r '.data.dataset.upstream.relationships[]? | "      \(.entity.platform.name) / \(.entity.name)"' 2>/dev/null
    info "  Downstream (${down}):"
    echo "$lineage" | jq -r '.data.dataset.downstream.relationships[]? | "      \(.entity.platform.name) / \(.entity.name)"' 2>/dev/null
    echo ""

    local field_count
    field_count=$(echo "$lineage" | jq '[.data.dataset.schemaMetadata.fields[]?] | length' 2>/dev/null)
    if [ "${field_count:-0}" -gt 0 ]; then
      info "Schema (${field_count} columns):"
      echo "$lineage" | jq -r '.data.dataset.schemaMetadata.fields[]? | "    \(.fieldPath): \(.nativeDataType)"' 2>/dev/null
      echo ""
    fi
  fi
}

case "${1:-}" in

  # ================================================================
  # START
  # ================================================================
  start)
    header "Starting Infrastructure"

    info "Bringing up PostgreSQL, Kafka, Kafka Connect, and DataHub..."
    cd "$PROJECT_DIR"
    docker compose -f "$COMPOSE_FILE" up -d 2>&1 | tail -3
    echo ""

    info "Waiting for services to become healthy..."
    wait_for_url "${DATAHUB_GMS_URL}/health" "DataHub GMS" 120
    wait_for_url "${DATAHUB_UI_URL}/health"  "DataHub Frontend" 120
    wait_for_url "${CONNECT_URL}/connectors"  "Kafka Connect" 60
    echo ""

    info "Configuring DataHub access policies..."
    setup_policies
    success "Done."

    header "Infrastructure Ready"
    info "DataHub UI:   ${DATAHUB_UI_URL}"
    info "Login:        datahub / datahub"
    echo ""
    info "Next step:    ./scripts/demo.sh before"
    echo ""
    ;;

  # ================================================================
  # BEFORE: pipeline running, DataHub sees nothing
  # ================================================================
  before)
    header "BEFORE: Pipeline Running, Zero Lineage"

    info "Deploying an e-commerce CDC pipeline:"
    info "  PostgreSQL (ecommerce) -> Debezium -> Kafka -> JDBC Sink -> PostgreSQL (analytics_staging)"
    info "  5 tables: users, products, orders, order_items, shipments"
    info "  Connectors do NOT have the OpenLineage SMT."
    echo ""

    info "Registering connectors..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/debezium/connector-config-no-smt.json" > /dev/null
    sleep 5
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/jdbc-sink/connector-config-no-smt.json" > /dev/null

    info "Waiting for initial snapshot to complete..."
    sleep 25

    info "Connector status:"
    show_connector_status "ecommerce-cdc-source"
    show_connector_status "analytics-warehouse-sink"
    echo ""

    info "Inserting live transactions..."
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d ecommerce -q <<'SQL'
INSERT INTO users (first_name, last_name, email, phone, country)
VALUES ('Nina', 'Koskinen', 'nina.koskinen@mail.fi', '+358401111111', 'Finland');

INSERT INTO products (sku, name, category, brand, unit_price)
VALUES ('WEAR-HAT-001', 'Beanie Classic', 'Accessories', 'Fjallraven', 39.99);

UPDATE orders SET status = 'shipped' WHERE order_id = 5;
SQL
    success "Transactions committed."
    echo ""

    info "Waiting for CDC events to propagate..."
    sleep 15

    info "Kafka topics created by Debezium:"
    docker compose -f "$COMPOSE_FILE" exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null \
      | grep ecommerce | while read -r t; do success "  $t"; done
    echo ""

    header "Result: DataHub Has No Visibility"

    DATASET_COUNT=$(graphql "{ search(input: {type: DATASET, query: \\\"*\\\", start: 0, count: 10}) { total } }" | jq '.data.search.total // 0' 2>/dev/null)
    info "Datasets in DataHub: ${DATASET_COUNT:-0}"
    echo ""

    info "The CDC pipeline is fully operational. Data flows from the"
    info "source database through Kafka into the analytics warehouse."
    info "But DataHub has no visibility into any of it."
    echo ""
    info "DataHub UI:   ${DATAHUB_UI_URL}"
    info "Login:        datahub / datahub"
    echo ""
    info "Next step:    ./scripts/demo.sh after"
    echo ""
    ;;

  # ================================================================
  # AFTER: enable OpenLineage SMT, lineage appears
  # ================================================================
  after)
    header "AFTER: Enabling OpenLineage Column-Level Lineage"

    info "Removing existing connectors..."
    for c in ecommerce-cdc-source analytics-warehouse-sink staging-sink search-sync-sink staging-cdc-source warehouse-sink; do
      curl -s -X DELETE "${CONNECT_URL}/connectors/$c" > /dev/null 2>&1 || true
    done
    sleep 5

    # Drop old replication slots
    for db in ecommerce analytics_staging; do
      docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d "$db" -q -c \
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE active = false;" 2>/dev/null || true
    done

    # ── Stage 1: Source DB -> Kafka -> Staging + Search ───────────────
    info "Stage 1: ecommerce -> Kafka -> analytics_staging + search_index"
    info "  Deploying ecommerce CDC source..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/debezium/connector-config.json" > /dev/null
    sleep 5

    info "  Deploying staging sink + search sink..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/jdbc-sink/connector-config-staging.json" > /dev/null
    sleep 2
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/jdbc-sink/connector-config-search.json" > /dev/null

    info "  Waiting for stage 1 snapshot and data to land in staging DB..."
    sleep 30

    show_connector_status "ecommerce-cdc-source"
    show_connector_status "staging-sink"
    show_connector_status "search-sync-sink"
    echo ""

    # ── Stage 2: Staging DB -> Kafka -> Warehouse ─────────────────────
    info "Stage 2: analytics_staging -> Kafka -> warehouse"
    info "  Deploying staging CDC source..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/debezium/connector-config-staging.json" > /dev/null
    sleep 5

    info "  Deploying warehouse sink..."
    curl -s -X POST "${CONNECT_URL}/connectors" \
      -H "Content-Type: application/json" \
      -d @"${PROJECT_DIR}/jdbc-sink/connector-config-warehouse.json" > /dev/null

    info "  Waiting for stage 2 snapshot and lineage propagation..."
    sleep 30

    show_connector_status "staging-cdc-source"
    show_connector_status "warehouse-sink"
    echo ""

    # ── Insert live data to trigger CDC through both stages ───────────
    info "Inserting live transactions..."
    docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U postgres -d ecommerce -q <<'SQL'
INSERT INTO users (first_name, last_name, email, phone, country)
VALUES ('Henrik', 'Larsen', 'henrik.larsen@mail.dk', '+4520123456', 'Denmark');

INSERT INTO products (sku, name, category, brand, unit_price)
VALUES ('ELEC-HEADP-001', 'WH-1000XM5', 'Electronics', 'Sony', 349.99);

INSERT INTO orders (user_id, total_amount, currency, status, shipping_country)
VALUES (6, 349.99, 'EUR', 'confirmed', 'France');

INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount)
VALUES (6, 6, 2, 229.00, 10.00);

INSERT INTO shipments (order_id, carrier, tracking_number, status)
VALUES (8, 'Posti', 'FI202412010001', 'in_transit');
SQL
    success "Transactions committed."
    info "Waiting for data to flow through both stages..."
    sleep 30

    header "Result: Full Lineage DAG"
    info "Pipeline: ecommerce -> Kafka -> staging + search -> Kafka -> warehouse"
    info "5 connectors, 4 databases, all lineage from the SMT, zero mutations"
    echo ""

    show_results

    header "DataHub UI"
    info "URL:          ${DATAHUB_UI_URL}"
    info "Login:        datahub / datahub"
    echo ""
    info "Best lineage views (set depth slider to 5+):"
    info "  Full chain:    ${DATAHUB_UI_URL}/dataset/urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.public.users,PROD)/Lineage"
    info "  Branch point:  ${DATAHUB_UI_URL}/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,ecommerce.public.users,PROD)/Lineage"
    info "  Mid-stage:     ${DATAHUB_UI_URL}/dataset/urn:li:dataset:(urn:li:dataPlatform:postgres,analytics_staging.public.users,PROD)/Lineage"
    echo ""
    ;;

  # ================================================================
  # CLEANUP
  # ================================================================
  cleanup)
    header "Cleanup"
    info "Tearing down all services..."
    cd "$PROJECT_DIR"
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>&1 | tail -5
    success "Done."
    echo ""
    ;;

  *)
    echo ""
    echo "Usage: $0 {start|before|after|cleanup}"
    echo ""
    echo "  start    Start all infrastructure (PostgreSQL, Kafka, DataHub)"
    echo "  before   Deploy CDC pipeline WITHOUT lineage, verify DataHub is empty"
    echo "  after    Enable OpenLineage SMT, verify lineage appears in DataHub"
    echo "  cleanup  Tear down all services"
    echo ""
    echo "Run in order: start -> before -> after -> cleanup"
    echo ""
    ;;
esac
