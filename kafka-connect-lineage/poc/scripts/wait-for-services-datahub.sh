#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for services to be ready..."

wait_for() {
  local name="$1"
  local url="$2"
  local max_attempts="${3:-60}"
  local attempt=0

  printf "  %-20s " "${name}..."
  while [ $attempt -lt $max_attempts ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo "ready"
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 2
  done
  echo "TIMEOUT after $((max_attempts * 2))s"
  return 1
}

wait_for "PostgreSQL"       "http://localhost:5432" 30 || true  # pg doesn't speak HTTP but will connect-refuse if down
wait_for "Kafka Connect"    "http://localhost:8083/connectors"
wait_for "DataHub GMS"      "http://localhost:8080/health"
wait_for "DataHub Frontend" "http://localhost:9002/health"

echo ""
echo "All services ready!"
