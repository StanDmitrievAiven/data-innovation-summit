#!/usr/bin/env bash
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGDATABASE="${PGDATABASE:-lineage_poc}"

export PGPASSWORD

echo "Inserting test data to trigger CDC events..."

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" <<'SQL'
-- New customer
INSERT INTO customers (name, email, age) VALUES ('Dave', 'dave@test.com', 28);

-- New order for existing customer
INSERT INTO orders (customer_id, product, amount, status)
VALUES (1, 'Sprocket', 29.99, 'pending');

-- New payment
INSERT INTO payments (order_id, method, total)
VALUES (3, 'paypal', 9.99);

-- Update existing record (generates UPDATE CDC event)
UPDATE customers SET age = 31 WHERE name = 'Alice';
UPDATE orders SET status = 'delivered' WHERE id = 2;
SQL

echo ""
echo "Test data inserted. CDC events should flow through connectors."
echo ""
echo "Waiting 10s for events to propagate..."
sleep 10

echo "Checking Kafka topics..."
docker exec -it "$(docker ps -q -f name=kafka)" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null | grep inventory || echo "(topics not yet available)"
