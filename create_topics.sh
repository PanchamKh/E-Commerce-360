#!/usr/bin/env bash
# Creates all 14 topics. Run once after `docker compose up -d`.
set -e
CONTAINER=${1:-e360-kafka}
PARTITIONS=${2:-3}

TOPICS=(
  page-views-topic search-queries-topic click-events-topic
  cart-events-topic checkout-requests-topic
  orders-topic fulfillment-topic order-cancellations-topic
  payment-requests-topic payment-transactions-topic payment-results-topic
  stock-movements-topic purchase-orders-topic stock-alerts-topic
)

for T in "${TOPICS[@]}"; do
  docker exec -i "$CONTAINER" kafka-topics.sh --create --if-not-exists \
    --topic "$T" --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions "$PARTITIONS"
done

echo "--- topics now on the broker ---"
docker exec -i "$CONTAINER" kafka-topics.sh --list --bootstrap-server localhost:9092
