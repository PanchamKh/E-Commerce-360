@echo off
REM Creates all 14 topics.  Usage: create_topics.bat e360-kafka
setlocal
set CONTAINER=%1
if "%CONTAINER%"=="" set CONTAINER=e360-kafka

for %%T in (
  page-views-topic search-queries-topic click-events-topic
  cart-events-topic checkout-requests-topic
  orders-topic fulfillment-topic order-cancellations-topic
  payment-requests-topic payment-transactions-topic payment-results-topic
  stock-movements-topic purchase-orders-topic stock-alerts-topic
) do (
  docker exec -i %CONTAINER% kafka-topics.sh --create --if-not-exists --topic %%T --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
)

echo --- topics now on the broker ---
docker exec -i %CONTAINER% kafka-topics.sh --list --bootstrap-server localhost:9092
