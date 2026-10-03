"""
shared/config.py
----------------
Single source of truth for every topic name, threshold and connection string
used by the four systems. Nothing else in the project hard-codes a topic.
"""

import os

# --------------------------------------------------------------------------
# Connections
# --------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "ecommerce360")

IST_OFFSET_HOURS = 5
IST_OFFSET_MINUTES = 30


# --------------------------------------------------------------------------
# Topics — grouped by the system that OWNS (publishes to) them
# --------------------------------------------------------------------------
class T:
    # --- Website Clickstream System (CLK) ---------------------------------
    PAGE_VIEWS = "page-views-topic"
    SEARCH_QUERIES = "search-queries-topic"
    CLICK_EVENTS = "click-events-topic"
    CART_EVENTS = "cart-events-topic"
    CHECKOUT_REQUESTS = "checkout-requests-topic"      # CLK -> OMS

    # --- Order Management System (OMS) ------------------------------------
    ORDERS = "orders-topic"
    FULFILLMENT = "fulfillment-topic"
    ORDER_CANCELLATIONS = "order-cancellations-topic"

    # --- Payment Management System (PMS) ----------------------------------
    PAYMENT_REQUESTS = "payment-requests-topic"        # OMS -> PMS
    PAYMENT_TRANSACTIONS = "payment-transactions-topic"
    PAYMENT_RESULTS = "payment-results-topic"          # PMS -> OMS

    # --- Inventory Management System (IMS) --------------------------------
    STOCK_MOVEMENTS = "stock-movements-topic"
    PURCHASE_ORDERS = "purchase-orders-topic"
    STOCK_ALERTS = "stock-alerts-topic"                # IMS -> OMS


ALL_TOPICS = [v for k, v in vars(T).items() if not k.startswith("_") and isinstance(v, str)]

TOPIC_SYSTEM = {
    T.PAGE_VIEWS: "CLK", T.SEARCH_QUERIES: "CLK", T.CLICK_EVENTS: "CLK",
    T.CART_EVENTS: "CLK", T.CHECKOUT_REQUESTS: "CLK",
    T.ORDERS: "OMS", T.FULFILLMENT: "OMS", T.ORDER_CANCELLATIONS: "OMS",
    T.PAYMENT_REQUESTS: "OMS",           # OMS raises the request
    T.PAYMENT_TRANSACTIONS: "PMS", T.PAYMENT_RESULTS: "PMS",
    T.STOCK_MOVEMENTS: "IMS", T.PURCHASE_ORDERS: "IMS", T.STOCK_ALERTS: "IMS",
}

PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
REPLICATION = int(os.getenv("KAFKA_REPLICATION", "1"))


# --------------------------------------------------------------------------
# MongoDB collections
# --------------------------------------------------------------------------
class C:
    PIPELINE_EVENTS = "pipeline_events"      # every message, every topic
    JOURNEY = "journey_orders"               # cross-system order journey
    ALERTS = "alerts"                        # unified alert stream

    CLK_SESSIONS = "clk_sessions"
    CLK_SEARCH_GAPS = "clk_search_gaps"
    CLK_CARTS = "clk_carts"

    OMS_ORDERS = "oms_orders"
    OMS_RETURNS = "oms_returns"

    PMS_TRANSACTIONS = "pms_transactions"
    PMS_PAYMENTS = "pms_payments"            # one doc per order

    IMS_STOCK = "ims_stock"                  # one doc per sku+warehouse
    IMS_MOVEMENTS = "ims_movements"
    IMS_PURCHASE_ORDERS = "ims_purchase_orders"


# --------------------------------------------------------------------------
# Business thresholds — every alert rule reads from here
# --------------------------------------------------------------------------
ZERO_RESULT_ALERT_THRESHOLD = 3       # same search term returning 0 results
CART_ABANDON_SECONDS = 45             # cart idle before it counts as abandoned
REPEAT_ABANDONER_THRESHOLD = 3

HIGH_VALUE_ORDER = 15000.0            # INR, manual review
MAX_PAYMENT_ATTEMPTS = 3              # after this the order is cancelled
FRAUD_RATE = 0.06                     # share of checkouts seeded as fraud
HIGH_VALUE_FRAUD_MIN = 60000.0

LOW_STOCK_THRESHOLD = 8
REORDER_POINT = 12
REORDER_QUANTITY = 30
DEFECT_CLUSTER_THRESHOLD = 3
