"""
shared/catalog.py
-----------------
The master data every system references. This is what makes the four systems
one business rather than four unrelated simulators: the SKU a shopper clicks in
the clickstream is the same SKU the OMS orders, the PMS charges for and the IMS
depletes.

Deterministic by design — seeded from a fixed list, not randomly generated at
import time, so every process that starts sees exactly the same catalog.
"""

import random
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

# product_id, name, category, sku, price (INR), opening_stock_per_warehouse
PRODUCTS = [
    ("PRD-1001", "Cotton Crew Neck T-Shirt",   "Apparel",     "SKU-1001-BLK-M",    899,  7),
    ("PRD-1002", "Slim Fit Denim Jeans",       "Apparel",     "SKU-1002-BLU-32",  1999,  14),
    ("PRD-1003", "Running Shoes",              "Footwear",    "SKU-1003-GRY-9",   2799,  11),
    ("PRD-1004", "Leather Formal Belt",        "Accessories", "SKU-1004-BRN-FS",   749,  22),
    ("PRD-1005", "Analog Wrist Watch",         "Accessories", "SKU-1005-SLV-FS",  3299,  7),
    ("PRD-2001", "Wireless Earbuds",           "Electronics", "SKU-2001-WHT-STD", 3499,  5),
    ("PRD-2002", "65W Fast Charger",           "Electronics", "SKU-2002-BLK-STD", 1299,  6),
    ("PRD-2003", "Smart Fitness Band",         "Electronics", "SKU-2003-BLK-STD", 2499,  6),
    ("PRD-2004", "Bluetooth Speaker",          "Electronics", "SKU-2004-BLU-STD", 1899,  5),
    ("PRD-2005", "1080p Webcam",               "Electronics", "SKU-2005-BLK-STD", 2199,  4),
    ("PRD-3001", "Stainless Steel Bottle 1L",  "Home",        "SKU-3001-SLV-1L",   649, 9),
    ("PRD-3002", "Cotton Bedsheet Set",        "Home",        "SKU-3002-GRN-DBL", 1499,  12),
    ("PRD-3003", "Ceramic Coffee Mug Set",     "Home",        "SKU-3003-WHT-SET",  899,  17),
    ("PRD-4001", "Organic Green Tea 250g",     "Grocery",     "SKU-4001-STD-250",  499,  28),
    ("PRD-4002", "Roasted Almonds 500g",       "Grocery",     "SKU-4002-STD-500",  799,  23),
    ("PRD-5001", "Yoga Mat 6mm",               "Sports",      "SKU-5001-PUR-6MM", 1199,  5),
    ("PRD-5002", "Adjustable Dumbbell 10kg",   "Sports",      "SKU-5002-BLK-10",  2999,  4),
    ("PRD-5003", "Cricket Bat English Willow", "Sports",      "SKU-5003-NAT-SH",  4499,  4),
]

BY_SKU = {p[3]: {"product_id": p[0], "product_name": p[1], "category": p[2],
                 "sku": p[3], "unit_price": float(p[4]), "opening_stock": p[5]}
          for p in PRODUCTS}
SKUS = list(BY_SKU)

# Real storefronts are Pareto: a handful of SKUs carry most of the demand.
# Weighting traffic this way is what makes stock-outs happen on the hot SKUs
# rather than spreading thinly and never depleting anything.
HOT_SKUS = ["SKU-2001-WHT-STD", "SKU-1001-BLK-M", "SKU-3001-SLV-1L",
            "SKU-2002-BLK-STD", "SKU-5001-PUR-6MM"]
SKU_WEIGHTS = [9 if s in HOT_SKUS else 1 for s in SKUS]


def pick_sku():
    """Weighted SKU pick — used by the clickstream so demand is concentrated."""
    return random.choices(SKUS, weights=SKU_WEIGHTS)[0]

WAREHOUSES = ["WH-DEL-01", "WH-MUM-02", "WH-BLR-03", "WH-HYD-04"]

CITIES = ["Delhi NCR", "Mumbai", "Bengaluru", "Hyderabad", "Chennai",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]

# Which warehouse normally serves which city — used for fulfilment routing
CITY_WAREHOUSE = {
    "Delhi NCR": "WH-DEL-01", "Jaipur": "WH-DEL-01", "Lucknow": "WH-DEL-01",
    "Mumbai": "WH-MUM-02", "Pune": "WH-MUM-02", "Ahmedabad": "WH-MUM-02",
    "Bengaluru": "WH-BLR-03", "Chennai": "WH-BLR-03",
    "Hyderabad": "WH-HYD-04", "Kolkata": "WH-HYD-04",
}

DEVICES = ["Mobile", "Desktop", "Tablet"]
CHANNELS = ["Organic Search", "Paid Ads", "Email Campaign", "Direct", "Social"]
SEGMENTS = ["New", "Repeat", "Prime", "Corporate"]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]
PAYMENT_GATEWAYS = ["Razorpay", "Cashfree", "PayU", "CCAvenue"]

SUPPLIERS = [("SUP-11", "Northstar Traders"), ("SUP-22", "Vertex Supplies"),
             ("SUP-33", "Orion Wholesale"), ("SUP-44", "Meridian Distributors")]

SEARCH_TERMS = [
    "t shirt", "jeans", "running shoes", "belt", "watch", "earbuds", "charger",
    "fitness band", "speaker", "webcam", "water bottle", "bedsheet", "coffee mug",
    "green tea", "almonds", "yoga mat", "dumbbell", "cricket bat",
]
# Terms with no catalog match — these drive the SEARCH_GAP_DETECTED alert
NO_RESULT_TERMS = ["air fryer", "smart tv", "office chair", "gaming laptop", "air purifier"]

FAILURE_REASONS = ["INSUFFICIENT_FUNDS", "GATEWAY_TIMEOUT", "BANK_DECLINED",
                   "CARD_EXPIRED", "RISK_CHECK_FAILED", "OTP_NOT_ENTERED"]

CANCEL_REASONS = ["Payment could not be completed", "Item went out of stock",
                  "Customer changed mind", "Delivery date too late"]
RETURN_REASONS = ["Size did not fit", "Product damaged in transit",
                  "Wrong item delivered", "Quality not as described",
                  "Defective unit", "Missing accessories"]
ADJUSTMENT_REASONS = ["Damaged during putaway", "Cycle count correction",
                      "Water damage on dock", "Packaging crushed"]


# --------------------------------------------------------------------------
class Customers:
    """A fixed pool so the same customer recurs across sessions and orders."""

    def __init__(self, size=180, seed=7):
        rng = random.Random(seed)
        self.people = []
        for i in range(size):
            city = rng.choice(CITIES)
            self.people.append({
                "customer_id": f"CUST-{10000 + i}",
                "city": city,
                "segment": rng.choices(SEGMENTS, weights=[35, 40, 20, 5])[0],
                "device": rng.choices(DEVICES, weights=[62, 30, 8])[0],
                "channel": rng.choices(CHANNELS, weights=[30, 25, 15, 20, 10])[0],
            })

    def pick(self):
        return random.choice(self.people)


def sku_info(sku):
    return BY_SKU[sku]


def warehouse_for(city):
    return CITY_WAREHOUSE.get(city, WAREHOUSES[0])


def now_ist():
    return datetime.now(IST)


def ts():
    """ISO-8601 IST timestamp — the standard event_timestamp across all systems."""
    return now_ist().isoformat(timespec="seconds")


def minute_bucket(dt=None):
    """Pre-computed minute label so charts need no date parsing."""
    return (dt or now_ist()).strftime("%m-%d %H:%M")
