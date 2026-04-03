# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Setup: Generate Raw Data
# MAGIC
# MAGIC This notebook generates synthetic raw data for the **RetailCo** data engineering exercise
# MAGIC and persists it as Delta tables in the `raw` schema.
# MAGIC
# MAGIC **Tables created:**
# MAGIC | Table | Rows | Description |
# MAGIC |---|---|---|
# MAGIC | `raw.customers` | ~500 | Customer master data |
# MAGIC | `raw.products` | ~105 | Product catalogue (includes duplicates) |
# MAGIC | `raw.orders` | ~5 000 | Order transactions |
# MAGIC
# MAGIC > Run this notebook once before starting the exercise.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS mart;

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)

# ── helpers ────────────────────────────────────────────────────────────────────

def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return str(start + timedelta(days=random.randint(0, delta)))

# ── customers ──────────────────────────────────────────────────────────────────

COUNTRIES = {
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Edinburgh", None],
    "France":         ["Paris", "Lyon", "Marseille", "Toulouse", "Nice", None],
    "Germany":        ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne", None],
    "Spain":          ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao", None],
    "Italy":          ["Rome", "Milan", "Naples", "Turin", "Florence", None],
}
SEGMENTS   = ["Premium", "Standard", "Basic"]
SEG_WEIGHT = [0.20,      0.50,       0.30]

FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Isla", "James", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zoe", "Adrian", "Bella", "Carlos", "Daisy",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson",
    "White", "Harris", "Martin", "Thompson", "Young", "Hall",
]

customers = []
for i in range(1, 501):
    country = random.choices(
        list(COUNTRIES.keys()), weights=[0.30, 0.25, 0.20, 0.15, 0.10]
    )[0]
    customers.append({
        "customer_id":  f"C{i:04d}",
        "first_name":   random.choice(FIRST_NAMES),
        "last_name":    random.choice(LAST_NAMES),
        "email":        f"user{i}@retailco-example.com",
        "country":      country,
        "city":         random.choice(COUNTRIES[country]),   # ~17 % nulls by design
        "segment":      random.choices(SEGMENTS, weights=SEG_WEIGHT)[0],
        "signup_date":  random_date(date(2020, 1, 1), date(2023, 12, 31)),
    })

print(f"Customers generated: {len(customers)}")

# COMMAND ----------

# ── products ───────────────────────────────────────────────────────────────────

CATALOGUE = {
    "Electronics": {
        "subcategories": ["Laptops", "Smartphones", "Headphones", "Tablets", "Cameras"],
        "brands":        ["TechPro", "NovaTech", "CoreElec", "Zenith"],
        "price_range":   (50, 1500),
    },
    "Clothing": {
        "subcategories": ["T-Shirts", "Jeans", "Jackets", "Shoes", "Accessories"],
        "brands":        ["UrbanWear", "StyleCo", "FitLine", "NordStyle"],
        "price_range":   (10, 250),
    },
    "Home": {
        "subcategories": ["Kitchen", "Bedding", "Decor", "Storage", "Lighting"],
        "brands":        ["HomePlus", "NestCo", "CozyCraft", "SpaceWise"],
        "price_range":   (8, 400),
    },
    "Sports": {
        "subcategories": ["Fitness", "Outdoor", "Cycling", "Team Sports", "Swimming"],
        "brands":        ["ActivePro", "TrailBlaze", "PeakGear", "SpeedFit"],
        "price_range":   (15, 600),
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Technical", "Children", "Comics"],
        "brands":        ["PageTurn", "ReadMore", "InkHouse", "WordCraft"],
        "price_range":   (5, 80),
    },
}
CAT_WEIGHTS = [0.25, 0.25, 0.20, 0.15, 0.15]

PRODUCT_ADJECTIVES = ["Pro", "Plus", "Elite", "Max", "Mini", "Ultra", "Smart", "Eco"]

products_unique = []
cat_list = list(CATALOGUE.keys())

for i in range(1, 101):
    cat    = random.choices(cat_list, weights=CAT_WEIGHTS)[0]
    info   = CATALOGUE[cat]
    subcat = random.choice(info["subcategories"])
    brand  = random.choice(info["brands"])
    adj    = random.choice(PRODUCT_ADJECTIVES)
    lo, hi = info["price_range"]
    cost   = round(random.uniform(lo * 0.4, hi * 0.4), 2)
    price  = round(cost * random.uniform(1.4, 3.2), 2)

    products_unique.append({
        "product_id":   f"P{i:03d}",
        "product_name": f"{brand} {subcat} {adj} {i}",
        "category":     cat,
        "subcategory":  subcat,
        "brand":        brand,
        "cost_price":   cost,
        "list_price":   price,
    })

# Inject 5 duplicate rows to create a data quality issue
duplicates = random.sample(products_unique, 5)
products   = products_unique + duplicates

print(f"Products generated: {len(products)}  (unique: {len(products_unique)}, duplicates: {len(duplicates)})")

# COMMAND ----------

# ── orders ─────────────────────────────────────────────────────────────────────

DISCOUNT_OPTS    = [0,   0,    0,    5,    10,   15,   20,   None]
DISCOUNT_WEIGHTS = [0.30, 0.20, 0.15, 0.15, 0.10, 0.05, 0.03, 0.02]
STATUS_OPTS      = ["Completed", "Returned",  "Cancelled"]
STATUS_WEIGHTS   = [0.85,        0.10,        0.05]

valid_customer_ids = [c["customer_id"] for c in customers]
valid_product_ids  = [p["product_id"]  for p in products_unique]

orders = []
for i in range(1, 5001):
    # Inject ~15 orders with a non-existent customer (referential integrity issue)
    if i in random.sample(range(1, 5001), 15):
        cust_id = f"C9999"
    else:
        cust_id = random.choice(valid_customer_ids)

    prod    = random.choice(products_unique)
    disc    = random.choices(DISCOUNT_OPTS, weights=DISCOUNT_WEIGHTS)[0]
    qty     = random.choices([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], weights=[35, 25, 15, 8, 5, 4, 3, 2, 2, 1])[0]

    # Inject 5 negative quantities (data entry error)
    if i in [142, 879, 1563, 3201, 4750]:
        qty = -qty

    orders.append({
        "order_id":    f"ORD-{i:05d}",
        "customer_id": cust_id,
        "product_id":  prod["product_id"],
        "order_date":  random_date(date(2022, 1, 1), date(2024, 12, 31)),
        "quantity":    qty,
        "unit_price":  round(prod["list_price"] * (1 - (disc or 0) / 100), 2),
        "discount_pct": disc,
        "status":      random.choices(STATUS_OPTS, weights=STATUS_WEIGHTS)[0],
    })

print(f"Orders generated: {len(orders)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist as Delta tables

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

# ── raw.customers ──────────────────────────────────────────────────────────────
df_customers = spark.createDataFrame(customers)
(df_customers
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("raw.customers"))

print(f"raw.customers → {df_customers.count()} rows")

# ── raw.products ───────────────────────────────────────────────────────────────
df_products = spark.createDataFrame(products)
(df_products
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("raw.products"))

print(f"raw.products  → {df_products.count()} rows")

# ── raw.orders ─────────────────────────────────────────────────────────────────
df_orders = spark.createDataFrame(orders)
(df_orders
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("raw.orders"))

print(f"raw.orders    → {df_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS tbl, COUNT(*) AS rows FROM raw.customers
# MAGIC UNION ALL
# MAGIC SELECT 'products',         COUNT(*)         FROM raw.products
# MAGIC UNION ALL
# MAGIC SELECT 'orders',           COUNT(*)         FROM raw.orders;
