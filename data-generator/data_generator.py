# =========================================================
# Spark Certification Project - Data Generator
# Improved Version
# =========================================================

import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from setup.env import *

spark = SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

random.seed(42)

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000

countries = ["USA", "Brazil", "Germany", "France", "Canada"]
country_weights = [0.1, 0.6, 0.1, 0.1, 0.1]

categories = [
    "electronics",
    "books",
    "home",
    "fashion",
    "sports"
]

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def random_date():
    start = datetime.now() - timedelta(days=365)
    end = datetime.now()
    return start + (end - start) * random.random()


def maybe_null(value, probability=0.05):
    if random.random() < probability:
        return None
    return value


# ---------------------------------------------------------
# Generate IDs First (important for referential integrity)
# ---------------------------------------------------------

customer_ids = [str(uuid.uuid4()) for _ in range(NUM_CUSTOMERS)]
product_ids = [str(uuid.uuid4()) for _ in range(NUM_PRODUCTS)]
order_ids = [str(uuid.uuid4()) for _ in range(NUM_ORDERS)]

# ---------------------------------------------------------
# Customers
# ---------------------------------------------------------

def generate_customers():

    rows = []

    for cid in customer_ids:

        country = random.choices(countries, weights=country_weights)[0]

        email = f"user_{random.randint(1,10000)}@email.com"

        row = (
            cid,
            maybe_null(email),
            random.choice(["M", "F"]),
            country,
            random.randint(18,70),
            random_date()
        )

        rows.append(row)

        # simulate duplicates
        if random.random() < 0.05:
            rows.append(row)

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    df = spark.createDataFrame(rows, schema)

    df.write.mode("overwrite").option("header", True).csv(CUSTOMERS_PATH)


# ---------------------------------------------------------
# Products (Nested JSON with Explicit Schema)
# ---------------------------------------------------------

def generate_products():

    rows = []

    for pid in product_ids:

        category = random.choice(categories)

        price = round(random.uniform(5, 500), 2)

        if random.random() < 0.02:
            price = -price

        attributes = {
            "color": random.choice(["red", "blue", "black", "white"]),
            "size": random.choice(["S", "M", "L"])
        }

        tags = [
            random.choice(["sale", "popular", "new"]),
            random.choice(["eco", "premium", "budget"])
        ]

        variants = []

        for _ in range(random.randint(1,3)):

            variants.append({
                "variant_id": str(uuid.uuid4()),
                "stock": random.randint(0,100)
            })

        rows.append((
            pid,
            f"product_{pid[:8]}",
            category,
            price,
            attributes,
            tags,
            variants
        ))

    schema = StructType([

        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),

        StructField(
            "attributes",
            StructType([
                StructField("color", StringType(), True),
                StructField("size", StringType(), True)
            ])
        ),

        StructField(
            "tags",
            ArrayType(StringType())
        ),

        StructField(
            "variants",
            ArrayType(
                StructType([
                    StructField("variant_id", StringType(), True),
                    StructField("stock", IntegerType(), True)
                ])
            )
        )
    ])

    df = spark.createDataFrame(rows, schema)

    df.write.mode("overwrite").json(PRODUCTS_PATH)


# ---------------------------------------------------------
# Orders
# ---------------------------------------------------------

def generate_orders():

    rows = []

    for oid in order_ids:

        rows.append((
            oid,
            random.choice(customer_ids),
            random_date(),
            random.choice(["CREATED","SHIPPED","DELIVERED","CANCELLED"])
        ))

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("status", StringType(), True)
    ])

    df = spark.createDataFrame(rows, schema)

    df.write.mode("overwrite").parquet(ORDERS_PATH)


# ---------------------------------------------------------
# Order Items
# ---------------------------------------------------------

def generate_order_items():

    rows = []

    for _ in range(NUM_ORDERS * 2):

        rows.append((
            str(uuid.uuid4()),
            random.choice(order_ids),
            random.choice(product_ids),
            random.randint(1,5),
            round(random.uniform(5,200),2)
        ))

    schema = StructType([
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])

    df = spark.createDataFrame(rows, schema)

    df.write.mode("overwrite").option("header", True).csv(ORDER_ITEMS_PATH)


# ---------------------------------------------------------
# Run Generator
# ---------------------------------------------------------

if __name__ == "__main__":

    print("Generating customers...")
    generate_customers()

    print("Generating products...")
    generate_products()

    print("Generating orders...")
    generate_orders()

    print("Generating order items...")
    generate_order_items()

    print("Data generation completed!")