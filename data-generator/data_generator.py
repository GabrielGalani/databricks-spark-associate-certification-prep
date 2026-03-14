# =========================================================
# Spark Certification Project - Data Generator
# =========================================================

import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from setup.env import *

spark = SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()

random.seed(42)
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000

countries = ["USA", "Brazil", "Germany", "France", "Canada"]
country_weights = [0.1, 0.6, 0.1, 0.1, 0.1]
categories = ["electronics", "books", "home", "fashion", "sports"]

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def random_date():
    start = datetime.now() - timedelta(days=365)
    end = datetime.now()
    return start + (end - start) * random.random()

def maybe_null(value, probability=0.15): # Probabilidade aumentada
    if random.random() < probability:
        return None
    return value

def corrupt_data(value, prob=0.05, type="string"):
    """Injeta dados errados baseados no tipo."""
    if random.random() < prob:
        if type == "string": return "ERROR_999"
        if type == "int": return -1
        if type == "double": return -99.99
    return value

# IDs permanecem consistentes
customer_ids = [str(uuid.uuid4()) for _ in range(NUM_CUSTOMERS)]
product_ids = [str(uuid.uuid4()) for _ in range(NUM_PRODUCTS)]
order_ids = [str(uuid.uuid4()) for _ in range(NUM_ORDERS)]

# ---------------------------------------------------------
# Customers (Added: Nulls, Duplicates, Out-of-range Age)
# ---------------------------------------------------------
def generate_customers():
    rows = []
    for cid in customer_ids:
        country = random.choices(countries, weights=country_weights)[0]
        email = f"user_{random.randint(1,10000)}@email.com"
        
        row = (
            maybe_null(cid, 0.02), # ID Nulo ocasionalmente
            maybe_null(email),
            random.choice(["M", "F", "Unknown", "X"]), # Dados errados/inconsistentes
            maybe_null(country),
            corrupt_data(random.randint(18,70), 0.1, "int"), # Idades negativas
            maybe_null(random_date())
        )
        rows.append(row)
        if random.random() < 0.15: rows.append(row) # Duplicatas agressivas

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").csv(CUSTOMERS_PATH, header=True)

# ---------------------------------------------------------
# Products (Added: Negative Prices, Bad JSON nested data)
# ---------------------------------------------------------
def generate_products():
    rows = []
    for pid in product_ids:
        price = corrupt_data(round(random.uniform(5, 500), 2), 0.2, "double") # Preços negativos
        
        row = (
            pid,
            maybe_null(f"product_{pid[:8]}"),
            random.choice(categories + ["invalid_cat"]), # Categoria inexistente
            price,
            {"color": maybe_null(random.choice(["red", "blue"])), "size": "ERROR"}, # Nested corrompido
            maybe_null(["sale", "popular"]),
            [{"variant_id": str(uuid.uuid4()), "stock": -10}] # Estoque negativo
        )
        rows.append(row)
        if random.random() < 0.1: rows.append(row) # Duplicatas

    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("attributes", StructType([
            StructField("color", StringType(), True),
            StructField("size", StringType(), True)
        ])),
        StructField("tags", ArrayType(StringType())),
        StructField("variants", ArrayType(StructType([
            StructField("variant_id", StringType(), True),
            StructField("stock", IntegerType(), True)
        ])))
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").json(PRODUCTS_PATH)

# ---------------------------------------------------------
# Orders (Added: Illegal Status, Null IDs)
# ---------------------------------------------------------
def generate_orders():
    rows = []
    for oid in order_ids:
        status = random.choice(["CREATED","SHIPPED","DELIVERED","CANCELLED", "INVALID_STATUS"])
        row = (
            maybe_null(oid, 0.05),
            maybe_null(random.choice(customer_ids), 0.05),
            maybe_null(random_date()),
            status
        )
        rows.append(row)
        if random.random() < 0.1: rows.append(row)

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("status", StringType(), True)
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(ORDERS_PATH)

# ---------------------------------------------------------
# Order Items (Added: Zero Quantity, Outlier Prices)
# ---------------------------------------------------------
def generate_order_items():
    rows = []
    for _ in range(NUM_ORDERS * 2):
        row = (
            str(uuid.uuid4()),
            random.choice(order_ids),
            maybe_null(random.choice(product_ids)),
            random.randint(-5, 10), # Quantidades negativas ou zero
            corrupt_data(round(random.uniform(5,200),2), 0.1, "double")
        )
        rows.append(row)
        if random.random() < 0.05: rows.append(row)

    schema = StructType([
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").csv(ORDER_ITEMS_PATH, header=True)
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