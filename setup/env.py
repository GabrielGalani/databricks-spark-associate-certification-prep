# -----------------------------------------
# Catalog Configuration
# -----------------------------------------

CATALOG_NAME = "spark_cert_catalog"

# -----------------------------------------
# Lakehouse Schemas
# -----------------------------------------

SCHEMA_RAW = "raw"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# -----------------------------------------
# Base Paths
# -----------------------------------------

# Base project path (DBFS or Unity Catalog Volumes)
BASE_PATH = "/Volumes/spark_cert_catalog/raw/raw_data"

RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

# -----------------------------------------
# Dataset Paths (RAW Layer)
# -----------------------------------------

CUSTOMERS_PATH = f"{RAW_PATH}/customers"
PRODUCTS_PATH = f"{RAW_PATH}/products"
ORDERS_PATH = f"{RAW_PATH}/orders"
ORDER_ITEMS_PATH = f"{RAW_PATH}/order_items"

# -----------------------------------------
# File Formats Used
# -----------------------------------------

CUSTOMERS_FORMAT = "csv"
PRODUCTS_FORMAT = "json"
ORDERS_FORMAT = "parquet"
ORDER_ITEMS_FORMAT = "csv"

# -----------------------------------------
# Spark Configurations (Optional)
# -----------------------------------------

SPARK_APP_NAME = "spark-certification-project"

# Default shuffle partitions for study
DEFAULT_SHUFFLE_PARTITIONS = 8