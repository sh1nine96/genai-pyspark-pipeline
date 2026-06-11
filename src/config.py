import os
from pathlib import Path

# Base directory relative to this config file
BASE_DIR = Path(__file__).resolve().parent.parent

# Data Directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Data Generation Parameters
NUM_CUSTOMERS = 100_000
NUM_PRODUCTS = 10_000
NUM_ORDERS = 1_000_000

#spark settings
SPARK_MEMORY = "8g"
SPARK_CORES = 4


# File Paths (Parquet format for better performance with Spark)
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.parquet"
PRODUCTS_FILE = RAW_DATA_DIR / "products.parquet"
ORDERS_FILE = RAW_DATA_DIR / "orders.parquet"