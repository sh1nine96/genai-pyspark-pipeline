import os
from pathlib import Path

# Base directory relative to this config file
BASE_DIR = Path(__file__).resolve().parent.parent

# Data Directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Data Generation Parameters
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 20
NUM_ORDERS = 500

# File Paths
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
PRODUCTS_FILE = RAW_DATA_DIR / "products.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"