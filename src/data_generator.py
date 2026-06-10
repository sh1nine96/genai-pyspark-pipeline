import csv
import random
import logging
from typing import List, Dict, Any
from faker import Faker
from src import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

def ensure_directories_exist() -> None:
    """Ensures that the raw and processed data directories exist."""
    config.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Verified data directories exist.")

def generate_customers(num_customers: int) -> List[Dict[str, Any]]:
    """Generates a list of fake customer dictionaries."""
    logger.info(f"Generating {num_customers} customers...")
    return [
        {
            "customer_id": i + 1,
            "name": fake.name(),
            "email": fake.email(),
            "country": fake.country()
        }
        for i in range(num_customers)
    ]

def generate_products(num_products: int) -> List[Dict[str, Any]]:
    """Generates a list of fake product dictionaries."""
    logger.info(f"Generating {num_products} products...")
    return [
        {
            "product_id": i + 1,
            "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "price": round(random.uniform(10.0, 500.0), 2)
        }
        for i in range(num_products)
    ]

def generate_orders(num_orders: int, num_customers: int, num_products: int) -> List[Dict[str, Any]]:
    """Generates a list of fake order dictionaries linked to customers and products."""
    logger.info(f"Generating {num_orders} orders...")
    return [
        {
            "order_id": i + 1,
            "customer_id": random.randint(1, num_customers),
            "product_id": random.randint(1, num_products),
            "quantity": random.randint(1, 5),
            "order_date": fake.date_between(start_date='-1y', end_date='today').isoformat()
        }
        for i in range(num_orders)
    ]

def save_to_csv(data: List[Dict[str, Any]], filepath: str) -> None:
    """Saves a list of dictionaries to a CSV file."""
    if not data:
        logger.warning(f"No data to save for {filepath}")
        return

    keys = data[0].keys()
    with open(filepath, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    logger.info(f"Successfully saved data to {filepath}")

def main() -> None:
    """Main execution function to generate and save all data."""
    try:
        ensure_directories_exist()
        
        customers = generate_customers(config.NUM_CUSTOMERS)
        products = generate_products(config.NUM_PRODUCTS)
        orders = generate_orders(config.NUM_ORDERS, config.NUM_CUSTOMERS, config.NUM_PRODUCTS)

        save_to_csv(customers, str(config.CUSTOMERS_FILE))
        save_to_csv(products, str(config.PRODUCTS_FILE))
        save_to_csv(orders, str(config.ORDERS_FILE))
        
        logger.info("Data generation pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()