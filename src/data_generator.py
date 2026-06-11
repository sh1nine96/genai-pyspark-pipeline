import logging
import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
from typing import Tuple, List

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SyntheticDataGenerator:
    """
    A class to generate synthetic e-commerce data using Faker, NumPy, and Pandas.
    Generates Customers, Products, and Orders with realistic statistical distributions.
    """

    def __init__(self, random_seed: int = 42):
        """
        Initializes the generator with a specific random seed for reproducibility.
        """
        self.fake = Faker()
        Faker.seed(random_seed)
        np.random.seed(random_seed)
        logger.info(f"SyntheticDataGenerator initialized with seed {random_seed}")

    def generate_customers(self, num_customers: int = 100_000) -> pd.DataFrame:
        """
        Generates customer data with ages following a normal distribution.
        """
        logger.info(f"Generating {num_customers:,} customers...")
        
        # 1. Generate text data using Faker with a progress bar
        names = []
        emails = []
        cities = []
        countries = []
        
        for _ in tqdm(range(num_customers), desc="Generating Customer Details"):
            names.append(self.fake.name())
            emails.append(self.fake.unique.email())
            cities.append(self.fake.city())
            countries.append(self.fake.country())

        # 2. Generate ages using NumPy (Normal distribution around 35)
        # Using scale=12 for a realistic spread, then clipping outliers to 18-90
        ages_raw = np.random.normal(loc=35, scale=12, size=num_customers)
        ages = np.clip(ages_raw, 18, 90).astype(int)

        # 3. Generate registration dates
        start_date = pd.to_datetime('2020-01-01')
        end_date = pd.to_datetime('today')
        days_range = (end_date - start_date).days
        random_days = np.random.randint(0, days_range, size=num_customers)
        reg_dates = start_date + pd.to_timedelta(random_days, unit='D')

        df = pd.DataFrame({
            'customer_id': np.arange(1, num_customers + 1),
            'name': names,
            'email': emails,
            'age': ages,
            'city': cities,
            'country': countries,
            'registration_date': reg_dates
        })
        
        logger.info("Customer generation complete.")
        return df

    def generate_products(self, num_products: int = 10_000) -> pd.DataFrame:
        """
        Generates product catalog data.
        """
        logger.info(f"Generating {num_products:,} products...")
        categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
        
        names = []
        for _ in tqdm(range(num_products), desc="Generating Product Names"):
            names.append(self.fake.word().capitalize() + " " + self.fake.word().capitalize())

        df = pd.DataFrame({
            'product_id': np.arange(1, num_products + 1),
            'name': names,
            'category': np.random.choice(categories, size=num_products),
            'price': np.round(np.random.uniform(10.0, 500.0, size=num_products), 2),
            'stock': np.random.randint(0, 1000, size=num_products),
            'rating': np.round(np.random.uniform(1.0, 5.0, size=num_products), 1)
        })
        
        logger.info("Product generation complete.")
        return df

    def generate_orders(
        self, 
        customer_ids: np.ndarray, 
        product_ids: np.ndarray, 
        num_orders: int = 1_000_000
    ) -> pd.DataFrame:
        """
        Generates orders using a Pareto distribution to simulate the 80/20 rule
        (20% of customers make 80% of the orders).
        """
        logger.info(f"Generating {num_orders:,} orders (Applying Pareto distribution)...")

        # 1. Generate Pareto probabilities for customers
        # Alpha ~ 1.16 approximates the 80/20 rule
        pareto_weights = np.random.pareto(a=1.16, size=len(customer_ids))
        probabilities = pareto_weights / np.sum(pareto_weights)

        # 2. Vectorized selection of data
        # We process this in chunks to avoid massive memory spikes and to show a progress bar
        chunk_size = 100_000
        num_chunks = (num_orders // chunk_size) + (1 if num_orders % chunk_size != 0 else 0)
        
        order_dfs = []
        
        start_date = pd.to_datetime('2022-01-01')
        end_date = pd.to_datetime('today')
        days_range = (end_date - start_date).days

        for i in tqdm(range(num_chunks), desc="Generating Orders (Chunked)"):
            current_chunk_size = min(chunk_size, num_orders - (i * chunk_size))
            
            chunk_customer_ids = np.random.choice(
                customer_ids, 
                size=current_chunk_size, 
                p=probabilities
            )
            chunk_product_ids = np.random.choice(product_ids, size=current_chunk_size)
            chunk_quantities = np.random.randint(1, 11, size=current_chunk_size)
            
            random_days = np.random.randint(0, days_range, size=current_chunk_size)
            chunk_dates = start_date + pd.to_timedelta(random_days, unit='D')
            
            chunk_df = pd.DataFrame({
                'customer_id': chunk_customer_ids,
                'product_id': chunk_product_ids,
                'quantity': chunk_quantities,
                'order_date': chunk_dates
            })
            order_dfs.append(chunk_df)

        final_orders_df = pd.concat(order_dfs, ignore_index=True)
        final_orders_df.insert(0, 'order_id', np.arange(1, num_orders + 1))
        
        logger.info("Order generation complete.")
        return final_orders_df

    def run_pipeline(
        self, 
        num_customers: int = 100_000, 
        num_products: int = 10_000, 
        num_orders: int = 1_000_000
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Executes the full generation pipeline and returns all three DataFrames.
        """
        logger.info("--- Starting Synthetic Data Generation Pipeline ---")
        
        customers_df = self.generate_customers(num_customers)
        products_df = self.generate_products(num_products)
        orders_df = self.generate_orders(
            customer_ids=customers_df['customer_id'].values,
            product_ids=products_df['product_id'].values,
            num_orders=num_orders
        )
        
        logger.info("--- Pipeline Completed Successfully ---")
        return customers_df, products_df, orders_df

if __name__ == "__main__":
    from src import config
    
    # Ensure directories exist (using the config from the previous step)
    config.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Initialize and run
    generator = SyntheticDataGenerator(random_seed=42)
    
    # Using the default requirements from the prompt
    customers, products, orders = generator.run_pipeline(
        num_customers=100_000, 
        num_products=10_000, 
        num_orders=1_000_000
    )
    
    # Save to CSV
    logger.info("Saving DataFrames to CSV...")
    customers.to_csv(config.CUSTOMERS_FILE, index=False)
    products.to_csv(config.PRODUCTS_FILE, index=False)
    orders.to_csv(config.ORDERS_FILE, index=False)
    logger.info("Files saved successfully.")