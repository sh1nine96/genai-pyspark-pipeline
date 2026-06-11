import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from src.data_generator import SyntheticDataGenerator
from src import config


def get_file_size_mb(file_path: Path) -> float:
    """
    Get file size in MB.
    """
    if file_path.exists():
        return file_path.stat().st_size / (1024 * 1024)
    return 0.0


def main():
    """
    Main execution pipeline:
    1. Generates synthetic e-commerce data (customers, products, orders)
    2. Saves data as Parquet files
    3. Prints generation time and file sizes
    """
    try:
        # Start timer
        pipeline_start_time = time.time()
        
        # Ensure output directories exist
        config.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        config.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # Initialize data generator
        generator = SyntheticDataGenerator(random_seed=42)
        
        # Generate data
        customers_df, products_df, orders_df = generator.run_pipeline(
            num_customers=config.NUM_CUSTOMERS,
            num_products=config.NUM_PRODUCTS,
            num_orders=config.NUM_ORDERS
        )
        
        # Define Parquet file paths
        customers_parquet = config.RAW_DATA_DIR / "customers.parquet"
        products_parquet = config.RAW_DATA_DIR / "products.parquet"
        orders_parquet = config.RAW_DATA_DIR / "orders.parquet"
        
        # Save to Parquet format
        customers_df.to_parquet(customers_parquet, index=False, compression='snappy')
        products_df.to_parquet(products_parquet, index=False, compression='snappy')
        orders_df.to_parquet(orders_parquet, index=False, compression='snappy')
        
        # Calculate total execution time
        total_time = time.time() - pipeline_start_time
        
        # Get file sizes
        customers_size = get_file_size_mb(customers_parquet)
        products_size = get_file_size_mb(products_parquet)
        orders_size = get_file_size_mb(orders_parquet)
        
        # Print summary output
        print(f"\nCompleted in {total_time:.1f} seconds")
        print("Files saved:")
        
        # Format file sizes appropriately
        customers_display = f"{customers_size:.1f} MB" if customers_size >= 1 else f"{customers_size * 1024:.0f} KB"
        products_display = f"{products_size:.1f} MB" if products_size >= 1 else f"{products_size * 1024:.0f} KB"
        orders_display = f"{orders_size:.1f} MB" if orders_size >= 1 else f"{orders_size * 1024:.0f} KB"
        
        print(f"- customers.parquet ({customers_display})")
        print(f"- products.parquet ({products_display})")
        print(f"- orders.parquet ({orders_display})\n")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"ERROR: File not found - {e}")
        return 1
        
    except PermissionError as e:
        print(f"ERROR: Permission denied - {e}")
        return 1
        
    except ImportError as e:
        print(f"ERROR: Import error - {e}")
        print("Ensure all required dependencies are installed.")
        return 1
        
    except ValueError as e:
        print(f"ERROR: Value error during data generation - {e}")
        return 1
        
    except Exception as e:
        print(f"ERROR: Unexpected error occurred - {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
