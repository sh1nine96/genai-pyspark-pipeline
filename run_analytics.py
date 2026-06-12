import time
from pyspark.sql import functions as F
from src.spark_analytics import SalesAnalytics
from src import config

# ANSI escape codes for terminal text colors
COLOR_GREEN = '\033[92m'
COLOR_BLUE = '\033[96m'
COLOR_RESET = '\033[0m'

def main() -> None:
    """
    Orchestrates the PySpark analytics pipeline, applying custom display
    formatting to match the target sample output.
    """
    # Start the total execution timer
    start_total_time = time.time()
    
    analytics = SalesAnalytics()
    
    try:
        # 1. Initialize Session and Suppress Noisy Logs
        analytics.create_spark_session()
        # Set log level to ERROR to keep the terminal output clean like the screenshot
        analytics.spark.sparkContext.setLogLevel("ERROR") 

        # 2. Load Datasets
        customers_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "customers.parquet"))
        products_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "products.parquet"))
        orders_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "orders.parquet"))
        
        # Cache DataFrames as they are used across multiple queries
        orders_df.cache()
        products_df.cache()

        print("\n") # Initial spacing

        # --- STAGE 1: Top 10 Customers ---
        print(f"{COLOR_GREEN}Top 10 Customers by Revenue:{COLOR_RESET}")
        top_customers = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
        
        # Format total_revenue to include dollar sign and comma separators (e.g., $15,432.50)
        top_customers_display = top_customers.withColumn(
            "total_revenue", F.format_string("$%,.2f", F.col("total_revenue"))
        )
        top_customers_display.show(truncate=False)
        print("...\n")

        # --- STAGE 2: Sales by Category ---
        print(f"{COLOR_GREEN}Sales by Category:{COLOR_RESET}")
        category_sales = analytics.sales_by_category(orders_df, products_df)
        
        # Format units_sold with commas, and convert revenue to Millions format (e.g., $2.4M)
        category_sales_display = category_sales.withColumn(
            "units_sold", F.format_string("%,d", F.col("total_units_sold"))
        ).withColumn(
            "revenue", F.concat(F.lit("$"), F.round(F.col("total_revenue") / 1000000, 1), F.lit("M"))
        ).select("category", "units_sold", "revenue") # Select specific column names from the image
        
        category_sales_display.show(truncate=False)
        print("...\n")

        # --- STAGE 3: Monthly Trends ---
        # (Keeping this as requested in your original prompt, cleanly formatted)
        print(f"{COLOR_GREEN}Monthly Trends (MoM Growth):{COLOR_RESET}")
        monthly_growth = analytics.monthly_trends(orders_df, products_df)
        
        monthly_growth_display = monthly_growth.withColumn(
            "current_month_revenue", F.format_string("$%,.2f", F.col("current_month_revenue"))
        ).withColumn(
            "prev_month_revenue", F.format_string("$%,.2f", F.col("prev_month_revenue"))
        ).withColumn(
            "mom_growth_pct", F.concat(F.col("mom_growth_pct").cast("string"), F.lit("%"))
        )
        
        monthly_growth_display.show(5, truncate=False)
        print("...\n")

        # --- EXECUTION FOOTER ---
        total_time = time.time() - start_total_time
        print(f"{COLOR_BLUE}Completed in {total_time:.1f} seconds{COLOR_RESET}\n")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        
    finally:
        analytics.stop_session()

if __name__ == "__main__":
    main()