import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SalesAnalytics:
    """
    A PySpark analytics class to process and analyze e-commerce data.
    """

    def __init__(self):
        """Initializes the SalesAnalytics class."""
        self.spark: Optional[SparkSession] = None
        logger.info("SalesAnalytics initialized.")

    def create_spark_session(self, app_name: str = "EcommerceSalesAnalytics") -> SparkSession:
        """
        Configures and creates a SparkSession for local mode with specific 
        performance optimizations (4GB memory, AQE, Kryo Serialization).
        """
        logger.info("Configuring and creating SparkSession...")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        logger.info("SparkSession successfully created.")
        return self.spark

    def load_parquet(self, path: str) -> DataFrame:
        """
        Loads a Parquet file into a PySpark DataFrame.
        Uses pandas as an intermediary for WSL/Windows path compatibility.
        """
        if not self.spark:
            raise ValueError("SparkSession is not initialized. Call create_spark_session() first.")
        
        import pandas as pd
        from pathlib import Path
        
        file_path = Path(path)
        
        logger.info(f"Loading Parquet data from: {file_path}")
        
        # Load with pandas first (handles WSL paths better)
        # Then convert to Spark DataFrame
        pd_df = pd.read_parquet(file_path)
        spark_df = self.spark.createDataFrame(pd_df)
        
        return spark_df

    def top_customers_by_revenue(self, orders_df: DataFrame, products_df: DataFrame, n: int = 10) -> DataFrame:
        """
        Joins orders with products, calculates total spend per customer, 
        and returns the top N customers by revenue.
        """
        logger.info(f"Calculating top {n} customers by revenue...")
        
        # Join orders and products
        joined_df = orders_df.join(products_df, "product_id", "inner")
        
        # Calculate spend per order line
        spend_df = joined_df.withColumn("total_spend", F.col("quantity") * F.col("price"))
        
        # Aggregate by customer and get Top N
        top_customers = spend_df.groupBy("customer_id") \
            .agg(F.round(F.sum("total_spend"), 2).alias("total_revenue")) \
            .orderBy(F.desc("total_revenue")) \
            .limit(n)
            
        return top_customers

    def sales_by_category(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Groups data by product category, calculating total units sold and total revenue.
        """
        logger.info("Calculating sales and units sold by category...")
        
        joined_df = orders_df.join(products_df, "product_id", "inner")
        revenue_df = joined_df.withColumn("line_revenue", F.col("quantity") * F.col("price"))
        
        category_sales = revenue_df.groupBy("category") \
            .agg(
                F.sum("quantity").alias("total_units_sold"),
                F.round(F.sum("line_revenue"), 2).alias("total_revenue")
            ) \
            .orderBy(F.desc("total_revenue"))
            
        return category_sales

    def monthly_trends(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Calculates month-over-month revenue growth percentages using Window functions.
        """
        logger.info("Calculating month-over-month revenue trends...")
        
        # Join and calculate line revenue
        joined_df = orders_df.join(products_df, "product_id", "inner")
        revenue_df = joined_df.withColumn("line_revenue", F.col("quantity") * F.col("price"))
        
        # Extract Year-Month format (e.g., '2025-10')
        monthly_df = revenue_df.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
        
        # Aggregate total revenue per month
        monthly_revenue = monthly_df.groupBy("order_month") \
            .agg(F.sum("line_revenue").alias("current_month_revenue"))
            
        # Define Window specification ordered by the month string
        window_spec = Window.orderBy("order_month")
        
        # Use lag to get the previous month's revenue
        trends_df = monthly_revenue.withColumn(
            "prev_month_revenue", 
            F.lag("current_month_revenue", 1).over(window_spec)
        )
        
        # Calculate MoM Growth Percentage: ((Current - Previous) / Previous) * 100
        growth_df = trends_df.withColumn(
            "mom_growth_pct",
            F.round(
                ((F.col("current_month_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue")) * 100, 
                2
            )
        ).orderBy("order_month")
        
        # Clean up column rounding for presentation
        return growth_df.withColumn("current_month_revenue", F.round(F.col("current_month_revenue"), 2))

    def stop_session(self) -> None:
        """Stops the SparkSession safely."""
        if self.spark:
            logger.info("Stopping SparkSession...")
            self.spark.stop()
            self.spark = None


if __name__ == "__main__":
    from src import config
    
    # Initialize the analytics class
    analytics = SalesAnalytics()
    
    try:
        # 1. Create Spark Session
        analytics.create_spark_session()
        
        # 2. Load Parquet Data (Assumes the data generator has run)
        customers_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "customers.parquet"))
        products_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "products.parquet"))
        orders_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "orders.parquet"))
        
        # 3. Top Customers
        logger.info("\n--- Top 10 Customers by Revenue ---")
        top_customers = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
        top_customers.show()
        
        # 4. Sales by Category
        logger.info("\n--- Sales by Product Category ---")
        category_sales = analytics.sales_by_category(orders_df, products_df)
        category_sales.show()
        
        # 5. Monthly Trends (MoM Growth)
        logger.info("\n--- Month-Over-Month Revenue Trends ---")
        monthly_growth = analytics.monthly_trends(orders_df, products_df)
        monthly_growth.show(15)  # Show top 15 rows to see the trend
        
    except Exception as e:
        logger.error(f"Analytics Pipeline Failed: {e}", exc_info=True)
    finally:
        # Safely shut down Spark
        analytics.stop_session()