# GenAI PySpark E-Commerce Data Pipeline

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![PySpark 3.5+](https://img.shields.io/badge/PySpark-3.5%2B-success)](https://spark.apache.org/)

A production-grade, end-to-end data pipeline for generating synthetic e-commerce datasets and performing distributed analytics using Apache Spark. Designed for data engineering, ML pipeline development, and big data analytics demonstrations.

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Performance Benchmarking](#performance-benchmarking)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## ✨ Features

- **Synthetic Data Generation**: Generate realistic e-commerce datasets with 100K+ customers, 10K+ products, and 1M+ orders
- **Parquet-Optimized Storage**: All data stored in Parquet format for optimal compression (65%+ smaller than CSV)
- **Distributed Analytics**: Leverage Apache Spark for distributed computing across 8 cores
- **Business Insights**: 
  - Top N customers by revenue
  - Sales analysis by product category
  - Month-over-month revenue growth trends
- **Hardware Benchmarking**: Compare performance across CSV, Parquet, Excel, Feather, and ORC formats
- **Cross-Platform Support**: Works seamlessly on Windows, macOS, and Linux (including WSL)

---

## 🏗️ Architecture

```mermaid
graph TB
    subgraph "Data Generation Layer"
        DG["Synthetic Data Generator<br/>(Faker + NumPy)"]
        CUST["Customers Dataset<br/>100K rows"]
        PROD["Products Dataset<br/>10K rows"]
        ORD["Orders Dataset<br/>1M rows"]
    end
    
    subgraph "Storage Layer"
        PARQ["Parquet Files<br/>(Compressed)")
        RAW["data/raw/"]
        PROC["data/processed/"]
    end
    
    subgraph "Computation Layer"
        SPARK["Apache Spark<br/>(local[*])"]
        PANDAS["Pandas Intermediary<br/>(Path Handling)"]
    end
    
    subgraph "Analytics Layer"
        TOP_CUST["Top Customers<br/>by Revenue"]
        CATEGORY["Sales by<br/>Category"]
        TRENDS["Monthly<br/>Growth Trends"]
    end
    
    subgraph "Benchmarking Layer"
        BENCH["Hardware Performance<br/>Analyzer"]
        RESULTS["Format Comparison<br/>Results"]
    end
    
    DG --> CUST & PROD & ORD
    CUST & PROD & ORD --> PARQ
    PARQ --> RAW
    RAW --> PANDAS
    PANDAS --> SPARK
    SPARK --> TOP_CUST & CATEGORY & TRENDS
    SPARK --> BENCH
    BENCH --> RESULTS
    
    classDef generation fill:#e1f5ff
    classDef storage fill:#f3e5f5
    classDef compute fill:#fff3e0
    classDef analytics fill:#e8f5e9
    classDef bench fill:#fce4ec
    
    class DG,CUST,PROD,ORD generation
    class PARQ,RAW,PROC storage
    class SPARK,PANDAS compute
    class TOP_CUST,CATEGORY,TRENDS analytics
    class BENCH,RESULTS bench
```

---

## 📋 Prerequisites

- **Python**: 3.9 or higher
- **Java**: JDK 11 or higher (required for Spark)
- **RAM**: Minimum 8GB (16GB recommended)
- **Disk Space**: 2GB for generated data files
- **OS**: Windows (with WSL2), macOS, or Linux

### Verify Prerequisites

```bash
# Check Python version
python --version

# Check Java version
java -version

# Verify PySpark can initialize (after installation)
python -c "import pyspark; print(pyspark.__version__)"
```

---

## 🔧 Installation

### Step 1: Clone the Repository

```bash
cd ~/projects/360DigiTMG
git clone https://github.com/yourusername/genai-pyspark-pipeline.git
cd genai-pyspark-pipeline
```

### Step 2: Create Virtual Environment

**Using venv (Recommended):**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

**Using conda:**
```bash
conda create -n spark-pipeline python=3.10
conda activate spark-pipeline
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

**Key dependencies:**
- `pyspark>=3.5.0` - Distributed computing framework
- `pandas>=2.0.0` - Data manipulation
- `pyarrow>=15.0.0` - Parquet file handling
- `faker>=20.0.0` - Synthetic data generation
- `numpy>=1.24.0` - Numerical computing
- `tqdm>=4.65.0` - Progress bars

### Step 4: Verify Installation

```bash
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); print(f'Spark {spark.version} initialized')"
```

---

## 📁 Project Structure

```
genai-pyspark-pipeline/
├── README.md                          # Project documentation
├── LICENSE                            # MIT License
├── requirements.txt                   # Python dependencies
├── main.py                            # Data generation orchestrator
├── run_analytics.py                   # Analytics execution script
├── benchmark_file.py                  # Hardware performance benchmarking
│
├── src/
│   ├── __init__.py
│   ├── config.py                      # Configuration & path management
│   ├── data_generator.py              # Synthetic data generation (Faker + NumPy)
│   └── spark_analytics.py             # PySpark analytics engine
│
├── data/
│   ├── raw/                           # Generated parquet files
│   │   ├── customers.parquet
│   │   ├── products.parquet
│   │   └── orders.parquet
│   └── processed/                     # Processed outputs (future use)
│
└── notebooks/                         # Jupyter notebooks (optional)
```

---

## 🚀 Quick Start

### 1. Generate Synthetic Data

```bash
python main.py
```

**Expected Output:**
```
===================================================
Starting Synthetic Data Generation Pipeline
===================================================

Completed in 108.1 seconds
Files saved:
- customers.parquet (3.3 MB)
- products.parquet (253 KB)
- orders.parquet (9.9 MB)
```

### 2. Run Analytics Pipeline

```bash
python run_analytics.py
```

**Expected Output:**
```
Top 10 Customers by Revenue:
+-----------+---------------+
|customer_id|total_revenue  |
+-----------+---------------+
|82064      |$100,037,260.65|
|82172      |$20,390,705.06 |
...
+-----------+---------------+

Completed in 208.4 seconds
```

### 3. Benchmark File Formats

```bash
python benchmark_file.py
```

**Expected Output:**
```
===================================================
HARDWARE UTILITY & FORMAT PERFORMANCE REPORT
===================================================
Format          | Size (MB) | Write(s) | Read(s) | Energy(Wh)
Parquet (PyArrow) | 3.07    | 0.514    | 0.666   | 0.011285
Feather         | 3.84      | 0.304    | 0.427   | 0.006207
...
```

---

## 📖 Usage Examples

### Example 1: Generate Custom Dataset Size

**Edit `src/config.py`:**
```python
NUM_CUSTOMERS = 500_000    # Increase from 100K to 500K
NUM_PRODUCTS = 50_000      # Increase from 10K to 50K
NUM_ORDERS = 5_000_000     # Increase from 1M to 5M
```

**Run:**
```bash
python main.py
```

### Example 2: Custom Analytics Query

**Create `custom_analytics.py`:**
```python
from pyspark.sql import functions as F
from src.spark_analytics import SalesAnalytics
from src import config

analytics = SalesAnalytics()
analytics.create_spark_session()

# Load data
orders_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "orders.parquet"))
products_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "products.parquet"))

# Custom query: High-value customers (>$1M spend)
joined = orders_df.join(products_df, "product_id", "inner")
high_value = joined.groupBy("customer_id") \
    .agg(F.round(F.sum(F.col("quantity") * F.col("price")), 2).alias("total_spend")) \
    .filter(F.col("total_spend") > 1_000_000) \
    .orderBy(F.desc("total_spend"))

print(f"High-value customers (>$1M): {high_value.count()}")
high_value.show()

analytics.stop_session()
```

**Run:**
```bash
python custom_analytics.py
```

### Example 3: Load Data for Downstream Processing

```python
from src.spark_analytics import SalesAnalytics
from src import config

analytics = SalesAnalytics()
analytics.create_spark_session()

# Load as Spark DataFrames
customers_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "customers.parquet"))
orders_df = analytics.load_parquet(str(config.RAW_DATA_DIR / "orders.parquet"))

# Your custom transformations here
result = orders_df.groupBy("customer_id").count()

# Save results
result.coalesce(1).write.mode("overwrite").parquet("output/results.parquet")

analytics.stop_session()
```

---

## ⚙️ Configuration

All configuration is managed in `src/config.py`:

```python
# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Data generation parameters
NUM_CUSTOMERS = 100_000
NUM_PRODUCTS = 10_000
NUM_ORDERS = 1_000_000

# Spark settings
SPARK_MEMORY = "8g"
SPARK_CORES = 4

# File paths (Parquet format)
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.parquet"
PRODUCTS_FILE = RAW_DATA_DIR / "products.parquet"
ORDERS_FILE = RAW_DATA_DIR / "orders.parquet"
```

### Customizing Spark Configuration

**Edit `src/spark_analytics.py`:**
```python
def create_spark_session(self, app_name: str = "EcommerceSalesAnalytics") -> SparkSession:
    self.spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "16g")    # Increase from 4g
        .config("spark.executor.memory", "8g")   # Add executor config
        .config("spark.sql.shuffle.partitions", "200")  # Add partitioning
        .getOrCreate()
    return self.spark
```

---

## 📊 Performance Benchmarking

The `benchmark_file.py` script compares storage and computational efficiency:

### Run Benchmark:
```bash
python benchmark_file.py
```

### Key Metrics Tracked:
- **File Size**: Compression ratio vs baseline (CSV)
- **Write Time**: Time to serialize and write data
- **Read Time**: Time to deserialize and load data
- **Peak Memory**: RAM consumption during operations
- **CPU Time**: Processing time
- **Energy Consumption**: Estimated Watt-hours (TDP-based)

### Results Interpretation:
```
Parquet (PyArrow) Advantages:
  ✓ 65.8% smaller file size
  ✓ 91.1% faster writes
  ✓ 44.8% less RAM usage
  ✓ 88.2% energy savings
```

---

## 🐛 Troubleshooting

### Issue: "Java not found" error

**Solution:**
```bash
# Install Java
# macOS:
brew install openjdk@11

# Ubuntu:
sudo apt-get install openjdk-11-jdk

# Verify installation
java -version
```

### Issue: "UnsupportedFileSystemException" when using Spark

**Cause:** Spark/Hadoop can't access WSL paths directly

**Solution:** The project handles this automatically via pandas intermediary in `load_parquet()`. If issues persist:
```bash
# Copy data to Windows native path
cp -r data/ C:/temp/data/
# Update config to use C:/temp/data/raw
```

### Issue: Out of memory errors

**Solution:** Adjust Spark configuration in `src/spark_analytics.py`:
```python
.config("spark.driver.memory", "16g")  # Increase driver memory
.config("spark.sql.shuffle.partitions", "100")  # Reduce partitions
```

### Issue: Slow performance on first run

**Note:** First-run setup includes:
- JVM initialization (~10s)
- Spark context setup (~5s)
- Data loading and caching (~20s)

Subsequent runs will be faster due to JVM reuse.

---

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

1. **Fork the repository**
   ```bash
   git clone https://github.com/yourusername/genai-pyspark-pipeline.git
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes** and test thoroughly
   ```bash
   python -m pytest tests/  # Once tests are added
   ```

4. **Commit with clear messages**
   ```bash
   git commit -m "feat: add new analytics feature"
   ```

5. **Push and create a Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt

# Install pre-commit hooks (optional)
pip install pre-commit
pre-commit install
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### What This Means:
- ✅ You can use this commercially
- ✅ You can modify and distribute
- ✅ You can private use
- ⚠️ You must include license notice
- ⚠️ No liability or warranty

```
MIT License

Copyright (c) 2026 360DigiTMG

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

---

## 📞 Support & Contact

- **Issues**: [GitHub Issues](https://github.com/yourusername/genai-pyspark-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/genai-pyspark-pipeline/discussions)
- **Email**: support@360digitag.com

---

## 🎯 Roadmap

- [ ] Add Jupyter Notebook examples
- [ ] Implement unit tests and CI/CD pipeline
- [ ] Add Delta Lake support
- [ ] Create Docker containerization
- [ ] Add streaming data ingestion
- [ ] Implement MLOps pipeline integration
- [ ] Add distributed computing on Kubernetes

---

## 👏 Acknowledgments

- Built with [Apache Spark](https://spark.apache.org/) - Unified Analytics Engine
- Data generation with [Faker](https://faker.readthedocs.io/) - Realistic synthetic data
- Storage format by [Apache Parquet](https://parquet.apache.org/) - Columnar data format

---

**Happy Data Engineering! 🚀**