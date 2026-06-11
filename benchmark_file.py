import os
import sys
import time
import tracemalloc
import logging
from pathlib import Path
import numpy as np
import pandas as pd
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HardwareFormatBenchmark:
    """
    Benchmarks different file storage formats (CSV, XLSX, Parquet, ORC, Feather)
    across hardware utilization metrics including I/O speed, RAM overhead, CPU time,
    and estimated energy consumption.
    """

    def __init__(self, num_rows: int = 100_000, output_dir: str = "data/benchmarks"):
        self.num_rows = num_rows
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.tdp_w = 65  # Assumed CPU Thermal Design Power in Watts (e.g., standard desktop/server core)

    def generate_base_dataframe(self) -> pd.DataFrame:
        """Generates a highly optimized baseline DataFrame with 500,000 rows."""
        logger.info(f"Generating optimized base DataFrame with {self.num_rows:,} rows...")
        
        # Vectorized generation for maximum efficiency
        categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
        
        df = pd.DataFrame({
            'id': np.arange(1, self.num_rows + 1),
            'name': [f"Customer_Name_{i}" for i in range(self.num_rows)],
            'email': [f"customer_email_{i}@example.com" for i in range(self.num_rows)],
            'amount': np.round(np.random.uniform(5.0, 1000.0, size=self.num_rows), 2),
            'date': pd.date_range(start='2026-01-01', periods=self.num_rows, freq='min'),
            'category': np.random.choice(categories, size=self.num_rows)
        })
        logger.info("Base DataFrame generation complete.")
        return df

    def _execute_with_metrics(self, operation_func, *args, **kwargs) -> tuple:
        """Executes a function while tracking execution time, CPU time, and peak memory."""
        tracemalloc.start()
        start_wall = time.time()
        start_cpu = time.process_time()

        operation_func(*args, **kwargs)

        end_cpu = time.process_time()
        end_wall = time.time()
        _, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        wall_time = end_wall - start_wall
        cpu_time = end_cpu - start_cpu
        peak_mem_mb = peak_memory / (1024 * 1024)

        return wall_time, cpu_time, peak_mem_mb

    def run_benchmark(self) -> List[Dict[str, Any]]:
        """Orchestrates writing and reading benchmarks for all configurations."""
        df = self.generate_base_dataframe()
        results = []

        # Formats configuration setup
        # Format label -> (extension, write_func, read_func)
        formats_config = {
            "CSV": (
                "csv", 
                lambda path: df.to_csv(path, index=False), 
                lambda path: pd.read_csv(path)
            ),
            "Parquet (PyArrow)": (
                "parquet", 
                lambda path: df.to_parquet(path, engine='pyarrow', index=False), 
                lambda path: pd.read_parquet(path, engine='pyarrow')
            ),
            "Parquet (FastParquet)": (
                "parquet", 
                lambda path: df.to_parquet(path, engine='fastparquet', index=False), 
                lambda path: pd.read_parquet(path, engine='fastparquet')
            ),
            "ORC": (
                "orc", 
                lambda path: df.to_orc(path), 
                lambda path: pd.read_orc(path)
            ),
            "Feather": (
                "feather", 
                lambda path: df.to_feather(path), 
                lambda path: pd.read_feather(path)
            ),
            "Excel (XLSX)": (
                "xlsx", 
                lambda path: df.to_excel(path, index=False, engine='openpyxl'), 
                lambda path: pd.read_excel(path, engine='openpyxl')
            )
        }

        for fmt_name, (ext, write_op, read_op) in formats_config.items():
            file_path = self.output_dir / f"benchmark_data_{fmt_name.replace(' ', '_').replace('(', '').replace(')', '')}.{ext}"
            logger.info(f"Running hardware benchmark for format: {fmt_name}...")

            try:
                # 1. Benchmark Writing Phase
                # Alert for Excel as it processes large records on a single thread
                if fmt_name == "Excel (XLSX)":
                    logger.warning(f"Writing to Excel with {self.num_rows:,} rows takes longer and consumes more RAM. Processing...")
                
                w_time, w_cpu, w_mem = self._execute_with_metrics(write_op, file_path)
                
                # 2. Extract File Size Metric
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

                # 3. Benchmark Reading Phase
                r_time, r_cpu, r_mem = self._execute_with_metrics(read_op, file_path)

                # 4. Total Resource Computations
                total_cpu_time = w_cpu + r_cpu
                peak_memory_mb = max(w_mem, r_mem)
                
                # Formula: (CPU Time in seconds * TDP in Watts) / 3600 seconds = Watt-hours
                energy_wh = (total_cpu_time * self.tdp_w) / 3600

                results.append({
                    "Format": fmt_name,
                    "File Size (MB)": file_size_mb,
                    "Write Time (s)": w_time,
                    "Read Time (s)": r_time,
                    "Peak Memory (MB)": peak_memory_mb,
                    "Total CPU Time (s)": total_cpu_time,
                    "Energy (Wh)": energy_wh
                })
                
                # Cleanup benchmark file to conserve local storage space
                if file_path.exists():
                    os.remove(file_path)

            except Exception as e:
                logger.error(f"Failed to benchmark format {fmt_name}: {e}")
                
        return results

    def display_metrics_table(self, results: List[Dict[str, Any]]) -> None:
        """Compiles benchmarks, calculates percentage savings vs CSV baseline, and logs output."""
        benchmark_df = pd.DataFrame(results)
        
        # Locate CSV metrics as our baseline dictionary
        csv_baseline = benchmark_df.set_index("Format").loc["CSV"].to_dict()

        savings_records = []
        for res in results:
            fmt = res["Format"]
            if fmt == "CSV":
                savings_records.append({
                    "Format": fmt, "Size Savings": "Baseline", "Write Savings": "Baseline",
                    "Read Savings": "Baseline", "Memory Savings": "Baseline", "Energy Savings": "Baseline"
                })
                continue
                
            # Calculation structure: ((CSV_Value - Target_Value) / CSV_Value) * 100
            size_sav = ((csv_baseline["File Size (MB)"] - res["File Size (MB)"]) / csv_baseline["File Size (MB)"]) * 100
            write_sav = ((csv_baseline["Write Time (s)"] - res["Write Time (s)"]) / csv_baseline["Write Time (s)"]) * 100
            read_sav = ((csv_baseline["Read Time (s)"] - res["Read Time (s)"]) / csv_baseline["Read Time (s)"]) * 100
            mem_sav = ((csv_baseline["Peak Memory (MB)"] - res["Peak Memory (MB)"]) / csv_baseline["Peak Memory (MB)"]) * 100
            energy_sav = ((csv_baseline["Energy (Wh)"] - res["Energy (Wh)"]) / csv_baseline["Energy (Wh)"]) * 100

            savings_records.append({
                "Format": fmt,
                "Size Savings": f"{size_sav:+.1f}%",
                "Write Savings": f"{write_sav:+.1f}%",
                "Read Savings": f"{read_sav:+.1f}%",
                "Memory Savings": f"{mem_sav:+.1f}%",
                "Energy Savings": f"{energy_sav:+.1f}%"
            })

        savings_df = pd.DataFrame(savings_records)
        final_report_df = pd.merge(benchmark_df, savings_df, on="Format")

        # Formatting values for clean output presentation
        print("\n" + "="*115)
        print("                        HARDWARE UTILITY & FORMAT PERFORMANCE REPORT")
        print("="*115)
        print(f"{'Format':<23} | {'Size (MB)':<9} | {'Write(s)':<8} | {'Read(s)':<7} | {'Peak RAM':<8} | {'CPU (s)':<7} | {'Energy(Wh)':<10}")
        print("-"*115)
        for _, row in final_report_df.iterrows():
            print(f"{row['Format']:<23} | {row['File Size (MB)']:<9.2f} | {row['Write Time (s)']:<8.3f} | {row['Read Time (s)']:<7.3f} | {row['Peak Memory (MB)']:<8.2f} | {row['Total CPU Time (s)']:<7.3f} | {row['Energy (Wh)']:<10.6f}")
        
        print("\n" + "="*115)
        print("                        EFFICIENCY GAINS / LOSSES (RELATIVE TO CSV BASELINE)")
        print("="*115)
        print(f"{'Format':<23} | {'Size Savings':<12} | {'Write Speedup':<13} | {'Read Speedup':<12} | {'RAM Savings':<11} | {'Energy Saved':<12}")
        print("-"*115)
        for _, row in final_report_df.iterrows():
            print(f"{row['Format']:<23} | {row['Size Savings']:<12} | {row['Write Savings']:<13} | {row['Read Savings']:<12} | {row['Memory Savings']:<11} | {row['Energy Savings']:<12}")
        print("="*115 + "\n")

if __name__ == "__main__":
    # Initialize the benchmark tracker with reduced rows for faster execution
    benchmarker = HardwareFormatBenchmark(num_rows=100_000)
    
    try:
        # Run pipeline process and log summaries
        print("\n" + "="*115)
        print("Starting Hardware Benchmark (100k rows)...")
        print("="*115 + "\n")
        
        raw_results = benchmarker.run_benchmark()
        
        if raw_results:
            benchmarker.display_metrics_table(raw_results)
        else:
            print("No benchmark results generated. Check logs for errors.")
            
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error in benchmark: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)