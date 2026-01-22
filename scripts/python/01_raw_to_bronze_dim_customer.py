import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_dim_customer():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'dim_customer.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'dim_customer.parquet')
    print(f"[ACTION] Polars: Ingesting {raw_file}...")

    try:
        # 2. Read and Transform
        df = (
            pl.read_csv(
                raw_file, 
                has_header=True, 
                schema_overrides={
                    "customer_email_txt": pl.String,
                    "customer_firstname": pl.String,
                    "customer_lastname": pl.String
                }
             )
            .filter(pl.col("customer_email_txt").is_not_null())
            .with_columns([
                # TRIM
                pl.col("customer_email_txt").str.strip_chars(),
                pl.col("customer_firstname").str.strip_chars().alias("customer_firstname_txt"),
                pl.col("customer_lastname").str.strip_chars().alias("customer_lastname_txt")
            ])
            .select(["customer_email_txt", "customer_firstname_txt", "customer_lastname_txt"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_dim_customer()