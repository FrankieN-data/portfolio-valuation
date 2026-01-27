import polars as pl
import os
import sys
from utils import get_config, get_smart_logger

# Return status code
# 0 - Success
# 1 - No file
# 2 - Error  

# Setup logging with a safety check for the argument
logger = get_smart_logger("RAW TO BRONZE")

def ingest_dim_customer():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "dim_customer.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "dim_customer.parquet"

    # Ensure output directory exists
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Polars: Ingesting {raw_file}...")

    if not os.path.exists(raw_file):
        logger.warning(f"Skip: {raw_file} not found.")
        sys.exit(1)

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
        logger.info(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        print(f"Polars Error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ingest_dim_customer()