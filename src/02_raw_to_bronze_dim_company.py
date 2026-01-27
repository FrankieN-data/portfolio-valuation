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

def ingest_dim_company():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "dim_company.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "dim_company.parquet"

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
                    "company_number_key": pl.String,
                    "company_number_system_cd": pl.String,
                    "company_country_cd": pl.String,
                    "firm_register_number": pl.String,
                    "company_name_txt": pl.String,
                    "company_shortname_txt": pl.String
                }
             )
            .filter(pl.col("company_number_key").is_not_null())
            .with_columns([
                # TRIM  applied to all text columns
                pl.col("company_number_key").str.strip_chars(), 
                pl.col("company_number_system_cd").str.strip_chars(),
                pl.col("company_country_cd").str.strip_chars(),
                pl.col("firm_register_number").str.strip_chars().alias("firm_register_number_key"),
                pl.col("company_name_txt").str.strip_chars(),
                pl.col("company_shortname_txt").str.strip_chars().alias("company_shortname_txt")
            ])
            .select(["company_number_key", "company_number_system_cd", "company_country_cd", "firm_register_number_key", "company_name_txt", "company_shortname_txt"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        logger.info(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Polars Error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ingest_dim_company()