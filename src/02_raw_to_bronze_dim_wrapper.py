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

def ingest_dim_wrapper():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "dim_wrapper.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "dim_wrapper.parquet"

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
                    "wrapper_key": pl.String,
                    "wrapper_name_txt": pl.String,
                    "wrapper_type_cd": pl.String,
                    "wrapper_subtype_cd": pl.String,
                    "tax_regime_uk_cd": pl.String
                }
             )
            .filter(pl.col("wrapper_key").is_not_null())
            .with_columns([
                # TRIM
                pl.col("wrapper_key").str.strip_chars().alias("wrapper_key"),
                pl.col("wrapper_name_txt").str.strip_chars().alias("wrapper_name_txt"),
                pl.col("wrapper_type_cd").str.strip_chars().alias("wrapper_type_cd"),
                pl.col("wrapper_subtype_cd").str.strip_chars().alias("wrapper_subtype_cd"),
                pl.col("tax_regime_uk_cd").str.strip_chars().alias("tax_regime_uk_cd")
            ])
            .select(["wrapper_key", "wrapper_name_txt", "wrapper_type_cd", "wrapper_subtype_cd", "tax_regime_uk_cd"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        logger.info(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Polars Error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ingest_dim_wrapper()