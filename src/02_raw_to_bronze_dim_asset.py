import polars as pl
import os
import sys
from utils import get_config, get_smart_logger

# Return status code
# 0 - Success
# 1 - No file
# 2 - Error  

# Setup logging with a safety check for the argument
logger = get_smart_logger(__name__)

def ingest_dim_asset():
    # 1. Setup Paths (Anchored to Project Root)
    config = get_config()
    raw_file = config['paths']['raw'] / "dim_asset.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "dim_asset.parquet"

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
                # Force IDs and Codes to Strings to prevent 'got f64' errors
                schema_overrides={
                    "fdasst_asset_id": pl.String,
                    "isin": pl.String,
                    "asset_base_currency_cd": pl.String
                }
            )
            # WHERE fdasst_asset_id IS NOT NULL
            .filter(pl.col("fdasst_asset_id").is_not_null())
            .with_columns([
                # CAST(id AS INTEGER)
                pl.col("fdasst_asset_id").cast(pl.Int32).alias("asset_id"),
                
                # TRIM applied to all text columns
                pl.col("isin").str.strip_chars(),
                pl.col("asset_nm").str.strip_chars().alias("asset_name_txt"),
                pl.col("asset_short_nm").str.strip_chars().alias("asset_shortname_txt"),
                pl.col("stock_market_nm").str.strip_chars().alias("stock_market_name_txt"),
                pl.col("asset_class_nm").str.strip_chars().alias("asset_class_cd"),
                pl.col("asset_type_nm").str.strip_chars().alias("asset_type_cd"),
                pl.col("asset_income_treatment_nm").str.strip_chars().alias("asset_income_treatment_cd"),
                pl.col("asset_base_currency_cd").str.strip_chars()
            ])
            # SELECT only the final renamed/transformed columns
            .select([
                "asset_id",
                "isin",
                "asset_name_txt",
                "asset_shortname_txt",
                "stock_market_name_txt",
                "asset_class_cd",
                "asset_type_cd",
                "asset_income_treatment_cd",
                "asset_base_currency_cd"
            ])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        logger.info(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Polars Error on dim_asset: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ingest_dim_asset()