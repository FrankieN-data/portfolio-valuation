import polars as pl
import sys
import os
from utils import get_config, get_smart_logger

# Return status code
# 0 - Success
# 1 - Error 

# Setup logging with a safety check for the argument
logger = get_smart_logger("RAW TO BRONZE")

def ingest_asset_quotations():
    # 1. Setup Paths
    config = get_config()    
    raw_file = config['paths']['raw'] / "asset_quotations.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "asset_quotations.parquet"
    
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
                    "isin": pl.String,
                    "asset_price_GBP_amt": pl.String,
                    "quote_date_dt": pl.String
                }
             )
            .filter(pl.col("quote_date_dt").is_not_null())
            .with_columns([
                # strptime(..., '%d/%m/%Y')::DATE
                pl.col("quote_date_dt").str.to_date("%d/%m/%Y").alias("quotation_date"),
                
                # TRIM("isin")
                pl.col("isin").str.strip_chars(),
                
                # regexp_replace(...) AS DECIMAL(18,4)
                pl.col("asset_price_GBP_amt")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("unit_market_value_gbp_num")
            ])
            .select(["quotation_date", "isin", "unit_market_value_gbp_num"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        logger.info(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Polars Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    ingest_asset_quotations()