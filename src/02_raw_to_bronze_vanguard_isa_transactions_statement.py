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

def ingest_vanguard_isa_transactions_statement():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "vanguard_isa_transactions_statement.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "vanguard_isa_transactions_statement.parquet"

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
                    "Date": pl.String,
                    "InvestmentName": pl.String,
                    "TransactionDetails": pl.String,
                    "Quantity": pl.String,
                    "Price": pl.String,
                    "Cost": pl.String
                }
             )
            .filter(pl.col("Date").is_not_null())
            .with_columns([
                # strptime(..., '%d/%m/%Y')::DATE
                pl.col("Date").str.to_date("%d/%m/%Y").alias("trade_date"),

                # TRIM("InvestmentName")
                pl.col("InvestmentName").str.strip_chars().alias("asset_name_txt"),
                pl.col("TransactionDetails").str.strip_chars().alias("trade_details_txt"),

                # regexp_replace(...) AS DECIMAL(18,4)
                pl.col("Quantity")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("trade_quantity_num"),
                pl.col("Price")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("trade_unit_price_num"),
                pl.col("Cost")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("trade_amount_gbp_num"),
            ])
            .select(["trade_date", "asset_name_txt", "trade_details_txt", "trade_quantity_num", "trade_unit_price_num", "trade_amount_gbp_num"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        logger.info(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Polars Error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ingest_vanguard_isa_transactions_statement()