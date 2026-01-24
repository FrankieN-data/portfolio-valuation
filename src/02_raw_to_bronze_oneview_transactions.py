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

def ingest_oneview_transactions():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "oneview_transactions.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "oneview_transactions.parquet"

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
                    "Trade Date": pl.String,
                    "Transaction Type": pl.String,
                    "Fund Name": pl.String,
                    "Value": pl.String,
                    "Traded Units": pl.String,
                    "Trade Price": pl.String,
                    "Switch No.": pl.String
                }
             )
            .filter(pl.col("Trade Date").is_not_null())
            .with_columns([
                # strptime(..., '%d/%m/%Y')::DATE
                pl.col("Trade Date").str.to_date("%d/%m/%Y").alias("trade_date"),

                # TRIM("Transaction Type")
                pl.col("Transaction Type").str.strip_chars().alias("transaction_type_cd"),

                # TRIM("Fund Name")
                pl.col("Fund name").str.strip_chars().alias("fund_name_txt"),

                # regexp_replace(...) AS DECIMAL(18,4)
                pl.col("Value")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("trade_amount_gbp_num"),
                pl.col("Traded Units")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("trade_quantity_num"),
                pl.col("Trade Price")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("trade_price_gbp_num"),
            ])
            .select(["trade_date", "transaction_type_cd", "fund_name_txt", "trade_amount_gbp_num", "trade_quantity_num", "trade_price_gbp_num"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        print(f"[SUCCESS] Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        print(f"Polars Error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ingest_oneview_transactions()