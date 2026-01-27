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

def ingest_vanguard_isa_cash_statement():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "vanguard_isa_cash_statement.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "vanguard_isa_cash_statement.parquet"

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
                    "Details": pl.String,
                    "Amount": pl.String,
                    "Balance": pl.String
                }
             )
            .filter(pl.col("Date").is_not_null())
            .with_columns([
                # strptime(..., '%d/%m/%Y')::DATE
                pl.col("Date").str.to_date("%d/%m/%Y").alias("transfer_date"),

                # TRIM("Details")
                pl.col("Details").str.strip_chars().alias("transfer_details_txt"),

                # regexp_replace(...) AS DECIMAL(18,4)
                pl.col("Amount")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("transfer_amount_gbp_num"),
                pl.col("Balance")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("account_balance_gbp_num"),
            ])
            .select(["transfer_date", "transfer_details_txt", "transfer_amount_gbp_num", "account_balance_gbp_num"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        print(f"Success: Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        print(f"Polars Error: {e}")
        sys.exit(2)


if __name__ == "__main__":
    ingest_vanguard_isa_cash_statement()