import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_vanguard_isa_cash():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'vanguard_isa_cash.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'vanguard_isa_cash.parquet')
    print(f"[ACTION] Polars: Ingesting {raw_file}...")

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
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_vanguard_isa_cash()