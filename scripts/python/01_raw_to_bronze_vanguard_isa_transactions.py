import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_vanguard_isa_transactions():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'vanguard_isa_transactions.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'vanguard_isa_transactions.parquet')
    print(f"[ACTION] Polars: Ingesting {raw_file}...")

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
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_vanguard_isa_transactions()