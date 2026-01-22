import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_oneview_transactions():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'oneview_transactions.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'oneview_transactions.parquet')
    print(f"[ACTION] Polars: Ingesting {raw_file}...")

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
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_oneview_transactions()