import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_asset_quotations():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'asset_quotations.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'asset_quotations.parquet')

    print(f"[ACTION] Polars: Ingesting {raw_file}...")

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
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_asset_quotations()