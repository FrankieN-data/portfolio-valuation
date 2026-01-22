import polars as pl
import os

# Assuming SCRIPT_DIR is defined at the top of your orchestrator
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_dim_asset():
    # 1. Setup Paths (Anchored to Project Root)
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'dim_asset.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'dim_asset.parquet')

    print(f"[ACTION] Polars: Ingesting {raw_file}...")

    if not os.path.exists(raw_file):
        print(f"Skip: {raw_file} not found.")
        return False

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
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error on dim_asset: {e}")
        return False

if __name__ == "__main__":
    ingest_dim_asset()