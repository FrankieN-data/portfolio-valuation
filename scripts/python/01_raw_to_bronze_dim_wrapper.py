import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_dim_wrapper():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'dim_wrapper.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'dim_wrapper.parquet')

    print(f"[ACTION] Polars: Ingesting {raw_file}...")

    try:
        # 2. Read and Transform
        df = (
            pl.read_csv(
                raw_file, 
                has_header=True, 
                schema_overrides={
                    "wrapper_key": pl.String,
                    "wrapper_name_txt": pl.String,
                    "wrapper_type_cd": pl.String,
                    "wrapper_subtype_cd": pl.String,
                    "tax_regime_uk_cd": pl.String
                }
             )
            .filter(pl.col("wrapper_key").is_not_null())
            .with_columns([
                # TRIM
                pl.col("wrapper_key").str.strip_chars().alias("wrapper_key"),
                pl.col("wrapper_name_txt").str.strip_chars().alias("wrapper_name_txt"),
                pl.col("wrapper_type_cd").str.strip_chars().alias("wrapper_type_cd"),
                pl.col("wrapper_subtype_cd").str.strip_chars().alias("wrapper_subtype_cd"),
                pl.col("tax_regime_uk_cd").str.strip_chars().alias("tax_regime_uk_cd")
            ])
            .select(["wrapper_key", "wrapper_name_txt", "wrapper_type_cd", "wrapper_subtype_cd", "tax_regime_uk_cd"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_dim_wrapper()