import polars as pl
import os

import os

# This finds the directory where run_pipeline.py actually lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def ingest_dim_company():
    # 1. Setup Paths
    base_path = os.path.join(SCRIPT_DIR, '../..')
    raw_file = os.path.join(base_path, 'data', 'raw', 'dim_company.csv')
    bronze_file = os.path.join(base_path, 'data', 'bronze', 'dim_company.parquet')
    print(f"[ACTION] Polars: Ingesting {raw_file}...")

    try:
        # 2. Read and Transform
        df = (
            pl.read_csv(
                raw_file, 
                has_header=True, 
                schema_overrides={
                    "company_number_key": pl.String,
                    "company_number_system_cd": pl.String,
                    "company_country_cd": pl.String,
                    "firm_register_number": pl.String,
                    "company_name_txt": pl.String,
                    "company_shortname_txt": pl.String
                }
             )
            .filter(pl.col("company_number_key").is_not_null())
            .with_columns([
                # TRIM  applied to all text columns
                pl.col("company_number_key").str.strip_chars(), 
                pl.col("company_number_system_cd").str.strip_chars(),
                pl.col("company_country_cd").str.strip_chars(),
                pl.col("firm_register_number").str.strip_chars().alias("firm_register_number_key"),
                pl.col("company_name_txt").str.strip_chars(),
                pl.col("company_shortname_txt").str.strip_chars().alias("company_shortname_txt")
            ])
            .select(["company_number_key", "company_number_system_cd", "company_country_cd", "firm_register_number_key", "company_name_txt", "company_shortname_txt"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        print(f"Success: Created {bronze_file}")
        return True

    except Exception as e:
        print(f"Polars Error: {e}")
        return False

if __name__ == "__main__":
    ingest_dim_company()