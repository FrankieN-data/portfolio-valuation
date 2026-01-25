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

def ingest_equateplus_statement():
    # 1. Setup Paths
    config = get_config()
    raw_file = config['paths']['raw'] / "equateplus_statement.csv"
    bronze_path = config['paths']['bronze']
    bronze_file = bronze_path / "equateplus_statement.parquet"

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
                    "Allocation date": pl.String,
                    "Plan": pl.String,
                    "Instrument type": pl.String,
                    "Instrument": pl.String,
                    "Contribution type": pl.String,
                    "Strike price / Cost basis": pl.String,
                    "Market price": pl.String,
                    "Available from": pl.String,
                    "Expiry date": pl.String,
                    "Allocated quantity": pl.String,
                    "Outstanding quantity": pl.String,
                    "Available quantity": pl.String,
                    "Estimated current outstanding value": pl.String,
                    "Estimated current available value": pl.String
                }
             )
            .filter(pl.col("Allocation date").is_not_null())
            .with_columns([
                # strptime(..., '%d/%m/%Y')::DATE
                pl.col("Allocation date").str.to_date("%d/%m/%Y").alias("allocation_date"),
                pl.col("Available from").str.to_date("%d/%m/%Y").alias("available_from_date"),
                pl.col("Expiry date").str.to_date("%d/%m/%Y").alias("expiry_date"),

                # TRIM("Plan")
                pl.col("Plan").str.strip_chars().alias("plan_name_txt"),
                pl.col("Instrument type").str.strip_chars().alias("instrument_type_cd"),
                pl.col("Instrument").str.strip_chars().alias("instrument_cd"),
                pl.col("Contribution type").str.strip_chars().alias("contribution_type_cd"),

                # regexp_replace(...) AS DECIMAL(18,4)
                pl.col("Strike price / Cost basis")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("cost_basis_gbp_num"),
                pl.col("Market price")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("unit_market_value_gbp_num"),
                pl.col("Allocated quantity")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("allocated_quantity_num"),
                pl.col("Outstanding quantity")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("outstanding_quantity_num"),
                pl.col("Available quantity")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("available_quantity_num"),
                pl.col("Estimated current outstanding value")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("estimated_current_outstanding_value_gbp_num"),
                pl.col("Estimated current available value")
                .str.replace_all(r"[^0-9.-]", "")
                .cast(pl.Decimal(precision=18, scale=4))
                .alias("estimated_current_available_value_gbp_num")
            ])
            .select(["allocation_date", "plan_name_txt", "instrument_type_cd", "instrument_cd", "contribution_type_cd", "cost_basis_gbp_num", "unit_market_value_gbp_num", "allocated_quantity_num", "outstanding_quantity_num", "available_quantity_num", "estimated_current_outstanding_value_gbp_num", "estimated_current_available_value_gbp_num"])
        )

        # 3. Write to Parquet
        df.write_parquet(bronze_file, compression="snappy")
        logger.info(f"Success: Created {bronze_file}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Polars Error: {e}")
        sys.exit(2)


if __name__ == "__main__":
    ingest_equateplus_statement()