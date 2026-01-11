-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_dim_asset.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';

-- Convert dim_asset.csv → dim_asset.parquet (with explicit types)
-- asset_id: INTEGER
-- isin: VARCHAR
-- asset_name_txt: VARCHAR
-- asset_shortname_txt: VARCHAR
-- stock_market_name_txt: VARCHAR
-- asset_type_cd: VARCHAR
-- asset_income_treatment_cd: VARCHAR
-- asset_base_currency_cd: CHAR(3)
COPY (
  SELECT
    CAST("fdasst_asset_id" AS INTEGER) AS asset_id,
    TRIM("local_asset_id") AS isin,
    TRIM("asset_nm") AS asset_name_txt,
    TRIM("asset_short_nm") AS asset_shortname_txt,
    TRIM("stock_market_nm") AS stock_market_name_txt,
    TRIM("asset_type_nm") AS asset_type_cd,
    TRIM("asset_income_treatment_nm") AS asset_income_treatment_cd,
    TRIM("asset_base_currency_cd") AS asset_base_currency_cd
  FROM read_csv_auto(
    (getvariable('raw_path') || '/dim_asset.csv'),
    header = true,
    null_padding = true,
    types = {"fdasst_asset_id": "VARCHAR"}
  )
  WHERE "fdasst_asset_id" IS NOT NULL  
) TO (getvariable('bronze_path') || '/dim_asset.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');