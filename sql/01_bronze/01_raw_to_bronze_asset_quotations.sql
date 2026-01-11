-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_asset_quotations.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert asset_quotations.csv → asset_quotations.parquet (with explicit types)
-- quotation_date: DATE
-- asset_id: INTEGER
-- unit_market_value_gbp_num: VARCHAR
COPY (
  SELECT
    CAST("quote_date_dt" AS DATE) AS quotation_date, -- date format: YYYY-MM-DD
    CAST("fdasst_asset_id" AS INTEGER) AS asset_id,
    CAST(regexp_replace("asset_price_GBP_amt", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS unit_market_value_gbp_num
  FROM read_csv_auto(
    (getvariable('raw_path')) || '/asset_quotations.csv',
    header = true,
    null_padding = true,
    types = {
      "quote_date_dt": "VARCHAR", 
      "fdasst_asset_id": "VARCHAR", 
      "asset_price_GBP_amt": "VARCHAR"
    } 
  )
  WHERE "quote_date_dt" IS NOT NULL 
) TO (getvariable('bronze_path') || '/asset_quotations.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');