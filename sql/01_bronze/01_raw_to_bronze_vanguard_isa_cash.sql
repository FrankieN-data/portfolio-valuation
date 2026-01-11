-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_vanguard_isa_cash.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert vanguard_isa_cash.csv → vanguard_isa_cash.parquet (with explicit types)
-- transfer_date: DATE
-- transfer_details_txt: VARCHAR
-- transfer_amount_gbp_num: DECIMAL
-- account_balance_gbp_num: DECIMAL
COPY (
  SELECT
    strptime("Date", '%d/%m/%Y')::DATE AS transfer_date,
    TRIM("Details") AS transfer_details_txt,
    CAST(regexp_replace("Amount", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS transfer_amount_gbp_num,
    CAST(regexp_replace("Balance", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS account_balance_gbp_num
  FROM read_csv_auto(
    (getvariable('raw_path') || '/vanguard_isa_cash.csv'),
    header = true,
    null_padding = true,
    types = {
      "Date": "VARCHAR", 
      "Amount":  "VARCHAR",
      "Balance": "VARCHAR"
    }, 
    decimal_separator='.'   
  )
  WHERE "Date" IS NOT NULL 
) TO (getvariable('bronze_path') || '/vanguard_isa_cash.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');