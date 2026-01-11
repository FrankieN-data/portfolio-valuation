-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_vanguard_isa_transactions.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert vanguard_isa_transactions.csv → vanguard_isa_transactions.parquet (with explicit types)
-- trade_date: DATE
-- asset_name_txt: VARCHAR
-- trade_details_txt: VARCHAR
-- trade_quantity_num: DECIMAL
-- trade_unit_price_num: DECIMAL
-- trade_amount_gbp_num: DECIMAL
COPY (
  SELECT
    strptime("Date", '%d/%m/%Y')::DATE AS trade_date,
    TRIM("InvestmentName") AS asset_name_txt,
    TRIM("TransactionDetails") AS trade_details_txt,
    CAST(regexp_replace("Quantity", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS trade_quantity_num,
    CAST(regexp_replace("Price", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS trade_unit_price_num,
    CAST(regexp_replace("Cost", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS trade_amount_gbp_num
  FROM read_csv_auto(
    (getvariable('raw_path') || '/vanguard_isa_transactions.csv'),
    header = true,
    null_padding = true,
    types = {
      "Date": "VARCHAR", 
      "Quantity":  "VARCHAR",
      "Price": "VARCHAR",
      "Cost": "VARCHAR"
    }, 
    decimal_separator='.'   
  )
  WHERE "Date" IS NOT NULL 
) TO (getvariable('bronze_path') || '/vanguard_isa_transactions.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');