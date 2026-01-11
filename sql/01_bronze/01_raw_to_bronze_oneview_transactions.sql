-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_oneview_transactions.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert oneview_transactions.csv → oneview_transactions.parquet (with explicit types)
-- trade_date: DATE
-- transaction_type_cd: VARCHAR
-- fund_name_txt: VARCHAR
-- trade_amount_gbp_num: DECIMAL
-- trade_quantity_num: DECIMAL
-- trade_price_gbp_num: DECIMAL
-- switch_key: INTEGER
COPY (
  SELECT
    strptime("Trade Date", '%d/%m/%Y')::DATE AS trade_date,
    TRIM("Transaction Type") AS transaction_type_cd,
    TRIM("Fund Name") AS fund_name_txt,
    CAST(regexp_replace("Value", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS trade_amount_gbp_num,
    CAST(regexp_replace("Traded Units", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS trade_quantity_num,
    CAST(regexp_replace("Trade Price", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS trade_price_gbp_num,
    CAST("Switch No." AS INTEGER) AS switch_key
  FROM read_csv_auto(
    (getvariable('raw_path') || '/oneview_transactions.csv'),
    header = true,
    null_padding = true,
    types = {
      "Trade Date": "VARCHAR", 
      "Value":  "VARCHAR",
      "Traded Units": "VARCHAR", 
      "Trade Price": "VARCHAR", 
      "Switch No.": "VARCHAR"
    }, 
    decimal_separator='.'  
  )
  WHERE "Trade Date" IS NOT NULL 
) TO (getvariable('bronze_path') || '/oneview_transactions.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');