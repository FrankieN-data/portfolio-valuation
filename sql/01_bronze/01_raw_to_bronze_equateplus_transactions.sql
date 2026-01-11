-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_equateplus_transactions.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert equateplus_transactions.csv → equateplus_transactions.parquet (with explicit types)
-- allocation_date: DATE
-- plan_name_txt: VARCHAR
-- instrument_type_cd: VARCHAR
-- instrument_cd: VARCHAR
-- contribution_type_cd: VARCHAR
-- cost_basis_num: DECIMAL
-- unit_market_value_gbp_num: VARCHAR
-- available_from_date: DATE
-- expiry_date: DATE
-- allocated_quantity_num: DECIMAL
-- outstanding_quantity_num: DECIMAL
-- available_quantity_num: DECIMAL
-- estimated_current_outstanding_value_gbp_num: DECIMAL
-- estimated_current_available_value_gbp_num: DECIMAL
COPY (
  SELECT
    strptime("Allocation date", '%d/%m/%Y')::DATE AS allocation_date,
    TRIM("Plan") AS plan_name_txt,
    TRIM("Instrument type") AS instrument_type_cd,
    TRIM("Instrument") AS instrument_cd,
    TRIM("Contribution type") AS contribution_type_cd,
    CAST(regexp_replace("Strike price / Cost basis", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS cost_basis_gbp_num,
    CAST(regexp_replace("Market price", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS unit_market_value_gbp_num,
    strptime("Available from", '%d/%m/%Y')::DATE AS available_from_date,
    strptime("Expiry date", '%d/%m/%Y')::DATE AS expiry_date,
    CAST(regexp_replace("Allocated quantity", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS allocated_quantity_num,
    CAST(regexp_replace("Outstanding quantity", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS outstanding_quantity_num,
    CAST(regexp_replace("Available quantity", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS available_quantity_num,
    CAST(regexp_replace("Estimated current outstanding value", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS estimated_current_outstanding_value_gbp_num,
    CAST(regexp_replace("Estimated current available value", '[^0-9.-]', '', 'g') AS DECIMAL(18,4)) AS estimated_current_available_value_gbp_num
  FROM read_csv_auto(
    (getvariable('raw_path') || '/equateplus_transactions.csv'),
    header = true,
    null_padding = true,
    types = {
      "Allocation date": "VARCHAR", 
      "Strike price / Cost basis":  "VARCHAR",
      "Market price": "VARCHAR", 
      "Available from": "VARCHAR", 
      "Expiry date": "VARCHAR", 
      "Allocated quantity": "VARCHAR", 
      "Outstanding quantity": "VARCHAR", 
      "Available quantity": "VARCHAR", 
      "Estimated current outstanding value": "VARCHAR", 
      "Estimated current available value": "VARCHAR" 
    }, 
    decimal_separator='.'   
  )
  WHERE "Allocation date" IS NOT NULL 
) TO (getvariable('bronze_path') || '/equateplus_transactions.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');