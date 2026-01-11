-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_dim_customer.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert dim_customers.csv → dim_customers.parquet (with explicit types)
-- customer_email: VARCHAR
-- customer_firstname: VARCHAR
-- customer_lastname: VARCHAR
COPY (
  SELECT
    TRIM("customer_email_txt") AS customer_email_txt,
    TRIM("customer_firstname") AS customer_firstname_txt,
    TRIM("customer_lastname") AS customer_lastname_txt
  FROM read_csv_auto(
    (getvariable('raw_path') || '/dim_customer.csv'),
    header = true,
    null_padding = true
  )
  WHERE "customer_email_txt" IS NOT NULL  
) TO (getvariable('bronze_path') || '/dim_customer.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');