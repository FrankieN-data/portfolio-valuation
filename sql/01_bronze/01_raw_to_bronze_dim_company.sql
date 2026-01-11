-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_dim_company.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert dim_company.csv → dim_company.parquet (with explicit types)
-- company_number_key: VARCHAR
-- company_number_type_cd: VARCHAR
-- company_country_cd: VARCHAR
-- firm_register_number_key: VARCHAR
-- company_name_txt: VARCHAR
-- company_shortname_txt: VARCHAR
COPY (
  SELECT
    TRIM("company_number_key")  AS company_number_key,
    TRIM("company_number_type_cd") AS company_number_type_cd,
    TRIM("company_country_cd") AS company_country_cd,
    TRIM("firm_register_number") AS firm_register_number_key,
    TRIM("company_name_txt") AS company_name_txt,
    TRIM("company_shortname_txt") AS company_shortname_txt
  FROM read_csv_auto(
    (getvariable('raw_path')) || '/dim_company.csv',
    header = true,
    null_padding = true,
    types = {"company_number_key": "VARCHAR", "firm_register_number": "VARCHAR"} 
  )
  WHERE "company_number_key" IS NOT NULL 
) TO (getvariable('bronze_path') || '/dim_company.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');