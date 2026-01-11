-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_raw_to_bronze_dim_wrapper.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- Convert dim_wrapper.csv → dim_wrapper.parquet (with explicit types)
-- wrapper_key: VARCHAR
-- wrapper_name_txt: VARCHAR
-- wrapper_type_cd: VARCHAR
-- wrapper_subtype_cd: VARCHAR
-- tax_regime_uk_cd: VARCHAR
COPY (
  SELECT
    TRIM("wrapper_key") AS wrapper_key,
    TRIM("wrapper_name_txt") AS wrapper_name_txt,
    TRIM("wrapper_type_cd") AS wrapper_type_cd,
    TRIM("wrapper_subtype_cd") AS wrapper_subtype_cd,
    TRIM("tax_regime_uk_cd") AS tax_regime_uk_cd
  FROM read_csv_auto(
    (getvariable('raw_path') || '/dim_wrapper.csv'),
    header = true,
    null_padding = true
  )
  WHERE "wrapper_key" IS NOT NULL  
) TO (getvariable('bronze_path') || '/dim_wrapper.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');
