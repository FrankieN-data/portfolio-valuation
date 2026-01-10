-- 🚀 Convert raw CSVs → typed, documented Parquet (staging layer)
-- Run: duckdb -init 01_load_and_convert.sql


-- Define base paths as variables
SET variable onedrive_path = getenv('OneDrive');
SET variable base_path = getvariable('onedrive_path') || '/Projects/portfolio-valuation';
SET variable raw_path = getvariable('base_path') || '/data/raw';
SET variable bronze_path = getvariable('base_path') || '/data/bronze';


-- 1. Convert dim_account.csv → dim_account.parquet (with explicit types)
-- account_key: INTEGER
-- account_name_txt: VARCHAR
-- account_type_cd: VARCHAR
-- account_subtype_cd: VARCHAR
-- tax_regime_uk_cd: VARCHAR
COPY (
  SELECT
    CAST("account_key" AS INTEGER) AS account_key,
    TRIM("account_commercial_name_txt") AS account_name_txt,
    TRIM("account_type_cd") AS account_type_cd,
    TRIM("account_subtype_cd") AS account_subtype_cd,
    TRIM("tax_regime_uk_cd") AS tax_regime_uk_cd
  FROM read_csv_auto(
    (getvariable('raw_path') || '/dim_account.csv'),
    header = true,
    null_padding = true,
    types = {"account_key": "VARCHAR"}
  )
  WHERE "account_key" IS NOT NULL  
) TO (getvariable('bronze_path') || '/dim_account.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');


-- 2. Convert dim_asset.csv → dim_asset.parquet (with explicit types)
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


-- 3. Convert dim_customers.csv → dim_customers.parquet (with explicit types)
-- customer_key: INTEGER
-- customer_firstname: VARCHAR
-- customer_lastname: VARCHAR
COPY (
  SELECT
    CAST("customer_key" AS INTEGER) AS customer_key,
    TRIM("customer_firstname") AS customer_firstname_txt,
    TRIM("customer_lastname") AS customer_lastname_txt
  FROM read_csv_auto(
    (getvariable('raw_path') || '/dim_customer.csv'),
    header = true,
    null_padding = true,
    types = {"customer_key": "VARCHAR"}
  )
  WHERE "customer_key" IS NOT NULL  
) TO (getvariable('bronze_path') || '/dim_customer.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');


-- 4. Convert dim_investment_platform.csv → dim_investment_platform.parquet (with explicit types)
-- investment_platform_key: INTEGER
-- investment_platform_name_txt: VARCHAR
-- investment_platform_shortname_txt: VARCHAR
COPY (
  SELECT
    CAST("investment_platform_key" AS INTEGER) AS investment_platform_key,
    TRIM("investment_platform_commercial_name_txt") AS investment_platform_name_txt,
    TRIM("investment_platform_shortname_txt") AS investment_platform_shortname_txt
  FROM read_csv_auto(
    (getvariable('raw_path')) || '/dim_investment_platform.csv',
    header = true,
    null_padding = true,
    types = {"investment_platform_key": "VARCHAR"} 
  )
  WHERE "investment_platform_key" IS NOT NULL 
) TO (getvariable('bronze_path') || '/dim_investment_platform.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');


-- 5. Convert asset_quotations.csv → asset_quotations.parquet (with explicit types)
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


-- 6. Convert equateplus_transactions.csv → equateplus_transactions.parquet (with explicit types)
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


-- 7. Convert oneview_transactions.csv → oneview_transactions.parquet (with explicit types)
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


-- 8. Convert vanguard_isa_cash.csv → vanguard_isa_cash.parquet (with explicit types)
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


-- 9. Convert vanguard_pension_cash.csv → vanguard_pension_cash.parquet (with explicit types)
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
    (getvariable('raw_path') || '/vanguard_pension_cash.csv'),
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
) TO (getvariable('bronze_path') || '/vanguard_pension_cash.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');


-- 10. Convert vanguard_isa_transactions.csv → vanguard_isa_transactions.parquet (with explicit types)
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


-- 11. Convert vanguard_pension_transactions.csv → vanguard_pension_transactions.parquet (with explicit types)
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
    (getvariable('raw_path') || '/vanguard_pension_transactions.csv'),
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
) TO (getvariable('bronze_path') || '/vanguard_pension_transactions.parquet') (FORMAT PARQUET, COMPRESSION 'SNAPPY');