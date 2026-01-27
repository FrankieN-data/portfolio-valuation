{{
  config(
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/gold/dim_assets.parquet' (FORMAT 'PARQUET')"
  )
}}

select 
    asset_id,
    isin,
    asset_name_txt as asset_name,
    asset_display_name_txt as asset_display_name,
    stock_market_name_txt as stock_exchange,
    asset_class_cd as asset_class,
    asset_type_cd as asset_type,
    asset_income_treatment_cd as income_treatment,
    asset_base_currency_cd as base_currency
from {{ ref('int_dim_asset') }}