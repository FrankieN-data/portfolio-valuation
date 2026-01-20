{{
  config(
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/gold/fct_asset_quotations.parquet' (FORMAT 'PARQUET')"
  )
}}

select
    asset_quotation_id,
    strftime(quotation_dt, '%Y%m%d')::INT as quotation_date_key,
    {{ dbt_utils.generate_surrogate_key(['isin']) }} as asset_id,
    market_unit_price_gbp_num as market_unit_price_gbp
from {{ ref('int_asset_quotations') }}