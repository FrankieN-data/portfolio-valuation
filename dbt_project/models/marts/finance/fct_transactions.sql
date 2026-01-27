{{
  config(
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/gold/fct_transactions.parquet' (FORMAT 'PARQUET')"
  )
}}

select
  strftime(transaction_dt, '%Y%m%d')::INT as transaction_date_key,
  customer_id,  
  company_id,    
  wrapper_id,  
  asset_id,
  event_categorisation_id,
  transaction_details_txt as transaction_details,
  quantity_held_num as quantity_held, 
  market_unit_price_gbp_num as market_unit_price_gbp,
  extended_transaction_amount_gbp_num as transaction_amount_gbp
from {{ ref('int_transactions') }}