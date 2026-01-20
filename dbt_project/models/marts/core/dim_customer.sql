{{
  config(
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/gold/dim_customer.parquet' (FORMAT 'PARQUET')"
  )
}}

select
    customer_id,
    customer_fullname_txt as customer_name
from {{ ref('int_dim_customer') }}