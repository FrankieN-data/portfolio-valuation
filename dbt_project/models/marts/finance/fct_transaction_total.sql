{{
  config(
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/gold/fct_transaction_total.parquet' (FORMAT 'PARQUET')"
  )
}}


-- The grain : one row per customer, per wrapper, per asset, per day
with daily_summarised as (
    -- Step 1: Sum all transactions happening on the same day
    select
        transaction_date_key,
        customer_id,
        wrapper_id,
        asset_id,
        sum(quantity_held) as total_quantity_held,
        sum(transaction_amount_gbp) as daily_amount_gbp
    from {{ ref('fct_transactions') }}
    group by 
        transaction_date_key, 
        customer_id, 
        wrapper_id,
        asset_id
)

/*
running_total as (
    -- Step 2: Use the Window Function to add them up chronologically
    select
        transaction_date_key,
        customer_id,
        wrapper_id,
        daily_net_change_gbp,
        sum(daily_net_change_gbp) over (
            partition by customer_id, wrapper_id 
            order by transaction_date_key
            rows between unbounded preceding and current row
        ) as total_balance_gbp
    from daily_summarised
)
*/

select * from daily_summarised