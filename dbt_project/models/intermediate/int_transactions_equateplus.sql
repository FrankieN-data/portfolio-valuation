with string_replace as (
    select 
        *,
        regexp_replace(plan_name_txt, ' SIP$', '') as asset_local_name_txt -- removing the account type 
    from {{ ref('stg_transactions_equateplus') }}
),

math_calculation as (
    select
        *,
        quantity_held_num * market_unit_price_gbp_num as extended_transaction_amount_gbp_num
    from string_replace
),

final as (
    select        
        transaction_dt,
        customer_email_txt,        
        company_number_key,
        company_number_system_cd,
        wrapper_key,
        asset_local_name_txt,
        order_type_cd,
        order_subtype_cd,
        transaction_details_txt,
        quantity_held_num, 
        market_unit_price_gbp_num,
        extended_transaction_amount_gbp_num

    from math_calculation
)

select * from final