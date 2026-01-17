with string_decode as (
    select 
        *,
        case
            when transaction_type_cd = 'CONTRIBUTION' then 'BUY'
            when transaction_type_cd = 'SWITCH IN' then 'BUY'
            when transaction_type_cd = 'SWITCH OUT' then 'SELL'
            else 'UNDEFINED'
        end as order_type_cd,
        case
            when transaction_type_cd = 'CONTRIBUTION' then 'ASSET ACQUISITION'
            when transaction_type_cd LIKE 'SWITCH%' then transaction_type_cd
            else 'UNDEFINED'
        end as order_subtype_cd,     
        transaction_type_cd || ' ' || asset_local_name_txt as transaction_details_txt
    from {{ ref('stg_transactions_oneview')}}
),

math_calculation as (
    select 
        *,
        quantity_held_num * market_unit_price_gbp_num as extended_transaction_amount_gbp_num
    from string_decode
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