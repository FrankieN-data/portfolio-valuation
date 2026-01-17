with string_decode as (
    select 
        *,
        case 
            when extended_transaction_amount_gbp_num > 0 then 'BUY'
            when extended_transaction_amount_gbp_num < 0 then 'SELL'
            else 'UNDEFINED'
        end as order_type_cd,
        case 
            when transaction_details_txt LIKE '%FEE%' then 'ACCOUNT FEE'
            when transaction_details_txt LIKE '%SELL%' then 'ACCOUNT FEE'
            when transaction_details_txt LIKE '%SOLD%' then 'ACCOUNT FEE'
            when transaction_details_txt LIKE '%BOUGHT%' then 'ASSET ACQUISITION'
            else 'UNDEFINED'
        end as order_subtype_cd        
    from {{ ref('stg_transactions_vanguard_isa') }}
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

    from string_decode
)

select * from final