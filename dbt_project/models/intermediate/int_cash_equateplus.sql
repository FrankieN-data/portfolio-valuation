with string_decode as (
    select 
        *,
        case 
            when transfer_subtype_cd = 'PURCHASE' then 'NET CONTRIBUTION'
            when transfer_subtype_cd = 'DIVIDEND' then 'DIVIDEND'
            else 'UNDEFINED'
        end as decode_transfer_subtype_cd
    from {{ ref('stg_cash_equateplus') }}
),

math_calculation as (
    select
        *,
        market_unit_price_gbp_num * quantity_held_num as transfer_amount_gbp_num
    from string_decode
),

final as (
    select
        transfer_dt,
        customer_email_txt,        
        company_number_key,        
        company_number_system_cd,
        wrapper_key,
        cast('DEPOSIT' as VARCHAR) as transfer_type_cd,
        decode_transfer_subtype_cd as transfer_subtype_cd,
        transfer_amount_gbp_num

    from math_calculation
)

select * from final