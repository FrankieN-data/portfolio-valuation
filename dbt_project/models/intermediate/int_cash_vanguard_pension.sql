with string_decode as (
    select 
        *,
        case 
            when transfer_amount_gbp_num > 0 then 'DEPOSIT'
            when transfer_amount_gbp_num < 0 then 'WITHDRAWAL'
            else 'UNDEFINED'
        end as transfer_type_cd,
        case 
            when upper(transfer_details_txt) LIKE '%TRANSFER IN%' then 'TRANSFER IN'
            when upper(transfer_details_txt) LIKE '%BOUGHT%' then 'PURCHASE'
            when upper(transfer_details_txt) LIKE '%TAX RELIEF%' then 'TAX RELIEF'
            when upper(transfer_details_txt) LIKE '%PENSION CONTRIBUTION%' then 'NET CONTRIBUTION'
            when upper(transfer_details_txt) LIKE '%INTEREST%' then 'CASH ACCOUNT INTEREST'
            when upper(transfer_details_txt) LIKE '%FEE%' then 'ACCOUNT FEE'
            when upper(transfer_details_txt) LIKE '%SELL%' then 'ACCOUNT FEE'
            when upper(transfer_details_txt) LIKE '%SOLD%' then 'ACCOUNT FEE'
            when upper(transfer_details_txt) LIKE '%DIVIDEND%' then 'DIVIDEND'
            when upper(transfer_details_txt) LIKE '%WITHDRAWAL%' then 'PAYMENT'
            else 'UNDEFINED'
        end as transfer_subtype_cd
    from {{ ref('stg_cash_vanguard_pension') }}
),

final as (
    select
        
        transfer_dt,
        customer_email_txt,                 
        company_number_key,       
        company_number_system_cd,
        wrapper_key,
        transfer_type_cd,
        transfer_subtype_cd,
        transfer_amount_gbp_num

    from string_decode
)

select * from final