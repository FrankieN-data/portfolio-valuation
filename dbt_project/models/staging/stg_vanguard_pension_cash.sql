with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'vanguard_pension_cash') }}
),

string_cleaning as (
    select 
        *,
        case 
            when cast(transfer_amount_gbp_num as DECIMAL(18,4)) > 0 then 'DEPOSIT'
            when cast(transfer_amount_gbp_num as DECIMAL(18,4)) < 0 then 'WITHDRAWAL'
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
    from source
),

final as (
    select
        
        cast(transfer_date as DATE) as transfer_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,                 
        cast('07243412' as VARCHAR) as company_number_key,
        cast('WRP_SIPP_TAX' as VARCHAR) as wrapper_key,
        cast(transfer_type_cd as VARCHAR) as transfer_type_cd,
        cast(transfer_subtype_cd as VARCHAR) as transfer_subtype_cd,
        cast(transfer_amount_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num

    from string_cleaning
)

select * from final