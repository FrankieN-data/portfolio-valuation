with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'vanguard_isa_cash') }}
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
            when upper(transfer_details_txt) LIKE '%FEE%' then 'ACCOUNT FEE'
            when upper(transfer_details_txt) LIKE '%DEPOSIT%' then 'NET CONTRIBUTION'
            when upper(transfer_details_txt) LIKE '%BOUGHT%' then 'PURCHASE'
            when upper(transfer_details_txt) LIKE '%INTEREST%' then 'CASH ACCOUNT INTEREST'
            when upper(transfer_details_txt) LIKE '%DIVIDEND%' then 'DIVIDEND'
            when upper(transfer_details_txt) LIKE '%SELL%' then 'ACCOUNT FEE'
            when upper(transfer_details_txt) LIKE '%SOLD%' then 'ACCOUNT FEE'
            when upper(transfer_details_txt) LIKE '%WITHDRAWAL%' then 'PAYMENT'
            else 'UNDEFINED'
        end as transfer_subtype_cd
    from source
),

final as (
    select
        
        cast(transfer_date as DATE) as transfer_dt,
        cast('FRANCINE NZUZI' as VARCHAR) as user_fullname_txt,        
        cast(upper('Vanguard Asset Management Ltd') as VARCHAR) as investment_platform_shortname_txt,
        cast('SELF MANAGED ISA' as VARCHAR) as account_name_txt,
        cast(transfer_type_cd as VARCHAR) as transfer_type_cd,
        cast(transfer_subtype_cd as VARCHAR) as transfer_subtype_cd,
        cast(transfer_amount_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num

    from string_cleaning
)

select * from final