with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'transactions_equateplus') }}
),

string_cleaning as (
    select 
        *,
        regexp_replace(plan_name_txt, ' SIP$', '') as asset_name_txt, -- removing the account type 
        case 
            when contribution_type_cd = 'Purchase' then 'BUY'
            when contribution_type_cd = 'Dividend' then 'DIVIDEND'
            else 'OTHER'
        end as order_type_cd
    from source
),

math_calculation as (
    select
        *,
        cost_basis_gbp_num * allocated_quantity_num as transaction_amount_gbp_num
    from string_cleaning
),

final as (
    select
    -- 1. Standardize Names
        cast(allocation_date as DATE) as transaction_dt,
        'FRANCINE NZUZI' as user_fullname_txt,        
        'COMPUTERSHARE' as investment_platform_shortname_txt,
        UPPER(plan_name_txt) as account_name_txt,
        UPPER(asset_name_txt) as asset_name_txt,
        order_type_cd,
        instrument_cd as transaction_details,
        cast(allocated_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(cost_basis_gbp_num as DECIMAL(18,4)) as unit_price_gbp_num,
        cast(transaction_amount_gbp_num as DECIMAL(18,4)) as extended_transaction_amount_gbp_num

    from math_calculation
)

select * from final