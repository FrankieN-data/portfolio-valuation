with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'vanguard_pension_transactions') }}
),

string_cleaning as (
    select 
        *,
        case 
            when cast(trade_amount_gbp_num as DECIMAL(18,4)) > 0 then 'BUY'
            when cast(trade_amount_gbp_num as DECIMAL(18,4)) < 0 then 'SELL'
            else 'UNDEFINED'
        end as order_type_cd,
        case 
            when upper(trade_details_txt) LIKE '%FEE%' then 'ACCOUNT FEE'
            when upper(trade_details_txt) LIKE '%SELL%' then 'ACCOUNT FEE'
            when upper(trade_details_txt) LIKE '%SOLD%' then 'ACCOUNT FEE'
            when upper(trade_details_txt) LIKE '%BOUGHT%' then 'ASSET ACQUISITION'
            else 'UNDEFINED'
        end as order_subtype_cd        
    from source
),

final as (
    select
        
        cast(trade_date as DATE) as transaction_dt,

        cast('FRANCINE NZUZI' as VARCHAR) as user_fullname_txt,        
        cast ('VANGUARD ASSET MANAGEMENT LTD' as VARCHAR) as investment_platform_shortname_txt,
        cast(upper('Personal Pension') as VARCHAR) as account_name_txt,
        cast(upper(asset_name_txt) as VARCHAR) as asset_name_txt,
        cast(order_type_cd as VARCHAR) as order_type_cd,
        cast(order_subtype_cd as VARCHAR) as order_subtype_cd,
        cast(trade_details_txt as VARCHAR) as transaction_details_txt,
        
        cast(trade_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(trade_unit_price_num as DECIMAL(18,4)) as unit_price_gbp_num,
        cast(trade_amount_gbp_num as DECIMAL(18,4)) as extended_transaction_amount_gbp_num

    from string_cleaning
)

select * from final