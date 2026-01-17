with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'vanguard_pension_transactions') }}
),

string_cleaning as (
    select 
        *,
        upper(trim(asset_name_txt)) as asset_local_name_txt,
        upper(trim(trade_details_txt)) as transaction_details_txt
    from source
),

final as (
    select
        
        cast(trade_date as DATE) as transaction_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,                 
        cast('07243412' as VARCHAR) as company_number_key,        
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_SIPP_TAX' as VARCHAR) as wrapper_key,
        cast(asset_local_name_txt as VARCHAR) as asset_local_name_txt,
        cast(trade_details_txt as VARCHAR) as transaction_details_txt,        
        cast(trade_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(trade_unit_price_num as DECIMAL(18,4)) as market_unit_price_gbp_num,
        cast(trade_amount_gbp_num as DECIMAL(18,4)) as extended_transaction_amount_gbp_num

    from string_cleaning
)

select * from final