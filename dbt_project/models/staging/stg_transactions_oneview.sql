with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'oneview_transactions') }}
),

string_cleaning as (
    select 
        *,
        upper(trim(fund_name_txt)) as asset_local_name_txt,
        upper(trim(transaction_type_cd)) as clean_transaction_type_cd
    from source
),

final as (
    select
        
        cast(trade_date as DATE) as transaction_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,       
        cast('00984275' as VARCHAR) as company_number_key,        
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_PENSION_TAX' as VARCHAR) as wrapper_key,
        cast(asset_local_name_txt as VARCHAR) as asset_local_name_txt,
        cast(clean_transaction_type_cd as VARCHAR) as transaction_type_cd,        
        cast(trade_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(trade_price_gbp_num as DECIMAL(18,4)) as market_unit_price_gbp_num,

    from string_cleaning
)

select * from final