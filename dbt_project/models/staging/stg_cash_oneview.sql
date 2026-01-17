with source as (
    select * from {{ source('bronze_portfolio', 'oneview_transactions') }}
),

string_cleaning as (
    select 
        *,
        upper(trim(transaction_type_cd)) as transfer_type_cd
    from source
),

final as (
    select
        cast(trade_date as DATE) as transfer_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,        
        cast('00984275' as VARCHAR) as company_number_key,       
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_PENSION_TAX' as VARCHAR) as wrapper_key,
        cast(transfer_type_cd as VARCHAR) as transfer_type_cd,
        cast(trade_quantity_num as DECIMAL(18,4)) as market_unit_price_gbp_num,
        cast(trade_price_gbp_num as DECIMAL(18,4)) as quantity_held_num
    from string_cleaning
)

select * from final