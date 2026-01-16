with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'oneview_transactions') }}
),

string_cleaning as (
    select 
        *,

        -- The transfer type and subtypr can be decided at this point (SWITCH or DEPOSIT)
        case
            when upper(transaction_type_cd) LIKE '%CONTRIBUTION%' then 'BUY'
            when upper(transaction_type_cd) LIKE '%SWITCH IN%' then 'BUY'
            when upper(transaction_type_cd) LIKE '%SWITCH OUT%' then 'SELL'
            else 'UNDEFINED'
        end as order_type_cd,
        case
            when upper(transaction_type_cd) LIKE '%CONTRIBUTION%' then 'ASSET ACQUISITION'
            when upper(transaction_type_cd) LIKE '%SWITCH%' then transaction_type_cd
            else 'UNDEFINED'
        end as order_subtype_cd,     

        transaction_type_cd || ' ' || fund_name_txt as transaction_details
    from source
),

final as (
    select
        
        cast(trade_date as DATE) as transaction_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,       
        cast('00984275' as VARCHAR) as company_number_key,        
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_PENSION_TAX' as VARCHAR) as wrapper_key,
        cast(upper(fund_name_txt) as VARCHAR) as asset_local_name_txt,
        cast(upper(order_type_cd) as VARCHAR) as order_type_cd,
        cast(upper(order_subtype_cd) as VARCHAR) as order_subtype_cd,
        cast(upper(transaction_details) as VARCHAR) as transaction_details_txt,
        
        cast(trade_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(trade_price_gbp_num as DECIMAL(18,4)) as unit_price_gbp_num,
        cast(trade_quantity_num as DECIMAL(18,4)) * cast(trade_price_gbp_num as DECIMAL(18,4)) as extended_transaction_amount_gbp_num

    from string_cleaning
)

select * from final