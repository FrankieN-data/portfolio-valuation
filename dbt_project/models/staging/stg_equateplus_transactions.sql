with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'equateplus_transactions') }}
),

string_cleaning as (
    select 
        *,
        regexp_replace(plan_name_txt, ' SIP$', '') as asset_name_txt -- removing the account type 
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
        
        cast(allocation_date as DATE) as transaction_dt,

        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,        
        cast('03015818' as VARCHAR) as company_number_key,
        cast('WRP_SIP_NO_TAX' as VARCHAR) as wrapper_key,
        cast(upper(asset_name_txt) as VARCHAR) as asset_name_txt,
        cast('BUY' as VARCHAR) as order_type_cd,
        cast ('ASSET ACQUISITION' as VARCHAR) as order_subtype_cd,
        cast(instrument_cd as VARCHAR) as transaction_details_txt,
        
        cast(allocated_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(cost_basis_gbp_num as DECIMAL(18,4)) as unit_price_gbp_num,
        cast(transaction_amount_gbp_num as DECIMAL(18,4)) as extended_transaction_amount_gbp_num

    from math_calculation
    where contribution_type_cd == 'Purchase'
)

select * from final