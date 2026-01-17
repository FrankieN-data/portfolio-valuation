with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'equateplus_transactions') }}
),

string_cleaning as (
    select 
        *,
        upper(trim(plan_name_txt)) as clean_plan_name_txt
    from source
),

final as (
    select
        
        cast(allocation_date as DATE) as transaction_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,        
        cast('03015818' as VARCHAR) as company_number_key,        
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_MMC_SIP_NO_TAX' as VARCHAR) as wrapper_key,
        cast(clean_plan_name_txt as VARCHAR) as plan_name_txt,
        cast('BUY' as VARCHAR) as order_type_cd,
        cast ('ASSET ACQUISITION' as VARCHAR) as order_subtype_cd,
        cast(instrument_cd as VARCHAR) as transaction_details_txt,        
        cast(allocated_quantity_num as DECIMAL(18,4)) as quantity_held_num, 
        cast(cost_basis_gbp_num as DECIMAL(18,4)) as market_unit_price_gbp_num

    from string_cleaning
    where upper(contribution_type_cd) == 'PURCHASE'
)

select * from final