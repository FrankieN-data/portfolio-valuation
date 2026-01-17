with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'equateplus_transactions') }}
),

string_cleaning as (
    select 
        *,
        upper(trim(contribution_type_cd)) as transfer_subtype_cd
    from source
),

final as (
    select
        
        cast(allocation_date as DATE) as transfer_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,        
        cast('03015818' as VARCHAR) as company_number_key,        
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_MMC_SIP_TAX' as VARCHAR) as wrapper_key,
        cast(transfer_subtype_cd as VARCHAR) as transfer_subtype_cd,
        cast(cost_basis_gbp_num as DECIMAL(18,4)) as market_unit_price_gbp_num,
        cast(allocated_quantity_num as DECIMAL(18,4)) as quantity_held_num

    from string_cleaning
)

select * from final