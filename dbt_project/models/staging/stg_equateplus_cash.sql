with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'equateplus_transactions') }}
),

string_cleaning as (
    select 
        *,
        case 
            when contribution_type_cd = 'Purchase' then 'ASSET ACQUISITION'
            when contribution_type_cd = 'Dividend' then 'DIVIDEND'
            else 'OTHER'
        end as transfer_subtype_cd
    from source
),

math_calculation as (
    select
        *,
        cost_basis_gbp_num * allocated_quantity_num as extended_transfer_amount_gbp_num
    from string_cleaning
),

final as (
    select
        
        cast(allocation_date as DATE) as transfer_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,        
        cast('03015818' as VARCHAR) as company_number_key,
        cast('WRP_SIP_NO_TAX' as VARCHAR) as wrapper_key,
        cast('DEPOSIT' as VARCHAR) as transfer_type_cd,
        cast(upper(transfer_subtype_cd) as VARCHAR) as transfer_subtype_cd,
        cast(extended_transfer_amount_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num

    from math_calculation
)

select * from final