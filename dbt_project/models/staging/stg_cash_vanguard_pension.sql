with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'vanguard_pension_cash') }}
),

string_cleaning as (
    select 
        *, 
        upper(trim(transfer_details_txt)) as clean_transfer_details_txt
    from source
),

final as (
    select
        
        cast(transfer_date as DATE) as transfer_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,                 
        cast('07243412' as VARCHAR) as company_number_key,       
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_SIPP_TAX' as VARCHAR) as wrapper_key,
        cast(clean_transfer_details_txt as VARCHAR) as transfer_details_txt,
        cast(transfer_amount_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num

    from string_cleaning
)

select * from final