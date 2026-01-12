with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'dim_company') }}
),

final as (
    select
        cast(upper(company_number_key) AS VARCHAR) as company_number_key,
        cast(upper(company_number_type_cd) AS VARCHAR) as company_number_type_cd,
        cast(upper(company_country_cd) AS CHAR(3)) as company_country_cd,
        cast(upper(firm_register_number_key) AS VARCHAR) as firm_register_number_key,
        cast(upper(company_name_txt) AS VARCHAR) as company_name_txt,
        cast(upper(company_shortname_txt) AS VARCHAR) as company_display_name_txt
    from source
)

select * from final