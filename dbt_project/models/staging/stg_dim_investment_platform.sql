with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'dim_investment_platform') }}
),

cleaning as (
    select 
        investment_platform_key,
        upper(trim(investment_platform_name_txt)) as investment_platform_name_txt,
        upper(trim(investment_platform_shortname_txt)) as investment_platform_shortname_txt
    from source
),

final as (
    select
        cast(investment_platform_key AS INTEGER) as investment_platform_key,
        cast(investment_platform_name_txt AS VARCHAR) as investment_platform_name_txt,
        cast(investment_platform_shortname_txt AS VARCHAR) as investment_platform_shortname_txt
    from cleaning
)

select * from final