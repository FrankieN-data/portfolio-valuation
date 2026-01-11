with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'dim_wrapper') }}
),

cleaning as (
    select 
        trim(wrapper_key) as wrapper_key,
        trim(wrapper_name_txt) as wrapper_name_txt,
        trim(wrapper_type_cd) as wrapper_type_cd,
        trim(wrapper_subtype_cd) as wrapper_subtype_cd,
        trim(tax_regime_uk_cd) as tax_regime_uk_cd
    from source
),

final as (
    select
        cast(upper(wrapper_key) AS VARCHAR) as wrapper_key,
        cast(upper(wrapper_name_txt) AS VARCHAR) as wrapper_name_txt,
        cast(upper(wrapper_type_cd) AS VARCHAR) as wrapper_type_cd,
        cast(upper(wrapper_subtype_cd) AS VARCHAR) as wrapper_subtype_cd,
        cast(upper(tax_regime_uk_cd) AS VARCHAR) as tax_regime_uk_cd
    from cleaning
)

select * from final
