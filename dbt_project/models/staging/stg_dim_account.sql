with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'dim_account') }}
),

cleaning as (
    select 
        account_key,
        upper(trim(account_name_txt)) as account_name_txt,
        upper(trim(account_type_cd)) as account_type_cd,
        upper(trim(account_subtype_cd)) as account_subtype_cd,
        upper(trim(tax_regime_uk_cd)) as tax_regime_uk_cd
    from source
),

final as (
    select
        cast(account_key AS INTEGER) as account_key,
        cast(account_name_txt AS VARCHAR) as account_name_txt,
        cast(account_type_cd AS VARCHAR) as account_type_cd,
        cast(account_subtype_cd AS VARCHAR) as account_subtype_cd,
        cast(tax_regime_uk_cd AS VARCHAR) as tax_regime_uk_cd
    from cleaning
)

select * from final
