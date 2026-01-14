with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'dim_asset') }}
),

cleaning as (
    select 
        asset_id,
        upper(trim(isin)) as isin,
        upper(trim(asset_name_txt)) as asset_name_txt,
        asset_shortname_txt,
        stock_market_name_txt,
        upper(trim(asset_class_cd)) as asset_class_cd,
        upper(trim(asset_type_cd)) as asset_type_cd,
        upper(trim(asset_income_treatment_cd)) as asset_income_treatment_cd,
        upper(trim(asset_base_currency_cd)) as asset_base_currency_cd
    from source
),

final as (
    select
        cast(isin AS VARCHAR) as asset_key,
        cast(asset_name_txt AS VARCHAR) as asset_name_txt,
        cast(asset_shortname_txt AS VARCHAR) as asset_display_name_txt,
        cast(stock_market_name_txt AS VARCHAR) as stock_market_name_txt,
        cast(asset_class_cd AS VARCHAR) as asset_class_cd,
        cast(asset_type_cd AS VARCHAR) as asset_type_cd,
        cast(asset_income_treatment_cd AS VARCHAR) as asset_income_treatment_cd,
        cast(asset_base_currency_cd AS CHAR(3)) as asset_base_currency_cd
    from cleaning
)

select * from final