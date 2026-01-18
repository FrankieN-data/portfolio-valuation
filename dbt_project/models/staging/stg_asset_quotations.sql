with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'asset_quotations') }}
),

string_cleaning as (
    select
        *,
        upper(trim(isin)) as cleaned_isin
    from source

),

final as (
    select
        cast(quotation_date AS DATE) as quotation_dt,
        cast(cleaned_isin AS CHAR(12)) as isin,
        cast(unit_market_value_gbp_num AS DECIMAL(18, 4)) as market_unit_price_gbp_num
    from string_cleaning
)

select * from final
