with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'asset_quotations') }}
),

final as (
    select
        cast(quotation_date AS DATE) as quotation_dt,
        cast(asset_id AS INTEGER) as asset_id,
        cast(unit_market_value_gbp_num AS DECIMAL(18, 4)) as unit_market_value_gbp_num
    from source
)

select * from final
