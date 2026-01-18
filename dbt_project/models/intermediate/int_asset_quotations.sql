select 
    {{ dbt_utils.generate_surrogate_key(['quotation_dt', 'isin']) }} as asset_quotation_id,
    quotation_dt,
    {{ dbt_utils.generate_surrogate_key(['isin']) }} as asset_id,
    isin,
    market_unit_price_gbp_num
from {{ ref('stg_asset_quotations') }}