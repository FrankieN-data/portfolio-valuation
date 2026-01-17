select 
    {{ dbt_utils.generate_surrogate_key(['isin']) }} as asset_id,
    *
from {{ ref('stg_dim_asset') }}