select 
    {{ dbt_utils.generate_surrogate_key(['wrapper_key']) }} as wrapper_id,
    *
from {{ ref('stg_dim_wrapper') }}