select 
    {{ dbt_utils.generate_surrogate_key(['company_number_key', 'company_number_system_cd']) }} as company_id,
    *
from {{ ref('stg_dim_company') }}