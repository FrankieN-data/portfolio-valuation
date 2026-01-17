select 
    {{ dbt_utils.generate_surrogate_key(['customer_email_txt']) }} as customer_id,
    *
from {{ ref('stg_dim_customer') }}