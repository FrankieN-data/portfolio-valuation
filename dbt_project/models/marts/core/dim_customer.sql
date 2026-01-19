select
    customer_id,
    customer_fullname_txt as customer_name
from {{ ref('int_dim_customer') }}