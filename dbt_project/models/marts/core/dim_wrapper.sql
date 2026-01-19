select
    wrapper_id,
    wrapper_name_txt as wrapper_name,
    wrapper_type_cd as wrapper_type,
    wrapper_subtype_cd as wrapper_subtype,
    tax_regime_uk_cd as uk_tax_regime
from {{ ref('int_dim_wrapper') }}    