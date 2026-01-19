select 
    event_categorisation_id,
    event_categorisation_type_cd as event_categorisation_type,
    event_type_cd as event_type,
    event_subtype_cd as event_subtype
from {{ ref('int_dim_event_categorisation') }}