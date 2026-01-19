{{
  config(
    materialized='incremental',
    unique_key=['event_categorisation_type_cd', 'event_type_cd', 'event_subtype_cd']
  )
}}

with transactions_equateplus as (
    select distinct
        cast('ORDER' as varchar) as event_categorisation_type_cd,
        order_type_cd as event_type_cd,
        order_subtype_cd as event_subtype_cd
    from {{ ref('int_transactions_equateplus') }}
),

transactions_oneview as (
    select distinct
        cast('ORDER' as varchar) as event_categorisation_type_cd,
        order_type_cd as event_type_cd,
        order_subtype_cd
    from {{ ref('int_transactions_oneview') }}
),

transactions_vanguard_isa as (
    select distinct
        cast('ORDER' as varchar) as event_categorisation_type_cd,
        order_type_cd as event_type_cd,
        order_subtype_cd as event_subtype_cd
    from {{ ref('int_transactions_vanguard_isa') }}
),

transactions_vanguard_pension as (
    select distinct
        cast('ORDER' as varchar) as event_categorisation_type_cd,
        order_type_cd as event_type_cd,
        order_subtype_cd as event_subtype_cd
    from {{ ref('int_transactions_vanguard_pension') }}
),

cash_equateplus as (
    select distinct
        cast('TRANSFER' as varchar) as event_categorisation_type_cd,
        transfer_type_cd as event_type_cd,
        transfer_subtype_cd as event_subtype_cd
    from {{ ref('int_cash_equateplus') }}
),

cash_oneview as (
    select distinct
        cast('TRANSFER' as varchar) as event_categorisation_type_cd,
        transfer_type_cd as event_type_cd,
        transfer_subtype_cd as event_subtype_cd
    from {{ ref('int_cash_oneview') }}
),

cash_vanguard_isa as (
    select distinct
        cast('TRANSFER' as varchar) as event_categorisation_type_cd,
        transfer_type_cd as event_type_cd,
        transfer_subtype_cd as event_subtype_cd
    from {{ ref('int_cash_vanguard_isa') }}
),

cash_vanguard_pension as (
    select distinct
        cast('TRANSFER' as varchar) as event_categorisation_type_cd,
        transfer_type_cd as event_type_cd,
        transfer_subtype_cd as event_subtype_cd
    from {{ ref('int_cash_vanguard_pension') }}
),

all_event_categorisation as (
    select * from transactions_equateplus
    union
    select * from transactions_oneview
    union
    select * from transactions_vanguard_isa
    union
    select * from transactions_vanguard_pension
    union
    select * from cash_equateplus
    union
    select * from cash_oneview
    union
    select * from cash_vanguard_isa
    union
    select * from cash_vanguard_pension
),

final as (
    select 
        {{ dbt_utils.generate_surrogate_key(['event_categorisation_type_cd', 'event_type_cd', 'event_subtype_cd']) }} as event_categorisation_id,
        * 
    from all_event_categorisation evt
    
    {% if is_incremental() %}
        where not exists (
            select 1
            from {{ this }} cur
            where 
                evt.event_categorisation_type_cd = cur.event_categorisation_type_cd
                and evt.event_type_cd = cur.event_type_cd
                and evt.event_subtype_cd = cur.event_subtype_cd
        )
    {% endif %}
)

select * from final