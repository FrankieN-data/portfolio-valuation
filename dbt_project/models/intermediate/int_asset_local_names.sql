{{
  config(
    materialized='incremental',
    unique_key=['asset_local_name_txt', 'company_number_key', 'company_number_system_cd']
  )
}}

with dim_asset as (
    select * from {{ ref('int_dim_asset') }}
),

asset_local_name_equateplus as (
    select distinct
        asset_local_name_txt,
        company_number_key,
        company_number_system_cd
    from {{ ref('int_transactions_equateplus') }}
),

asset_local_name_oneview as (
    select distinct
        asset_local_name_txt,
        company_number_key,
        company_number_system_cd
    from {{ ref('int_transactions_oneview') }}
),

asset_local_name_vanguard_isa as (
    select distinct
        asset_local_name_txt,
        company_number_key,
        company_number_system_cd
    from {{ ref('int_transactions_vanguard_isa') }}
),

asset_local_name_vanguard_pension as (
    select distinct
        asset_local_name_txt,
        company_number_key,
        company_number_system_cd
    from {{ ref('int_transactions_vanguard_pension') }}
),

all_asset_local_names as (
    select * from asset_local_name_equateplus
    union
    select * from asset_local_name_oneview
    union
    select * from asset_local_name_vanguard_isa
    union
    select * from asset_local_name_vanguard_pension
),

asset_local_names as (
    select     
        {{ dbt_utils.generate_surrogate_key(
            ['aln.asset_local_name_txt', 'aln.company_number_key', 'aln.company_number_system_cd']
        ) }} as asset_local_name_id,
        a_messy_match.asset_id as asset_id,
        aln.asset_local_name_txt,
        aln.company_number_key,
        aln.company_number_system_cd

    from all_asset_local_names aln

    left join dim_asset a_exact_match
        on aln.asset_local_name_txt = a_exact_match.asset_name_txt

    left join dim_asset a_messy_match
        on jaccard(aln.asset_local_name_txt, a_messy_match.asset_name_txt) > 0.8

    -- Ignore messy matches already in the asset_local_names table
    {% if is_incremental() %}
        left join {{ this }} current
            on aln.asset_local_name_txt = current.asset_local_name_txt
            and aln.company_number_key = current.company_number_key
            and aln.company_number_system_cd = current.company_number_system_cd
    {% endif %}

    where a_exact_match.asset_id is null -- Ignore exact matches

    {% if is_incremental() %}
        and current.asset_local_name_id is null
    {% endif %}

    qualify row_number() over (
        partition by aln.asset_local_name_txt, aln.company_number_key, aln.company_number_system_cd 
        order by jaccard(aln.asset_local_name_txt, a_messy_match.asset_name_txt) desc
    ) = 1
)

select * from asset_local_names
