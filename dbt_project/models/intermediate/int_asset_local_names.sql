{{
  config(
    materialized='incremental',
    unique_key=['asset_local_name_txt', 'company_number_key', 'company_number_system_cd']
  )
}}

with dim_asset as (
    select * from {{ ref('int_dim_asset') }}
),

all_asset_local_names as (
    select distinct
        asset_local_name_txt, 
        company_number_key, 
        company_number_system_cd 
    from {{ ref('int_transactions_equateplus') }}
    union
    select distinct 
        asset_local_name_txt, 
        company_number_key, 
        company_number_system_cd 
    from {{ ref('int_transactions_oneview') }}
    union
    select distinct 
        asset_local_name_txt, 
        company_number_key, 
        company_number_system_cd 
    from {{ ref('int_transactions_vanguard_isa') }}
    union
    select distinct 
        asset_local_name_txt, 
        company_number_key, 
        company_number_system_cd 
    from {{ ref('int_transactions_vanguard_pension') }}
),

asset_local_names_matches as (
    select     
        {{ dbt_utils.generate_surrogate_key([
            'aln.asset_local_name_txt', 
            'aln.company_number_key', 
            'aln.company_number_system_cd'
        ]) }} as asset_local_name_id,
        ast.asset_id,
        jaro_winkler_similarity(aln.asset_local_name_txt, ast.asset_name_txt) as match_score,
        ast.asset_name_txt,
        aln.asset_local_name_txt,
        aln.company_number_key,
        aln.company_number_system_cd

    from all_asset_local_names aln
    cross join dim_asset ast

    where jaro_winkler_similarity(aln.asset_local_name_txt, ast.asset_name_txt) > 0.7

    -- This qualify ensures we only take the BEST match for each unique local asset string
    qualify row_number() over (
        partition by aln.asset_local_name_txt, aln.company_number_key, aln.company_number_system_cd 
        order by match_score desc
    ) = 1
)

select * from asset_local_names_matches

{% if is_incremental() %}
    -- Only insert names we haven't mapped yet
    where asset_local_name_id not in (select asset_local_name_id from {{ this }})
{% endif %}