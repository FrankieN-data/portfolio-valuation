with int_transactions_equateplus as (
    select * from {{ ref('int_transactions_equateplus') }}
), 

int_transactions_oneview as (
    select * from {{ ref('int_transactions_oneview') }}
), 

int_transactions_vanguard_isa as (
    select * from {{ ref('int_transactions_vanguard_isa') }}
), 

int_transactions_vanguard_pension as (
    select * from {{ ref('int_transactions_vanguard_pension') }}
), 

union_all_transactions as (
    select * from int_transactions_equateplus
    union all
    select * from int_transactions_oneview
    union all
    select * from int_transactions_vanguard_isa
    union all
    select * from int_transactions_vanguard_pension
),

final as (
    select
        t.transaction_dt,
        {{ dbt_utils.generate_surrogate_key(['t.customer_email_txt']) }} as customer_id,
        t.customer_email_txt,    
        {{ dbt_utils.generate_surrogate_key(['t.company_number_key', 't.company_number_system_cd']) }} as company_id,    
        t.company_number_key,        
        t.company_number_system_cd,
        {{ dbt_utils.generate_surrogate_key(['t.wrapper_key']) }} as wrapper_id,         
        t.wrapper_key,
        aln.asset_id,
        t.asset_local_name_txt,
        order_type_cd,
        order_subtype_cd,
        transaction_details_txt,
        quantity_held_num, 
        market_unit_price_gbp_num,
        extended_transaction_amount_gbp_num

    from union_all_transactions t

    left join {{ ref('int_asset_local_names') }} as aln
        on aln.asset_local_name_id = {{ 
            dbt_utils.generate_surrogate_key(['t.asset_local_name_txt', 't.company_number_key', 't.company_number_system_cd']) 
        }}
)

select * from final