with int_cash_equateplus as (
    select * from {{ ref('int_cash_equateplus') }}
), 

int_cash_oneview as (
    select * from {{ ref('int_cash_oneview') }}
), 

int_cash_vanguard_isa as (
    select * from {{ ref('int_cash_vanguard_isa') }}
), 

int_cash_vanguard_pension as (
    select * from {{ ref('int_cash_vanguard_pension') }}
), 

union_all_cash as (
    select * from int_cash_equateplus
    union all
    select * from int_cash_oneview
    union all
    select * from int_cash_vanguard_isa
    union all
    select * from int_cash_vanguard_pension
),

final as (
    select
        transfer_dt,
        {{ dbt_utils.generate_surrogate_key(['customer_email_txt']) }} as customer_id,
        customer_email_txt,    
        {{ dbt_utils.generate_surrogate_key(['company_number_key', 'company_number_system_cd']) }} as company_id,    
        company_number_key,        
        company_number_system_cd,
        {{ dbt_utils.generate_surrogate_key(['wrapper_key']) }} as wrapper_id,         
        wrapper_key,
        transfer_type_cd,
        transfer_subtype_cd,
        transfer_amount_gbp_num
    from union_all_cash
)

select * from final