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


all_transactions as (
    select * from int_transactions_equateplus
    union all
    select * from int_transactions_oneview
    union all
    select * from int_transactions_vanguard_isa
    union all
    select * from int_transactions_vanguard_pension
)

select * from all_transactions