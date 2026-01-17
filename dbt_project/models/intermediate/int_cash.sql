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


all_cash as (
    select * from int_cash_equateplus
    union all
    select * from int_cash_oneview
    union all
    select * from int_cash_vanguard_isa
    union all
    select * from int_cash_vanguard_pension
)

select * from all_cash