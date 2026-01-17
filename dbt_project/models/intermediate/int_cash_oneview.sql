with decode_string as (
    select 
        *,
        case
            when transfer_type_cd LIKE 'SWITCH%' then 'SWITCH'
            when transfer_type_cd = 'CONTRIBUTION' then 'DEPOSIT'
            else 'UNDEFINED'
        end as new_transfer_type_cd,
    from {{ ref('stg_cash_oneview') }}
),

math_calculation as (
    select
        *,
        -- Calculation for the deposit amount. Employee is paying only a fourth of the monthly contribution.
        quantity_held_num * market_unit_price_gbp_num as transfer_amount_gbp_num,
        quantity_held_num * market_unit_price_gbp_num / 4 as net_contribution_amount_gbp_num,
        quantity_held_num * market_unit_price_gbp_num * 3 / 4 as employer_contribution_amount_gbp_num
    from decode_string
),

assign_net_contribution as (
    -- The table is queried twice, first table to calculate the workplace contribution
    select
        *,
        case
            when new_transfer_type_cd == 'SWITCH' then transfer_type_cd
            when new_transfer_type_cd == 'DEPOSIT' then 'EMPLOYER CONTRIBUTION'
            else 'UNDEFINED'
        end as transfer_subtype_cd,
        case
            when new_transfer_type_cd == 'SWITCH' then transfer_amount_gbp_num
            when new_transfer_type_cd == 'DEPOSIT' then employer_contribution_amount_gbp_num
            else 'UNDEFINED'
        end as transfer_amount_gbp_num
    from math_calculation
    union    
    select -- Second table to calculate the net contribution
        *,
        'NET CONTRIBUTION',
        net_contribution_amount_gbp_num
    from math_calculation
    where new_transfer_type_cd != 'SWITCH'
),

final as (
    select
        transfer_dt,
        customer_email_txt,        
        company_number_key,       
        company_number_system_cd,
        wrapper_key,
        new_transfer_type_cd as transfer_type_cd,
        transfer_subtype_cd,
        cast(transfer_amount_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num
    from assign_net_contribution
)

select * from final