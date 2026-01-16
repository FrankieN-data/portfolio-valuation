with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'oneview_transactions') }}
),

string_cleaning as (
    select 
        *,

        -- The transfert type can be decided at this point (SWITCH or DEPOSIT)
        case
            when upper(transaction_type_cd) LIKE '%SWITCH%' then 'SWITCH'
            when upper(transaction_type_cd) LIKE '%CONTRIBUTION%' then 'DEPOSIT'
            else 'UNDEFINED'
        end as transfer_type_cd,
    from source
),

math_calculation as (
    select
        *,

        -- Calculation for the deposit amount. Employee is paying only a fourth of the monthly contribution.
        cast(trade_quantity_num as DECIMAL(18,4)) * cast(trade_price_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num,
        cast(trade_quantity_num as DECIMAL(18,4)) * cast(trade_price_gbp_num as DECIMAL(18,4)) / 4 as net_contribution_amount_gbp_num,
        cast(trade_quantity_num as DECIMAL(18,4)) * cast(trade_price_gbp_num as DECIMAL(18,4)) * 3 / 4 as employer_match_amount_gbp_num
    from string_cleaning
),

calculate_net_contribution as (
    -- The table is queried twice, first table to calculate the workplace contribution
    select
        trade_date,
        transfer_type_cd,
        case
            when transfer_type_cd == 'SWITCH' then transaction_type_cd
            when transfer_type_cd == 'DEPOSIT' then 'EMPLOYER MATCH'
            else 'UNDEFINED'
        end as transfer_subtype_cd,
        fund_name_txt,
        case
            when transfer_type_cd == 'SWITCH' then transfer_amount_gbp_num
            when transfer_type_cd == 'DEPOSIT' then employer_match_amount_gbp_num
            else 'UNDEFINED'
        end as transfer_amount_gbp_num
    from math_calculation
    union    
    select -- Second table to calculate the net contribution
        trade_date,
        transfer_type_cd,
        'NET CONTRIBUTION',
        fund_name_txt,
        net_contribution_amount_gbp_num
    from math_calculation
    where transfer_type_cd != 'SWITCH'
),

final as (
    select
        cast(trade_date as DATE) as transfer_dt,
        cast('FRANCINE.NZUZI@GMAIL.COM' as VARCHAR) as customer_email_txt,        
        cast('00984275' as VARCHAR) as company_number_key,       
        cast('CRN' as VARCHAR) as company_number_system_cd,
        cast('WRP_PENSION_TAX' as VARCHAR) as wrapper_key,
        cast(upper(transfer_type_cd) as VARCHAR) as transfer_type_cd,
        cast(upper(transfer_subtype_cd) as VARCHAR) as transfer_subtype_cd,
        cast(transfer_amount_gbp_num as DECIMAL(18,4)) as transfer_amount_gbp_num
    from calculate_net_contribution
)

select * from final