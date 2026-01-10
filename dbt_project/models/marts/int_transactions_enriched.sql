with transactions as (
    select * from {{ ref('stg_equateplus_transactions') }}
),

accounts as (
    select * from {{ ref('stg_dim_account') }}
),

final as (
    select
        -- On garde toutes les colonnes de la transaction
        t.transaction_dt,
        t.user_fullname_txt,
        t.investment_platform_shortname_txt,
        t.account_name_txt,
        t.asset_name_txt,
        t.order_type_cd,
        t.transaction_details,
        t.quantity_held_num,
        t.unit_price_gbp_num,
        t.extended_transaction_amount_gbp_num,

        -- On ajoute les colonnes de la dimension compte grâce à la jointure
        a.account_type_cd,
        a.account_subtype_cd,
        a.tax_regime_uk_cd

    from transactions t
    left join accounts a 
        on t.account_name_txt = a.account_name_txt
)

select * from final