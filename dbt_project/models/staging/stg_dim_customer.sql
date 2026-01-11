with source as (
    -- This 'source' function links to our YAML definition above
    select * from {{ source('bronze_portfolio', 'dim_customer') }}
),

cleaning as (
    select 
        *,
        upper(trim(customer_firstname_txt||' '||customer_lastname_txt)) as customer_fullname_txt
    from source
),

final as (
    select
        cast(upper(customer_email_txt) AS VARCHAR) as customer_email_txt,
        cast(upper(customer_firstname_txt) AS VARCHAR) as customer_firstname_txt,
        cast(upper(customer_lastname_txt) AS VARCHAR) as customer_lastname_txt,
        cast(upper(customer_fullname_txt) AS VARCHAR) as customer_fullname_txt
    from cleaning
)

select * from final