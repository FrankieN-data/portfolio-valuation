{{
  config(
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/gold/dim_date.parquet' (FORMAT 'PARQUET')"
  )
}}

with date_spine as (
    select range::DATE as date_day
    from range('2020-01-01'::DATE, '2079-12-08'::DATE, INTERVAL 1 DAY)
)

select 
    strftime(date_day, '%Y%m%d')::INT as date_key,
    date_day,
    strftime(date_day, '%d/%m/%Y') as display_date,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    strftime(date_day, '%Y-%m') as year_month,
    dayname(date_day) as day_of_week,
    -- UK Fiscal Year Logic: April 6th Switch
    case 
        when date_day < make_date(year(date_day), 4, 6) then (year(date_day) - 1) || '/' || year(date_day)
        else year(date_day) || '/' || (year(date_day) + 1)
    end as uk_fiscal_year,
    case
        when day(date_day) <= 28 then make_date(year(date_day), month(date_day), 28)
        when month(date_day) < 12 then make_date(year(date_day), month(date_day)+1, 28)
        else make_date(year(date_day)+1, 1, 28)
    end as last_day_of_the_month,
    case 
        when date_day < make_date(year(date_day), 4, 6) then make_date(year(date_day), 4, 5)
        else make_date(year(date_day)+1, 4, 5)
    end as last_day_of_uk_fiscal_year
from date_spine