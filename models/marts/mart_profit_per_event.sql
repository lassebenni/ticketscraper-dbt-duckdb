{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_profit_per_event.parquet") }}

with
listings as (select * from {{ ref("int_listings_enriched") }}
),

listings_scraped as (
    select
        date_trunc('day', updated) as scraped_day,
        event_name,
        cast(event_start_date as datetime) as event_start_date,
        entrance_title,
        amount_of_listings,
        original_price,
        price,
        profit

    from listings

),

grouped_event_startdate as (
    select
        event_name,
        event_start_date,

        count(distinct scraped_day) as scraped_days,
        round(sum(profit)) as total_profit,
        sum(amount_of_listings) as total_listings_sold

    from listings_scraped
    group by 1, 2
)

-- select * from grouped_event_startdate
select
    event_name,
    event_start_date,
    total_profit,
    total_listings_sold

from grouped_event_startdate
order by total_profit desc
