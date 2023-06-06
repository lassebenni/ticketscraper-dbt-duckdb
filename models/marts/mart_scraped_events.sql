{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_scraped_events.parquet") }}

with
scraped as (select * from {{ ref("int_event_scrape") }}
),

entrance as (select * from {{ ref("int_event_entrances") }}
),

event_entrances_scraped as (
    select
        scraped.event_entrance_id,
        scraped.scraped_day,
        scraped.event_name,
        cast(scraped.event_start_date as datetime) as event_start_date,
        scraped.entrance_title,
        scraped.sum_price_per_day,
        scraped.sum_listings_per_day,
        scraped.avg_price_per_ticket_per_day,
        scraped.avg_original_price_per_ticket_per_day,
        scraped.avg_profit_per_ticket_per_day,

        entrance.scraped_days_per_entrance,
        entrance.total_listings_per_entrance,
        entrance.total_price_per_entrance,
        entrance.total_original_price_per_entrance,
        entrance.total_profit_per_entrance,
        entrance.avg_price_per_entrance,
        entrance.avg_original_price_per_entrance,
        entrance.avg_profit_per_entrance,
        entrance.max_price_per_entrance,
        entrance.min_price_per_entrance,

        entrance.price_per_entrance_stddev,
        entrance.price_per_entrance_q1,
        entrance.price_per_entrance_q3

    from scraped
    left join entrance
        on scraped.event_entrance_id = entrance.event_entrance_id
    order by
        scraped.scraped_day asc,
        scraped.event_name,
        scraped.event_start_date desc,
        scraped.sum_listings_per_day desc
)

select *
from event_entrances_scraped
