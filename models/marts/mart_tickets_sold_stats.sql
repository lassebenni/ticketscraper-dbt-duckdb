{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_tickets_sold_stats.parquet") }}

with
    tickets_sold as (select * from {{ ref("stg_tickets_sold") }}),

    -- Get the count of prices for each event
    count_prices_cte as (
        select
            event_name,
            event_start_date,
            entrance_title,
            price,
            count(*) as count_prices
        from tickets_sold
        group by 1, 2, 3, 4
        order by 1 desc
    ),

    -- Get the median price for each event
    median_price_cte as (
        select
            event_name,
            entrance_title,
            count_prices,
            price,
            row_number() over (
                partition by event_name, entrance_title order by count_prices desc
            ) rn
        from count_prices_cte
        order by 1, 2, 3, 4 desc
    ),

    -- Get the statistics for each event
    stats_cte as (
        select
            s.event_name,
            s.event_start_date as start_date,
            s.entrance_title,
            m.price as median_price,
            count(s.price) as tickets_sold,
            min(s.original_price),
            round(m.price - min(s.original_price), 2) as median_profit,
            min(s.price),
            max(s.price),
            min(s.updated) as first_listing_date,
            max(s.updated) as last_listing_date
        from tickets_sold s
        left join
            median_price_cte m
            on s.event_name = m.event_name
            and s.entrance_title = m.entrance_title
            and m.rn = 1
        group by 1, 2, 3, 4
        order by 1, 3 asc, 2 asc
    )

select *
from stats_cte
