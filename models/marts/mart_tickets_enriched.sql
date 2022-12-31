{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet') }}


with
    _stg_tickets_sold as (select * from {{ ref("stg_tickets_sold") }}),
    stats as (select * from {{ ref("mart_tickets_sold_stats") }}),

    combined as (
        select
            a.event_name,
            a.entrance_title,
            a.url,
            a.updated,
            a.description,
            a.event_start_date,
            a.event_end_date,
            a.location,
            a.city,
            a.status,
            a.amount_of_tickets,
            a.original_price,
            a.price,
            round(a.price - a.original_price, 2) as profit,
            b.median_price,
            b.median_profit,
            b.tickets_sold
        from _stg_tickets_sold a
        left join
            stats b
            on a.event_name = b.event_name
            and a.entrance_title = b.entrance_title
    )

select
    event_name,
    entrance_title,
    url,
    updated,
    description,
    event_start_date,
    event_end_date,
    location,
    city,
    status,
    amount_of_tickets,
    original_price,
    price,
    profit,
    median_price,
    median_profit,
    tickets_sold
from combined
