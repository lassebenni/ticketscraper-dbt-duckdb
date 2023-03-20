{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_suspect_listings.parquet") }}

with
    tickets as (select * from {{ ref("int_tickets_enriched") }}),
    scraped_events as (select * from {{ ref("mart_scraped_events") }}),


    event_stdev as (
        select
            event_entrance_id,
            total_tickets_per_entrance,
            price_per_entrance_q1
            - 1.5
            * (price_per_entrance_q3 - price_per_entrance_q1) as price_lower_bound,
            price_per_entrance_q3
            + 1.5 * (price_per_entrance_q3 - price_per_entrance_q1) as price_upper_bound
        from scraped_events
    ),

    suspect_event as (

        select
            t.url,
            t.price,
            t.original_price,
            case
                when t.price > e.price_upper_bound or t.price < e.price_lower_bound
                then 1
                else 0
            end as suspect,
            e.*
        from tickets t
        left join event_stdev e on t.event_entrance_id = e.event_entrance_id
    )

select *
from suspect_event
where suspect = 1
