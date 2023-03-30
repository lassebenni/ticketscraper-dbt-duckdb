{{ config(materialized="table") }}

{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_suspect_listings.parquet") }}

with
tickets as (
    select * from {{ ref("int_tickets_enriched") }}
),
entrances as (
    select * from {{ ref("int_event_entrances") }}
),
invalid_listings as (
    select * from {{ ref("stg_invalid_listings") }}
),
event_stdev as (
    select
        event_entrance_id,
        total_tickets_per_entrance,
        {# round(price_per_entrance_q1 / 1.5) as price_lower_bound,
        round(price_per_entrance_q3 * 1.5) as price_upper_bound, #}
        round(median_price_per_entrance / 2) as price_lower_bound,
        round(median_price_per_entrance * 2) as price_upper_bound,
        median_price_per_entrance,

    from entrances
),
suspect_event as (
    select
        t.updated,
        t.id,
        t.event_name,
        t.event_start_date,
        t.entrance_title,
        t.price,
        median_price_per_entrance,
        abs(round((t.price - e.median_price_per_entrance) / e.median_price_per_entrance * 100)) as price_diff_median_percent,
        e.price_lower_bound,
        e.price_upper_bound,
        t.original_price,
        t.url,
        e.event_entrance_id,
        e.total_tickets_per_entrance,
        case
            when t.price > e.price_upper_bound or t.price < e.price_lower_bound
            then 1
            else 0
        end as suspect
    from tickets t
    left join event_stdev e on t.event_entrance_id = e.event_entrance_id
    where id not in (select id from invalid_listings)
)

select *
from suspect_event
where suspect = 1
order by price_diff_median_percent asc
