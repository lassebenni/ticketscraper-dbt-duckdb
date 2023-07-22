{{ config(materialized="table") }}

{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/int_suspect_listings.parquet") }}

with
listings as (
    select * from {{ ref("stg_listings_sold") }}
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
        event_start_date,
        total_listings_per_entrance,
        median_original_price_per_entrance,
        median_original_price_per_entrance / 2 as price_lower_bound,
        median_original_price_per_entrance * 2 as price_upper_bound

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
        median_original_price_per_entrance,
        abs(
            round(
                (t.price - e.median_original_price_per_entrance)
                / e.median_original_price_per_entrance
                * 100
            )
        ) as price_diff_median_percent,
        e.price_lower_bound,
        e.price_upper_bound,
        t.original_price,
        t.url,
        e.event_entrance_id,
        e.total_listings_per_entrance,
        case
            when t.price > e.price_upper_bound or t.price < e.price_lower_bound
                then 1
            else 0
        end as suspect_price,
        case
            when t.updated > e.event_start_date
                then 1
            when date_diff('day', t.updated, e.event_start_date) < 2
                then 1
            else 0
        end as suspect_datetime
    from listings t
    left join event_stdev e on t.event_entrance_id = e.event_entrance_id
    where id not in (select id from invalid_listings)
)

select *
from suspect_event
where (suspect_price = 1 or suspect_datetime = 1)
order by price_diff_median_percent asc

