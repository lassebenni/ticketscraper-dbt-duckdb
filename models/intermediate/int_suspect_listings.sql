{{
  config(
    materialized='table'
  )
}}


with
listings as (
    select * from {{ ref("stg_sold") }}
),

entrances as (
    select * from {{ ref("int_event_entrances") }}
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
        listings.updated,
        listings.ticket_id,
        listings.event_name,
        listings.event_start_date,
        listings.entrance_title,
        listings.price,
        median_original_price_per_entrance,
        abs(
            round(
                (listings.price - stdev.median_original_price_per_entrance)
                / stdev.median_original_price_per_entrance
                * 100
            )
        ) as price_diff_median_percent,
        stdev.price_lower_bound,
        stdev.price_upper_bound,
        listings.original_price,
        listings.url,
        stdev.event_entrance_id,
        stdev.total_listings_per_entrance,
        case
            when listings.price > stdev.price_upper_bound or listings.price < stdev.price_lower_bound
                then 1
            else 0
        end as suspect_price,
        case
            when listings.updated > stdev.event_start_date
                then 1
            when date_diff('day', listings.updated, stdev.event_start_date) < 2
                then 1
            else 0
        end as suspect_datetime
    from listings
    left join event_stdev stdev on listings.event_entrance_id = stdev.event_entrance_id
)

select *
from suspect_event
where (suspect_price = 1 or suspect_datetime = 1)
order by price_diff_median_percent asc
