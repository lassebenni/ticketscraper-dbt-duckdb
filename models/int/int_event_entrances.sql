{{ config(materialized="table") }}

with
listings_sold as (select * from {{ ref("stg_sold") }}
),

listings as (
    select
        event_name,
        event_start_date,
        entrance_title,
        event_entrance_id,
        amount_of_listings,
        original_price,
        price,
        date_trunc('day', updated) as scraped_day

    from listings_sold

),

event_entrances as (
    select
        event_name,
        event_start_date,
        entrance_title,
        event_entrance_id,

        count(distinct scraped_day) as scraped_days_per_entrance,
        sum(amount_of_listings) as total_listings_per_entrance,
        round(sum(price)) as total_price_per_entrance,
        round(sum(original_price)) as total_original_price_per_entrance,
        round(sum(price) - sum(original_price)) as total_profit_per_entrance,
        round(avg(price)) as avg_price_per_entrance,
        round(avg(original_price)) as avg_original_price_per_entrance,
        round(avg(price) - avg(original_price)) as avg_profit_per_entrance,
        max(price) as max_price_per_entrance,
        min(price) as min_price_per_entrance,
        round(median(original_price)) as median_original_price_per_entrance,


        round(stddev_samp(price)) as price_per_entrance_stddev,
        approx_quantile(price, 0.01) as price_per_entrance_q1,
        approx_quantile(price, 0.99) as price_per_entrance_q3

    from listings
    group by 1, 2, 3, 4
),

final as (
    select
        event_entrance_id,
        event_start_date,
        scraped_days_per_entrance,
        total_listings_per_entrance,
        total_price_per_entrance,
        total_original_price_per_entrance,
        total_profit_per_entrance,
        avg_price_per_entrance,
        avg_original_price_per_entrance,
        avg_profit_per_entrance,
        max_price_per_entrance,
        min_price_per_entrance,
        median_original_price_per_entrance,
        price_per_entrance_stddev,
        price_per_entrance_q1,
        price_per_entrance_q3

    from event_entrances
)

select *
from final
