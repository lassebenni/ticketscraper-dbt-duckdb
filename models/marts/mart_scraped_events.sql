{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_tickets_sold_stats.parquet") }}

with
    tickets_sold as (select * from {{ ref("stg_tickets_sold") }}),

    tickets as (
        select
            date_trunc('day', updated) as scraped_day,
            event_name,
            event_start_date,
            entrance_title,
            event_entrance_id,
            amount_of_tickets,
            original_price,
            price

        from tickets_sold

    ),

    grouped_scraped_day_event_startdate_entrance as (
        select
            scraped_day,
            event_name,
            event_start_date,
            entrance_title,
            event_entrance_id,

            round(sum(price)) as sum_price_per_day,
            sum(amount_of_tickets) as sum_tickets_per_day,
            round(sum(price) / sum(amount_of_tickets)) as avg_price_per_ticket_per_day,
            round(
                sum(original_price) / sum(amount_of_tickets)
            ) as avg_original_price_per_ticket_per_day,
            round(sum(price) / sum(amount_of_tickets)) - round(
                sum(original_price) / sum(amount_of_tickets)
            ) as avg_profit_per_ticket_per_day

        from tickets
        group by 1, 2, 3, 4, 5

    ),

    grouped_event_startdate_entrance as (
        select
            event_name,
            event_start_date,
            entrance_title,
            event_entrance_id,

            count(distinct scraped_day) as scraped_days_per_entrance,
            sum(amount_of_tickets) as total_tickets_per_entrance,
            round(sum(price)) as total_price_per_entrance,
            round(sum(original_price)) as total_original_price_per_entrance,
            round(sum(price) - sum(original_price)) as total_profit_per_entrance,
            round(avg(price)) as avg_price_per_entrance,
            round(avg(original_price)) as avg_original_price_per_entrance,
            round(avg(price) - avg(original_price)) as avg_profit_per_entrance,
            max(price) as max_price_per_entrance,
            min(price) as min_price_per_entrance,

            round(stddev_samp(price)) as price_per_entrance_stddev,
            approx_quantile(price, 0.01) as price_per_entrance_q1,
            approx_quantile(price, 0.99) as price_per_entrance_q3

        from tickets
        group by 1, 2, 3, 4
    ),

    final as (
        select
            scraped.event_entrance_id,
            scraped.scraped_day,
            scraped.event_name,
            scraped.event_start_date,
            scraped.entrance_title,
            scraped.sum_price_per_day,
            scraped.sum_tickets_per_day,
            scraped.avg_price_per_ticket_per_day,
            scraped.avg_original_price_per_ticket_per_day,
            scraped.avg_profit_per_ticket_per_day,

            entrance.scraped_days_per_entrance,
            entrance.total_tickets_per_entrance,
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

        from grouped_scraped_day_event_startdate_entrance scraped
        left join
            grouped_event_startdate_entrance entrance
            on scraped.event_entrance_id = entrance.event_entrance_id
        order by
            scraped.scraped_day asc,
            scraped.event_name,
            scraped.event_start_date desc,
            scraped.sum_tickets_per_day desc
    )

select *
from final
