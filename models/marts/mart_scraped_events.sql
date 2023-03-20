{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_tickets_sold_stats.parquet") }}

with
    tickets_sold as (select * from {{ ref("stg_tickets_sold") }}),

tickets as (
  select
    date_trunc('day', updated) as scraped_day,
    event_name,
    CAST(event_start_date AS DATETIME) as event_start_date,
    entrance_title,
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

    round(sum(price)) as sum_price_per_day,
    sum(amount_of_tickets) as sum_tickets_per_day,
    round(sum(price) / sum(amount_of_tickets)) as avg_price_per_ticket_per_day,
    round(sum(original_price) / sum(amount_of_tickets)) as avg_original_price_per_ticket_per_day,
    round(sum(price) / sum(amount_of_tickets)) - round(sum(original_price) / sum(amount_of_tickets)) as avg_profit_per_ticket_per_day

  from tickets
  group by 1, 2, 3, 4

),

grouped_event_startdate_entrance as (
  select
    event_name,
    event_start_date,
    entrance_title,

    count(distinct scraped_day) AS scraped_days_per_entrance,
    sum(amount_of_tickets) as total_tickets_per_entrance,
    round(sum(price)) as total_price_per_entrance,
    round(sum(original_price)) as total_original_price_per_entrance,
    round(sum(price) - sum(original_price)) as total_profit_per_entrance


  from tickets
  group by 1,2,3
),

final as (
    select 
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
    entrance.total_profit_per_entrance


    from grouped_scraped_day_event_startdate_entrance scraped
    left join grouped_event_startdate_entrance entrance
    on scraped.event_name = entrance.event_name and scraped.event_start_date = entrance.event_start_date and scraped.entrance_title = entrance.entrance_title
    order by scraped.scraped_day asc, scraped.event_name, scraped.event_start_date desc, scraped.sum_tickets_per_day desc
)

select * from final