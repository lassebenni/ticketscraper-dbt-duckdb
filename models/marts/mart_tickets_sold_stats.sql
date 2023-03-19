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
    price,
    profit

  from mart_tickets_enriched

),

grouped_scraped_day_event_startdate_entrance as (
  select
    scraped_day,
    event_name,
    event_start_date,
    entrance_title,

    round(sum(price)) as sum_price,
    sum(amount_of_tickets) as sum_tickets,
    round(sum(price) / sum(amount_of_tickets)) as avg_price_per_ticket,
    round(sum(original_price) / sum(amount_of_tickets)) as avg_original_price_per_ticket,
    round(sum(price) / sum(amount_of_tickets)) - round(sum(original_price) / sum(amount_of_tickets)) as avg_profit_per_ticket

  from tickets
  group by 1, 2, 3, 4

),

grouped_event_startdate_entrance as (
  select
    event_name,
    event_start_date,
    entrance_title,

    count(distinct scraped_day) AS scraped_days

  from tickets
  group by 1,2,3
),

final as (
    select 
    a.scraped_day,
    a.event_name,
    a.event_start_date,
    a.entrance_title,
    a.sum_price,
    a.sum_tickets,
    a.avg_price_per_ticket,
    a.avg_original_price_per_ticket,
    a.avg_profit_per_ticket,

    b.scraped_days

    from grouped_scraped_day_event_startdate_entrance a
    left join grouped_event_startdate_entrance b 
    on a.event_name = b.event_name and a.event_start_date = b.event_start_date and a.entrance_title = b.entrance_title
    order by a.scraped_day asc, a.event_name, a.event_start_date desc, a.sum_tickets desc
)

select * from final