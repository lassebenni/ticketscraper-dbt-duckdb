{{ config(materialized="table") }}

with
tickets_sold as (select * from {{ ref("stg_tickets_sold") }}
),

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

event_scraped as (
    select
        scraped_day,
        event_name,
        event_start_date,
        entrance_title,
        event_entrance_id,

        round(sum(price)) as sum_price_per_day,
        sum(amount_of_tickets) as sum_tickets_per_day,
        round(
            sum(price) / sum(amount_of_tickets)
        ) as avg_price_per_ticket_per_day,
        round(
            sum(original_price) / sum(amount_of_tickets)
        ) as avg_original_price_per_ticket_per_day,
        round(sum(price) / sum(amount_of_tickets)) - round(
            sum(original_price) / sum(amount_of_tickets)
        ) as avg_profit_per_ticket_per_day

    from tickets
    group by 1, 2, 3, 4, 5

),

final as (
    select
        event_entrance_id,
        scraped_day,
        event_name,
        event_start_date,
        entrance_title,
        sum_price_per_day,
        sum_tickets_per_day,
        avg_price_per_ticket_per_day,
        avg_original_price_per_ticket_per_day,
        avg_profit_per_ticket_per_day

    from event_scraped
    order by
        scraped_day asc,
        event_name,
        event_start_date desc,
        sum_tickets_per_day desc
)

select *
from final
