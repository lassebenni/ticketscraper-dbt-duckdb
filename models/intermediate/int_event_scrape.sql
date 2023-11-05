
with
listings_sold as (select * from {{ ref("int_validated_listings") }}
),

listings as (
    select
        date_trunc('day', updated) as scraped_day,
        event_name,
        event_start_date,
        entrance_title,
        event_entrance_id,
        amount_of_listings,
        original_price,
        price

    from listings_sold

),

event_scraped as (
    select
        scraped_day,
        event_name,
        event_start_date,
        entrance_title,
        event_entrance_id,

        round(sum(price)) as sum_price_per_day,
        sum(amount_of_listings) as sum_listings_per_day,
        round(
            sum(price) / sum(amount_of_listings)
        ) as avg_price_per_ticket_per_day,
        round(
            sum(original_price) / sum(amount_of_listings)
        ) as avg_original_price_per_ticket_per_day,
        round(sum(price) / sum(amount_of_listings)) - round(
            sum(original_price) / sum(amount_of_listings)
        ) as avg_profit_per_ticket_per_day

    from listings
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
        sum_listings_per_day,
        avg_price_per_ticket_per_day,
        avg_original_price_per_ticket_per_day,
        avg_profit_per_ticket_per_day

    from event_scraped
    order by
        scraped_day asc,
        event_name,
        event_start_date desc,
        sum_listings_per_day desc
)

select *
from final
