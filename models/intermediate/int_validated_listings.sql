
with
listings_sold as (select * from {{ ref("stg_sold") }}),

{# expired_tickets as (select * from {{ ref("int_expired_tickets") }}), #}

listings_sold_clean as (
    select
        ticket_id,
        description,
        event_name,
        event_start_date,
        event_end_date,
        amount_of_listings,
        entrance_title,
        entrance_id,
        original_price,
        price,
        currency,
        location,
        city,
        url,
        status,
        seller_name,
        seller_id,
        event_entrance_id,
        updated
    from listings_sold
    {# where
        and ticket_id not in (select ticket_id from expired_tickets) #}
)

select *
from listings_sold_clean
order by price desc
