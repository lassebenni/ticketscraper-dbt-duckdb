
with
validated_listings as (select * from {{ ref("int_validated_listings") }}),


listings as (
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
    from validated_listings
)

select *
from listings
