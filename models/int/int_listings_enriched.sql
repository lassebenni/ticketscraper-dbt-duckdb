{{ config(materialized="table") }}


with
listings_sold as (select * from {{ ref("int_validated_listings") }}
),

combined as (
    select
        ticket_id,
        event_name,
        entrance_title,
        event_entrance_id,
        url,
        updated,
        description,
        event_start_date,
        event_end_date,
        location,
        city,
        status,
        amount_of_listings,
        original_price,
        price,
        round(price - original_price, 2) as profit,
        seller_id
    from listings_sold
)

select * from combined
order by profit asc
