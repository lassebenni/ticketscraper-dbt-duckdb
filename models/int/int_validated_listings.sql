{{ config(materialized="table") }}

with
listings_sold as (select * from {{ ref("stg_listings_sold") }}),

invalid_listings as (select * from {{ ref("stg_invalid_listings") }}),

expired_listings as (select * from {{ ref("int_expired_listings") }}),

listings_sold_clean as (
    select
        id,
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
    where
        id not in (select id from invalid_listings)
        and id not in (select id from expired_listings)
)

select updated, *
from listings_sold_clean
order by price desc
