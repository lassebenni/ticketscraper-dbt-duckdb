
with
    listings_sold as (select * from {{ ref("stg_sold") }}),

    expired_tickets as (select * from {{ ref("int_expired_tickets") }}),

    listings_sold_clean as (
        select
            sold.ticket_id,
            sold.description,
            sold.event_name,
            sold.event_start_date,
            sold.event_end_date,
            sold.amount_of_listings,
            sold.entrance_title,
            sold.entrance_id,
            sold.original_price,
            sold.price,
            sold.currency,
            sold.location,
            sold.city,
            sold.url,
            sold.status,
            sold.seller_name,
            sold.seller_id,
            sold.event_entrance_id,
            sold.updated,
            case when expired.ticket_id is null then false else true end as is_expired
        from listings_sold sold
        left join expired_tickets expired
            on sold.ticket_id = expired.ticket_id
    )

select *
from listings_sold_clean
