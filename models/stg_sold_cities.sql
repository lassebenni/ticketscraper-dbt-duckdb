{{ config(materialized="table") }}

with source as (
      select * from {{ source('sold_cities', 'sold_cities') }}
),


renamed as (
    select
        updated,
        id,
        status,
        description,
        event_name,
        event_start_date,
        event_end_date,
        amount_of_tickets,
        entrance_title,
        entrance_id,
        original_price,
        price,
        location,
        url,
        seller_name,
        seller_id,
        currency,
        city,
        date

    from source
)

select * from renamed