{{ config(materialized="incremental", unique_key="id") }}
{{ config(materialized='external', format='parquet') }}

with
    source as (select * from {{ source("parquet", "sold") }}),

    renamed as (

        select
            strptime(updated, '%Y-%m-%d %H:%M:%S.%f') as updated,
            id,
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
            city,
            url,
            status,
            seller_name,
            seller_id

        from source

        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where updated > (select max(updated) from {{ this }})

        {% endif %}

    )

select
    updated,
    id,
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
    city,
    url,
    status,
    seller_name,
    seller_id

from renamed
