{{ config(materialized="incremental", unique_key="id") }}

with
    source as (select * from {{ source("parquet", "sold") }}),

    renamed as (

        select
            id,
            description,
            event_name,
            event_start_date,
            cast(event_start_date as datetime) as event_start_date,
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
            seller_id,
            case
                when position('.' in updated) > 0
                then strptime(updated, '%Y-%m-%d %H:%M:%S.%f')
                else strptime(updated, '%Y-%m-%d %H:%M:%S')
            end as updated,
            row_number() over (partition by id order by updated desc) as row_num


        from source

        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where updated > (select max(updated) from {{ this }})

        {% endif %}

    ),

    deduped as (
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

        where row_num = 1
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
    md5(concat(event_name, event_start_date, entrance_title)) as event_entrance_id,
    original_price,
    price,
    location,
    city,
    url,
    status,
    seller_name,
    seller_id

from deduped
