{{ config(materialized="incremental", unique_key="id") }}

with
source as (select * from {{ source("parquet", "sold") }}
),

renamed as (

    select
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
        seller_id,
        case
            when POSITION('.' in updated) > 0 then
                STRPTIME(updated, '%Y-%m-%d %H:%M:%S.%f')
            else
                STRPTIME(updated, '%Y-%m-%d %H:%M:%S')
        end as updated,
        ROW_NUMBER() over (partition by id order by updated desc) as row_num


    from source

    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where updated > (select MAX(updated) from {{ this }})

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
    original_price,
    price,
    location,
    city,
    url,
    status,
    seller_name,
    seller_id

from deduped
