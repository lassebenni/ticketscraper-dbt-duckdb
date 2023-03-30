{{ config(materialized="incremental", unique_key="id") }}

with
source as (select * from {{ source("sold", "sold") }}
),

renamed as (

    select
        id,
        description,
        event_name,
        cast(event_start_date as datetime) as event_start_date,
        event_end_date,
        amount_of_tickets,
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
        md5(
            concat(event_name, event_start_date, entrance_title)
        ) as event_entrance_id,
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

)

select * exclude (row_num)
from renamed
where
    row_num = 1
    -- Only EUR for now - 20-03-23
    and currency = 'EUR'
