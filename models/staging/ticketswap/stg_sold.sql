{{ config(materialized="incremental", unique_key="ticket_id") }}

with
source as (select * from {{ source("sold", "sold") }}
),

final as (

    select
        id as ticket_id,
        description,
        event_name,
        cast(event_start_date as datetime) as event_start_date,
        event_end_date,
        amount_of_tickets as amount_of_listings,
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


    from source

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where updated > (select max(updated) from {{ this }})
    {% endif %}

    qualify row_number() over (partition by ticket_id order by updated desc) = 1
)

select *
from final
where currency = 'EUR'
