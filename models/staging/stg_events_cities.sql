{{ config(materialized="table") }}

with
source as (select * from {{ source("event_cities", "event_cities") }}
),

final as (
    select
        updated,
        id as event_id,
        name,
        entrance_slug as slug,
        unnest(entrance_types).available_tickets as entrance_available_tickets,
        unnest(entrance_types).slug as entrance_slug,
        unnest(entrance_types).title as entrance_title,
        unnest(entrance_types).id as entrance_id,
        available_tickets,
        sold_tickets,
        wanted_tickets,
        location,
        city,
        cast(start_date as datetime) as start_date,
        cast(end_date as datetime) as end_date,
        url,
        row_number() over (partition by id order by updated desc) as row_num

    from source
)

select *
from final
where row_num = 1
