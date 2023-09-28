{{ config(materialized="incremental", unique_key="event_id") }}

with
    source as (select * from {{ source("events", "events") }}),

    base as (
        select
            id as event_id,
            cast(updated as datetime) as updated_at,
            name,
            entrance_slug as slug,
            {# unnest(entrance_types).available_tickets as entrance_available_tickets, #}
            {# unnest(entrance_types).slug as entrance_slug, #}
            {# unnest(entrance_types).title as entrance_title, #}
            {# unnest(entrance_types).id as entrance_id, #}
            available_tickets,
            sold_tickets,
            wanted_tickets,
            location,
            city,
            cast(start_date as datetime) as start_date,
            cast(end_date as datetime) as end_date,
            url,

        from source

    ),

    final as (
        select
            event_id,
            name,
            slug,
            available_tickets,
            sold_tickets,
            wanted_tickets,
            location,
            city,
            start_date,
            end_date,
            datediff('day', updated_at, start_date) as days_to_event_start,
            url,

            updated_at,

        from base

        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where
            updated_at is not null
            and updated_at not like 'NaT'
            and updated_at > (select max(updated_at) from {{ this }})

        {% else %}
        where updated_at is not null
            and updated_at not like 'NaT'

        {% endif %}

        qualify row_number() over (partition by event_id order by updated_at desc) = 1
    )

select *
from final
