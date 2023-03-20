{{ config(materialized="incremental", unique_key="id") }}
{{ config(materialized='external', format='parquet', location="events.parquet") }}

with
source as (select * from {{ source("events", "events") }}
)

select
    updated,
    id,
    name,
    entrance_slug as slug,
    unnest(entrance_types).available_tickets as entrance_available_tickets,
    unnest(entrance_types).slug as entrance_slug,
    unnest(entrance_types).title as entrance_title,
    available_tickets,
    sold_tickets,
    wanted_tickets,
    location,
    city,
    start_date,
    end_date,
    url
from source
