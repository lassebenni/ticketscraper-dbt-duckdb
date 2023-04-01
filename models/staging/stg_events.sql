{{ config(materialized="incremental", unique_key="id") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/stg_events.parquet") }}

with
source as (select * from {{ source("events", "events") }}
)

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
    start_date,
    end_date,
    url
from source
