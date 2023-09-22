{{ config(materialized="table") }}

with
source as (select * from {{ source("invalid_listings", "invalid_listings") }}
)

select
    id,
    event_name,
    url

from source
