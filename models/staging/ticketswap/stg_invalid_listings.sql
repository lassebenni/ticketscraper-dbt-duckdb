{{ config(materialized="table") }}

with
source as (select * from {{ source("invalid_listings", "invalid_listings") }}
),

final as (

select
    id,
    event_name,
    url

from source

)

select *
from final
