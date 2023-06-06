{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/dim_seller.parquet") }}

with
listings as (select * from {{ ref("int_listings_enriched") }}
),

seller as (select * from {{ ref("int_seller") }}
),


seller_listings as (
    select
        seller.id,
        seller.name,

        count(amount_of_listings) as total_listings,
        sum(amount_of_listings) as total_listings_listed,
        round(sum(profit)) as total_profit,
        min(updated) as first_ticket_sold,
        max(updated) as last_ticket_sold,
        any_value(url) as random_ticket_url
    from listings
    left join seller on seller.id = listings.seller_id
    group by 1, 2
    order by 3 desc
)


select *
from seller_listings
