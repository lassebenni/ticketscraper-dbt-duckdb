{{ config(materialized="table") }}

with
listings_sold as (select * from {{ ref("int_validated_listings") }}
),

seller as (
    select
        seller_name as name,
        seller_id as id

    from listings_sold
)

select *
from seller
