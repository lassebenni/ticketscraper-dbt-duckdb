{{ config(materialized="table") }}

with
tickets_sold as (select * from {{ ref("stg_tickets_sold") }}
),

invalid_listings as (
    select * from {{ ref("stg_invalid_listings") }}

    {# checked as (select * from {{ ref("int_checked_tickets") }} #}
),

tickets_sold_clean as (
    select *
    from tickets_sold
    where id not in (select id from invalid_listings)
)

select *
from tickets_sold_clean
