{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/mart_tickets_enriched.parquet") }}


with
    _stg_tickets_sold as (select * from {{ ref("stg_tickets_sold") }}),

    combined as (
        select
            event_name,
            entrance_title,
            url,
            updated,
            description,
            event_start_date,
            event_end_date,
            location,
            city,
            status,
            amount_of_tickets,
            original_price,
            price,
            round(price - original_price, 2) as profit,
        from _stg_tickets_sold
    )

select
    event_name,
    entrance_title,
    url,
    updated,
    description,
    event_start_date,
    event_end_date,
    location,
    city,
    status,
    amount_of_tickets,
    original_price,
    price,
    profit
from combined
