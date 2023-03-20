{{ config(materialized="table") }}
{{ config(materialized='external', format='parquet', location="s3://lbenninga-projects/ticketswap/dbt/int_tickets_enriched.parquet") }}


with
tickets_sold as (select * from {{ ref("stg_tickets_sold") }}
),

combined as (
    select
        event_name,
        entrance_title,
        event_entrance_id,
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
        seller_id
    from tickets_sold
)

select * from combined
