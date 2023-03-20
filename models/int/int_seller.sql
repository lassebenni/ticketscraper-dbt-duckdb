{{ config(materialized="table") }}

with
    tickets_sold as (select * from {{ ref("stg_tickets_sold") }}),

    seller as (
        select
            seller_name as name,
            seller_id as id

        from tickets_sold
    )

select *
from seller
