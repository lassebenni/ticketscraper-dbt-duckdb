{{
  config(
    enabled = false
    )
}}
with listings as (
select *
from {{ ref('int_validated_listings') }}
),

expired as (select * from {{ ref("int_expired_listings") }}),

suspect as (select * from {{ ref('int_suspect_listings') }}),

diff as (
    select *
        from listings
        where price != original_price
        and (price - original_price) / original_price * 100 > 100
)

{# select updated, id, event_name, price, original_price, url
from diff
where id in (
    select id
    from expired
) #}

select count(*) from int_suspect_listings
