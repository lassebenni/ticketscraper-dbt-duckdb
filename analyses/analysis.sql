{{
  config(
    enabled = false
    )
}}

with expired as (select * from {{ ref("int_expired_tickets") }})


select *
from expired
