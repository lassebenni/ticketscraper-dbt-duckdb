with toppers as (
select
  event_name,
  entrance_title,
  event_start_date as start_date,
  price,
  original_price,
  count(*),
  max(updated) as max_date,
  min(updated) as min_date
from
  tickets_sold
-- where event_name like '%Tomorrowland presents: Eric Prydz HOLO | #ADE%'
group by
  1,
  2,
  3,
  4,
  5
having price < original_price
)

select *
from toppers

-- select event_name, count(*), sum(price), sum(original_price), sum(price-original_price)
-- from toppers
-- group by 1 
