select event_name, entrance_title, max(updated), count(price) as prices_count, median(price) as median_price from mart_tickets_enriched
where price =
(
  select price from (select event_name, entrance_title, median(price) from mart_tickets_enriched
  group by 1,2)
  )
group by 1,2