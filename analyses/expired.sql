select * 
from (select * from {{ ref("int_expired_listings") }})
order by updated desc