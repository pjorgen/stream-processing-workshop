-- For demo - query 3

WITH ranked_artists AS (
select
a.name,
a1.state,
sum(s.count) number_of_streams,
ROW_NUMBER() OVER (PARTITION BY a1.state ORDER BY sum(s.count) DESC) as row_num
from "stream" as s
join artist a on s.artistid = a.id
join customer c on s.customerid = c.id
left join address a1 on c.id = a1.customerid and a1.type = 'RESID'
group by 1,2
order by state, number_of_streams desc
)
SELECT
  name,
  state,
  number_of_streams
FROM ranked_artists
WHERE row_num = 1
ORDER BY state, number_of_streams DESC

-- New Query - Query 4
select customer_state,
round(sum(price) / sum(tickets),2) avg_price_ticket,
round(max(price),2) highest_price_ticket,
sum(tickets) total_tickets
from (
select  
a1.state customer_state,
sum(t.price) price,
sum(t.count) tickets
from ticket t
join event e on t.eventid = e.id
join customer c on t.customerid = c.id
left join address a1 on c.id = a1.customerid and a1.type = 'RESID'
group by 1
)
group by 1
order by 2 desc


--Query 1

select customer_state,
venue_state,
sum(same_state_ticket_count) / sum(total_ticket_count) as pct,
sum(total_ticket_count) total_ticket_count
from (
  select customer_state,
  venue_state,
  sum(same_state_ticket_count) same_state_ticket_count,
  sum(total_ticket_count) total_ticket_count 
  from (
	select   
	a1.state customer_state,
	a2.state venue_state,
	case when a1.state = a2.state then t.count else 0 end same_state_ticket_count, 
	t.count total_ticket_count
	from ticket t
	join event e on t.eventid = e.id
	join venue v on e.venueid = v.id
	join customer c on t.customerid = c.id
	left join address a1 on c.id = a1.customerid and a1.type = 'RESID'
	left join address a2 on v.id = a2.id and a2.type = 'VENUE'
  	)
  	group by 1,2
 ) 
 group by 1,2
 order by 3 desc
  

select 
v.id,
a.genre,
sum(e.count) as ticket_count
from event e
join venue v on e.venueid = v.id
left join artist a on v.artistid = a.id
group by 1,2
order by 3 desc


select a1.state,
sum(t.count) ticket_count
from artist a
join event e on a.id = e.artistid
join ticket t on e.id = t.eventid
join customer c on t.customerid = c.id
left join address a1 on c.id = a1.customerid and a1.type = 'RESID'
group by 1 
order by 2 desc

select state,
sum(price) / sum(tickets) avg_price_ticket,
sum(tickets) total_tickets
from (
  select a1.state,
  sum(price) price,
  sum(t.count) tickets
  from customer c
  join address a1 on c.id = a1.customerid and a1.type = 'RESID'
  join ticket t on c.id = t.customerid
  group by 1
)
group by 1 
order by 2 desc

select *
from "stream" as s
join customer as c on c.id = s.customerid
join address a1 on a1.id = c.addressid and a1.type = 'RESID'


select *
from "stream" as s
join customer as c on c.id = s.customerid
join address as a on a.customerid = c.id
limit 10

select state,
sum(s.count) streams
from "stream" as s
join customer as c on c.id = s.customerid
join address as a on a.customerid = c.id
group by 1


select 
sum(count) number_of_streams
from "stream" as s
join artist a on s.artistid = a.id


select 
sum(s.count) number_of_streams
from "stream" as s
join artist a on s.artistid = a.id
join customer c on s.customerid = c.id
join address ad on c.addressid = ad.id 


select 
a.name,
a1.state,
sum(s.count) number_of_streams
from "stream" as s
join artist a on s.artistid = a.id
join customer c on s.customerid = c.id
left join address a1 on c.id = a1.customerid and a1.type = 'RESID'
group by 1,2
order by 3 desc

WITH ranked_artists AS (
select 
a.name,
a1.state,
sum(s.count) number_of_streams,
ROW_NUMBER() OVER (PARTITION BY a1.state ORDER BY sum(s.count) DESC) as row_num
from "stream" as s
join artist a on s.artistid = a.id
join customer c on s.customerid = c.id
left join address a1 on c.id = a1.customerid and a1.type = 'RESID'
group by 1,2
order by state, number_of_streams desc
)
SELECT
  name,
  state,
  number_of_streams
FROM ranked_artists
WHERE row_num = 1
ORDER BY state, number_of_streams DESC