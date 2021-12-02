--DATA IMPORT
with data as (
  select 
     n			= cast(row_number() over (order by (select null)) as int)
    ,Direction	= substring(v.val, 0, len(v.val) - 1)
    ,Value		= cast(right(v.val, 1) as int)

  from STRING_SPLIT(
  'forward 5
  down 5
  forward 8
  up 3
  down 8
  forward 2', char(13)) t
  cross apply (select val = replace(t.value, char(10), '')) v(val)
)

--PART 1
select 
	 horizontal  = sum(case Direction when 'forward' then Value end)
	,depth		 = sum(case Direction when 'down' then Value when 'up' then -Value end)
	,answer		 = sum(case Direction when 'forward' then Value end) * sum(case Direction when 'down' then Value when 'up' then -Value end)
from data


--PART 2
,pos as (
  select  horizontal = 0
       ,depth = 0
       ,aim = 0
       ,n = 0
  union all
  select  horizontal = horizontal + case Direction when 'forward' then Value else 0 end
       ,depth = depth + case Direction when 'forward' then (Value * aim) else 0 end
       ,aim = aim + case Direction when 'down' then Value when 'up' then -Value else 0 end
       ,n = data.n
  from pos
  inner join data on data.n = pos.n + 1
)

select horizontal * depth 
	  from pos
where n = (select max(n) from pos)
OPTION (MAXRECURSION 5000)
