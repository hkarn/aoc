with data as (
  select n = row_number() over (order by (select null)),
  value = cast(replace(value, char(10), '') as int)
  from string_split( 
  --example input
  '199
  200
  208
  210
  200
  207
  240
  269
  260
  263'
  --example input
  , char(13)) 
  )

--END DATA IMPORT
 
-- PART 1
,withLast as (
  select value, lastValue = lag(value,1) over (order by n) from data
)
select count(*) from withLast where value > lastValue


-- PART 2
,withLast as (
select value
,AnextValue = lead(value,1) over (order by n)
,BnextValue = lead(value,2) over (order by n)
,CnextValue = lead(value,3) over (order by n) 
from data
)
select count(*) 
from withLast 
 where CnextValue is not null
 and (value + AnextValue + BnextValue) <
 (AnextValue + BnextValue + CnextValue)
