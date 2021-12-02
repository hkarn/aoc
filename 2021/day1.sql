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
  
,withLast as (
  select value, lastValue = lag(value,1) over (order by n) from data
  )

select count(*) from withLast where value > lastValue


