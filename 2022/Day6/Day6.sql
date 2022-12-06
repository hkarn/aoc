if object_id('tempdb..#input_stage', 'U') is not null  
	drop table #input_stage
if object_id('tempdb..#input', 'U') is not null  
	drop table #input
create table #input_stage
(
    txt nvarchar(max)
)

BULK INSERT #input_stage
FROM 'C:\GitHub\aoc\2022\Day6\input.txt'
WITH
(
  FIRSTROW = 1,
  CODEPAGE = '65001', 
  DATAFILETYPE = 'Char',
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

create table #input (
	 row_n int not null
	,Ch nchar(1) not null
	)

;with r_cte as (
	select n = 1
	      ,Ch = substring(txt,1,1)
		  ,txt
	from #input_stage
	union all
	select n = n + 1
	      ,Ch = substring(txt,n + 1,1)
		  ,txt
	from r_cte

	where n < len(txt)
	)
insert into #input (row_n, Ch)
select row_n = n, Ch from r_cte
option (maxrecursion 0)

--PART 1

;with last4 as (
select 
	 [n] = row_n
	,[a] = Ch
	,[b] = lag(Ch,1) over (order by row_n) 
	,[c] = lag(Ch,2) over (order by row_n) 
	,[d] = lag(Ch,3) over (order by row_n) 
from #input)
select min(n) from last4 
where a not in (b,c,d)
	and b not in (a,c,d)
	and c not in (a,b,d)
	and d not in (a,b,c)


--PART 2

declare @dynSql nvarchar(max)

set @dynSql = (select 
';with lastLetters as (
select 
	 [n] = row_n
	,[1] = Ch')

declare @i int = 2
while @i <= 14
begin
	set @dynSql = @dynSql +	',' + quotename(@i) + ' = lag(Ch,' + cast(@i - 1 as nvarchar(max)) + ') over (order by row_n)'
	set @i = @i +1
end

set @dynSql = @dynSql +	'from #input)
			select min(n) from lastLetters where '

set @i = 1
while @i <= 14
begin
	set @dynSql = @dynSql +	iif(@i <> 1,'and','') + quotename(@i) + 'not in (' 
				+
				(select string_agg(quotename(row_n),',') from #input where row_n <> @i and row_n <= 14)
				+
				')'
	set @i = @i +1
end

exec sp_executesql @dynSql

