drop table if exists #input
create table #input
(
  Calories nvarchar(max)
)


BULK INSERT #input
FROM 'C:\GitHub\aoc\2022\Day1\input.csv'
WITH
(
  FIRSTROW = 1,
  CODEPAGE = '65001', 
  DATAFILETYPE = 'Char',
  FIELDTERMINATOR = ';',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

drop table if exists #Calories
create table #Calories
(
  ID       int identity(1, 1)
, Calories int null
, ElfID    int null
)

-- Part 1
insert into #Calories (Calories) 
select Calories from #input

drop table if exists #CaloriesGrouped
;with grp
as (select *
		, breakp = iif(Calories is null, ID, null) --ID only where null
	from #Calories)

,grouped
as (select Calories
         , ElfID = isnull(
				max(breakp) over (order by ID ROWS UNBOUNDED PRECEDING) --max non-null breakp before current row
				, 1) --first group as 1
    from grp)


select * into #CaloriesGrouped from grouped

select max(Calories)
from
(
  select Calories = sum(Calories) over (partition by ElfID)
  from #CaloriesGrouped
) t

-- Part 2


;with summed as (
	select ElfId,CaloriesSum= sum(Calories) over (partition by ElfID)
	from #CaloriesGrouped)

,ranked as (
select distinct CaloriesSum, [rank] = dense_rank() over (order by CaloriesSum desc) from summed
)

select sum(CaloriesSum) from ranked where [rank] <= 3


