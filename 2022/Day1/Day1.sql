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
)

insert into #Calories (Calories) select Calories from #input;
with grp
as (select *, breakp = iif(Calories is null, ID, null)from #Calories)
   , grouped
as (select Calories
         , ElfID = isnull(max(breakp) over (order by ID ROWS UNBOUNDED PRECEDING), 1) --last non null
    from grp)
select max(Calories)
from
(
  select Calories = sum(Calories) over (partition by ElfID)
  from grouped
) t


