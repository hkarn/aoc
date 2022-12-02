
drop table if exists #input
CREATE TABLE #input (
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
CREATE TABLE #Calories (
	ID int identity(1,1),
	Calories int null,
	ElfID int null
)

insert into #Calories (Calories)
select Calories from #input

declare @pointer int = (select min(ID) from #Calories)
declare @counter int = 1
while @pointer is not null
	begin
		update #Calories set ElfID = @counter where ID = @pointer
		if(select Calories from #Calories where ID = @pointer) is null 
			set @counter = @counter + 1
		set @pointer = (select min(ID) from #Calories where ID > @pointer)
	end

select max(Calories) from (
	select Calories = sum(Calories) over (partition by ElfID) from #Calories
) t

