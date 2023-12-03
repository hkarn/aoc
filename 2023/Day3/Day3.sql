create or alter function [dbo].[tmp_fn_AOC23ReplaceChars](@txt nvarchar(1000), @pattern nvarchar(100), @replace nchar(1))
returns nvarchar(1000)
as
begin
	if patindex(@pattern, @replace) > 0 return null --'@replace char can''t exists in @pattern.'  
    while patindex(@pattern, @txt) > 0
        set @txt = stuff(@txt, patindex(@pattern, @txt), 1, @replace)
    return @txt
end
go

set nocount on;

drop table if exists #input_stage
create table #input_stage
(
	txt nvarchar(max)
)
--unsafe order import
BULK INSERT #input_stage
FROM '/input_data/input.txt'
WITH
(
  FIRSTROW = 1,
  DATAFILETYPE = 'Char',
  --FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

--Day 3, part 1
--ETL
drop table if exists #input
create table #input
(
	 id int identity(1,1) not null primary key
	,txt nchar(140) not null
)

insert into #input (txt)
select txt = replace(
				replace(
						replace(txt, N'.', N'_') 
				,char(10), '')
			,char(13), '')
from #input_stage

update #input set txt = replace(txt, ' ', '_') --fixed length, fill blanks
update #input set txt = [dbo].[tmp_fn_AOC23ReplaceChars](txt,'%[^0-9_x]%','x')

drop table if exists #grid
create table #grid (
	 x int not null
	,y int not null
	,symbol nchar(1) not null
	,numberid int null
	primary key(x,y))

drop table if exists #numbers
create table #numbers (
	 numberid int not null identity(1,1)
	,number int null
primary key(numberid))

insert into #numbers (number) select null

declare @x int = 1
declare @y int = 1
declare @row nchar(140) = (select txt from #input where id = @y)
declare @symbol nchar(1) = null
declare @number int = null
declare @numberID table (id int)

while @row is not null
begin
	set @x = 1
	set @symbol = (select substring(txt,@x,1) from #input where id = @y)
	while @symbol <> ''
	begin
		set @number = null
		if (select patindex('%[0-9]%',@symbol)) = 1
		begin
			if (select patindex('%[0-9]%',substring(@row,@x-1,1))) = 1 --continuing number
				set @number = (select number from #numbers n inner join #grid g on n.numberid =g.numberid
										where g.x = @x - 1 and g.y = @y)
			else
				begin
					set @number = (select substring(@row,@x,iif(charindex('_',replace(@row,'x','_'),@x) = 0,len(@row)+1,charindex('_',replace(@row,'x','_'),@x)) -@x))
					delete from @numberID
					insert into #numbers (number)  
					output inserted.numberid into @numberID
					select number = @number
				end
		end
		insert into #grid (
				 x
				,y
				,symbol
				,numberid)
		select	 x			= @x
				,y			= @y
				,symbol		= @symbol
				,numberID	= iif(@number is null, -1, isnull((select top 1 id from @numberID), -1))
		set @x = @x + 1
		set @symbol = (select substring(txt,@x,1) from #input where id = @y)
	end
set @y = @y + 1
set @row = (select txt from #input where id = @y)
end

--Answer:

select sum(number) from #numbers where numberid in (
select distinct g2.numberid from #grid g1 
inner join #grid g2 on g2.x between g1.x-1 and g1.x+1
				   and g2.y between g1.y-1 and g1.y+1
where g1.symbol = 'x')

--Day 3, part 2 ---TODO: NOT SOLVED
--ETL

--truncate table #input

--insert into #input (txt)
--select txt = replace(
--				replace(
--						replace(txt, N'.', N'_') 
--				,char(10), '')
--			,char(13), '')
--from #input_stage

--update #input set txt = replace(txt, ' ', '_') --fixed length, fill blanks
--update #input set txt = [dbo].[tmp_fn_AOC23ReplaceChars](txt,'%[^0-9_*]%','_')

--truncate table #grid 
--truncate table #numbers

--insert into #numbers (number) select null

--set @x = 1
--set @y = 1
--set @row = (select txt from #input where id = @y)
--set @symbol = null
--set @number = null
--delete from @numberID

--while @row is not null
--begin
--	set @x = 1
--	set @symbol = (select substring(txt,@x,1) from #input where id = @y)
--	while @symbol <> ''
--	begin
--		set @number = null
--		if (select patindex('%[0-9]%',@symbol)) = 1
--		begin
--			if (select patindex('%[0-9]%',substring(@row,@x-1,1))) = 1 --continuing number
--				set @number = (select number from #numbers n inner join #grid g on n.numberid =g.numberid
--										where g.x = @x - 1 and g.y = @y)
--			else
--				begin
--					set @number = (select substring(@row,@x,iif(charindex('_',replace(@row,'*','_'),@x) = 0,len(@row)+1,charindex('_',replace(@row,'*','_'),@x)) -@x))
--					delete from @numberID
--					insert into #numbers (number)  
--					output inserted.numberid into @numberID
--					select number = @number
--				end
--		end
--		insert into #grid (
--				 x
--				,y
--				,symbol
--				,numberid)
--		select	 x			= @x
--				,y			= @y
--				,symbol		= @symbol
--				,numberID	= iif(@number is null, -1, isnull((select top 1 id from @numberID), -1))
--		set @x = @x + 1
--		set @symbol = (select substring(txt,@x,1) from #input where id = @y)
--	end
--set @y = @y + 1
--set @row = (select txt from #input where id = @y)
--end

----Answer:

--select sum(n) from (
--select n = exp(sum(log(number)))  from (

--select distinct x2 = g2.x
--			   ,y2 = g2.y
--			   ,symbol = g2.symbol
--			   ,numberid = g2.numberid
--			   ,x1 = g1.x
--			   ,y1 = g1.y
--,a = count((g2.x*10000)+g2.y) over (partition by g1.x,g1.y)
--from #grid g1 
--inner join #grid g2 on g2.x between g1.x-1 and g1.x+1
--				   and g2.y between g1.y-1 and g1.y+1
--where g1.symbol = '*' and g2.symbol <> '_'
--) t
--inner join #numbers n on n.numberid = t.numberid
--where a = 3
--group by x1, y1
--)t

drop function if exists [dbo].[tmp_fn_AOC23ReplaceChars]
