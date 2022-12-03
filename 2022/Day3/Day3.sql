drop table if exists #input
create table #input
(
   Content nvarchar(max) collate SQL_Latin1_General_CP1_CS_AS
)


BULK INSERT #input
FROM 'C:\GitHub\aoc\2022\Day3\input.txt'
WITH
(
  FIRSTROW = 1,
  CODEPAGE = '65001', 
  DATAFILETYPE = 'Char',
  FIELDTERMINATOR = ' ',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

--select * from #input
drop table if exists #evenSplit
create table #evenSplit (id int identity(1,1), s1 nvarchar(max), s2 nvarchar(max))
insert into #evenSplit (s1, s2)
select left(Content,len(Content)/2), right(Content,len(Content)/2)
from #input

drop function if exists dbo.AOCTransform
go
create function dbo.AOCTransform(@string nvarchar(max))
returns @numbers table (number int)
as
begin
	declare  @alfabet nvarchar(max) = 'abcdefghijklmnopqrstuvwxyz'
	        ,@count int = 1
			,@total int = (select len(@string))
			,@char nchar(1)
			,@pos int
	while @count <= @total
	begin
		set @char = (select substring(@string, @count, 1))
		if (select @char collate SQL_Latin1_General_CP1_CS_AS) = (select upper(@char) collate SQL_Latin1_General_CP1_CS_AS)
			set @pos = 26 + (select charindex(@char, @alfabet))
		else
			set @pos = 0 + (select charindex(@char, @alfabet))
		
		insert into @numbers (number)
		select @pos

		set @count = @count + 1
	end
	return
end
go

drop table if exists #numberd
create table #numberd (id int, n1 int, n2 int)

declare @count int = (select min(id) from #evenSplit)
while @count is not null
begin
	insert into #numberd (id, n1, n2)
	select id = @count
			,n1 = n1.number
			,n2 = n2.number
	from #evenSplit i
	cross join (select id = row_number() over (order by number), number from dbo.AOCTransform((select s1 from #evenSplit where id = @count))) n1
	inner join (select id = row_number() over (order by number), number from dbo.AOCTransform((select s2 from #evenSplit where id = @count))) n2 on n1.id=n2.id
	where i.id = @count
	set @count = (select min(id) from #evenSplit where id > @count)
end

--PART 1 - answer
select sum(n1) from (
	select distinct a.id,a.n1 from #numberd a
	where exists (select 1 from #numberd b where a.id=b.id and a.n1 = b.n2 )
	group by a.id,a.n1
) t

--PART 2
drop table if exists #joinedNumbered
create table #joinedNumbered (id int,grp int, n1 int)

insert into #joinedNumbered (id,grp,n1)
select id,grp = floor((id-1)/3),n1 from #numberd
union all
select id,grp = floor((id-1)/3),n2 from #numberd

select sum(n1) from (
select distinct a.id,a.n1 from #joinedNumbered a
inner join #joinedNumbered b on a.grp = b.grp and a.n1 = b.n1 and a.id = b.id +1
inner join #joinedNumbered c on a.grp = c.grp and a.n1 = c.n1 and b.id = c.id +1
) t


go
drop function if exists dbo.AOCTransform

