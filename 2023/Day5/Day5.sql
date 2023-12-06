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

drop table if exists #input
create table #input (
	  id int not null identity(1,1) primary key
	, txt nvarchar(max) not null
)

drop table if exists #seeds
drop table if exists #almanac

create table #seeds (
	  id int not null identity(1,1)
	, seed int not null primary key)
create table #almanac (
	  srcType nvarchar(20) not null
	, tgtType nvarchar(20) not null
	, tgtID int not null
	, srcID int not null
	, r int not null
	, offset as ( tgtID - srcID )
	primary key(tgtID,srcType,tgtType))

insert into #input (txt)
select trim(replace(
		 replace(txt,char(10),'')
	  ,char(13),''))
from #input_stage

select * from #input

insert into #seeds (seed)
select seed = cast(trim(value) as int)
from #input 
cross apply string_split(substring(txt,8,len(txt)),' ')
where id = 1

declare  @pointer int = (select min(id) from #input where id > 1) 
		,@srcType nvarchar(20) = null
		,@tgtType nvarchar(20) = null
		,@txt nvarchar(max) = null

while @pointer is not null 
begin
	set @txt = (select trim(txt) from #input where id = @pointer)
	if @txt <> '' and @txt like N'%map%'
		begin
			set @srcType = trim(substring(@txt,0,charindex(N'-',@txt)))
			set @tgtType = trim(left(substring(@txt,len(@txt) - charindex(N'-',reverse(@txt)) + 2,len(@txt))
											, charindex(N' ',substring(@txt,len(@txt) - charindex(N'-',reverse(@txt)) + 2,len(@txt)))
											))
		end
	if @txt <> '' and len(@txt) > 3 and @txt not like N'%map%'
	begin
		insert into #almanac (
					  srcType
					, tgtType
					, tgtID
					, srcID
					, r)
		select		  srcType	= @srcType
					, tgtType	= @tgtType
					, tgtID		= trim(left(@txt,charindex(N' ', @txt)))
					, srcID		= trim(substring(@txt,charindex(N' ',@txt),charindex(N' ',@txt,charindex(N' ',@txt)+1) - charindex(N' ',@txt)))
					, r			= trim(right(@txt,charindex(N' ', reverse(@txt))))
		from #input
		where id = @pointer
	end
	set @pointer = (select min(id) from #input where id > @pointer) 
end

drop table if exists #numbers

;with cte as (
	select n = 1
	union all
	select n = n + 1 from cte
	where n < 1000
	)
select n 
into #numbers from cte
option (maxrecursion 1000)

drop table if exists #fullMap 
create table #fullMap 
	( srcT nvarchar(20) not null
	, tgtT nvarchar(20) not null
	, tgtN int not null
	, srcN int not null)

insert into #fullMap

select srcT	= srcType
	  ,tgtT	= tgtType
	  ,srcN	= n.n
	  ,tgtN = isnull(n.n + a.offset, n.n)
	from #numbers n
full join #almanac a on n.n >= a.srcID and n.n < (a.srcID + a.r)
where a.srcType is not null


select *
	  from #seeds s
full join #almanac a on s.seed >= a.srcID and s.seed <= a.srcID + a.r and a.srcType = 'seed'
where isnull(a.srcType, 'seed') = 'seed'

select * from #seeds
inner join #almanac a2 on a1.tgtType = a2.srcType
inner join #almanac a3 on a2.tgtType = a3.srcType
inner join #almanac a4 on a3.tgtType = a4.srcType
inner join #almanac a5 on a4.tgtType = a5.srcType
inner join #almanac a6 on a5.tgtType = a6.srcType
inner join #almanac a7 on a6.tgtType = a7.srcType
where a1.srcType = 'seed'



select * from  #almanac a1
inner join #almanac a2 on a1.tgtType = a2.srcType
inner join #almanac a3 on a2.tgtType = a3.srcType
inner join #almanac a4 on a3.tgtType = a4.srcType
inner join #almanac a5 on a4.tgtType = a5.srcType
inner join #almanac a6 on a5.tgtType = a6.srcType
inner join #almanac a7 on a6.tgtType = a7.srcType
where a1.srcType = 'seed'