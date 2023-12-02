create or alter function [dbo].[tmp_fn_AOC23RemoveChars](@txt nvarchar(1000), @pattern nvarchar(100))
returns nvarchar(1000)
as
begin
    while patindex(@pattern, @txt) > 0
        set @txt = stuff(@txt, patindex(@pattern, @txt), 1, '')
    return @txt
end
go

drop table if exists #input
create table #input
(
    txt nvarchar(max)
)
--unsafe order import
BULK INSERT #input
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

--AOC Day2 etl
drop table if exists #CubeTypes
drop table if exists #GameSets
drop table if exists #SetCubeTypeRelation

create table #CubeTypes (
	 CubeTypeID int identity(1,1) not null
	,Color nvarchar(20) not null
primary key (CubeTypeID)
)

create table #GameSets (
	 GameID int not null
	,SetID int identity(1,1) not null
	,SetTxt nvarchar(255)
primary key(GameID,SetID)
)

create table #SetCubeTypeRelation (
	 SetID int not null
	,CubeTypeID int  not null
	,Number int not null
primary key(SetID,CubeTypeID)
)

insert into #CubeTypes (Color)
select  distinct
	Color = value
	from #input
	cross apply string_split(
		trim(
			replace(
				replace(
					replace(
						replace(
							translate(txt,'123456789:;,', '000000000000')
						, '0', '')
					, 'Game   ','')
				,char(10),'')
			,char(13),'')
		)
	, ' ')
where replace(value, ' ', '') <> ''

;with games as (
select	 GameID = cast(substring(txt,6,charindex(':',txt) - 6) as int)
		,[Sets] = trim(right(txt,len(txt) - charindex(':',txt)))
from #input
)
insert into #GameSets (
	 GameID
	,SetTxt
)
select GameID
	  ,SetTxt = trim(value)
from games
cross apply string_split([Sets], ';')

;with allsets as (
select 
	 GameID		= GameID
	,SetID		= SetID
	,CubeColor	= trim(replace(replace([dbo].[tmp_fn_AOC23RemoveChars](value,'%[0-9]%'),char(13),''),char(10),''))
	,Number		= cast(trim(replace(replace([dbo].[tmp_fn_AOC23RemoveChars](value,'%[A-Z]%'),char(13),''),char(10),'')) as int)
from #GameSets
cross apply string_split([SetTxt], ',')
)
insert into #SetCubeTypeRelation (
	 SetID
	,CubeTypeID
	,Number)
select   SetID		= s.SetID
		,CubeTypeID	= ct.CubeTypeID
		,Number		= s.Number
from allsets s
left join #CubeTypes ct on s.CubeColor = ct.Color

--AOC 2023. Day 2, part 1
select [Sum] = sum(GameID) from (
	select distinct GameID from #GameSets
	except
	select distinct GameID from 
	#GameSets g
	inner join #SetCubeTypeRelation r on r.SetID = g.SetID
	inner join #CubeTypes c on c.CubeTypeID = r.CubeTypeID
	where (r.Number > 12 and c.Color = 'red')
		or (r.Number > 13 and c.Color = 'green')
		or (r.Number > 14 and c.Color = 'blue')
	) t(GameID)

--AOC 2023. Day 2, part 2
;with reqCubes as (
select distinct
	 GameID						= g.GameID
	,CubeTypeID					= c.CubeTypeID
	,requiredNum				= max(r.Number) over (partition by g.GameID, c.CubeTypeID) 
	from #GameSets g
	inner join #SetCubeTypeRelation r on r.SetID = g.SetID
	inner join #CubeTypes c on c.CubeTypeID = r.CubeTypeID
)
,reqCubesZeroCubes as (
select	 GameID						= GameID
		,CubeTypeID					= CubeTypeID
		,requiredNum				= requiredNum
		,multiplyByZero				= iif(count(CubeTypeID) over (partition by GameID) = 3,0 ,1)
from reqCubes
)
,gamePower as (
select	 GameID						= GameID
		,gamePower					= exp(sum(log(requiredNum)))
from reqCubesZeroCubes
where multiplyByZero = 0
group by GameID)

select [Sum2] = sum(gamePower) from gamePower

drop function if exists [dbo].[tmp_fn_AOC23RemoveChars]