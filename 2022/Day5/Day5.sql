drop table if exists #input_stage
create table #input_stage
(
    txt nvarchar(max)
)

--extract. Unsafe order import but fine for aoc
BULK INSERT #input_stage
FROM 'C:\GitHub\aoc\2022\Day5\input.txt'
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

drop table if exists #input
create table #input
(
    id int identity(1,1) not null
   ,txt nvarchar(max)
)

insert into #input (txt)
select (txt) from #input_stage

--transform
drop table if exists #Crates
drop table if exists #Moves
create table #Crates (
	 CrateType nchar(1) not null
    ,RowID int not null
	,ColumnID int not null
	unique (RowID, ColumnID)
)

create table #Moves (
    MoveOrder int not null
   ,MoveAmount int not null
   ,FromColumnID int not null
   ,ToColumnID int not null
)

--get number if columns and charindex (0-9 columns)
drop table if exists  #columnStructure
create table #columnStructure (
	 ColID int not null
	,CIndex int not null
)
insert into #columnStructure
select  ColID = value 
	   ,CIndex = charindex(value,txt)
from #input
cross apply string_split(txt, ' ')
where id = (select id - 1 from #input where txt is null)
and value <> ''

--also remove blanks on top
insert into #Crates (RowID,ColumnID,CrateType)
select   RowID= row_number() over (partition by ColID order by ID)
		,ColumnID = ColID
		,CrateType=trim(replace(replace(substring(txt,CIndex-1,3),'[',''),']',''))
from #input
cross join #columnStructure
where id < (select id-1 from #input where txt is null)
and trim(replace(replace(substring(txt,CIndex-1,3),'[',''),']','')) <> ''

insert into #Moves (
    MoveOrder
   ,MoveAmount
   ,FromColumnID 
   ,ToColumnID
   )
select
 MoveOrder = row_number() over (order by id)
,MoveAmount = cast(substring(txt,6,len(txt) - (len(txt)+6 -  charindex(' from',txt))) as int)
,FromColumnID = cast(substring(txt,charindex(' from',txt)+5,len(txt) - (len(txt)+charindex(' from',txt)+5 -  charindex(' to',txt))) as int)
,ToColumnID = cast(substring(txt,charindex(' to',txt)+3,(len(txt)+charindex(' to',txt)+3)) as int)
from #input
where id > (select id from #input where txt is null)

declare @Move int = (select min(MoveOrder) from #Moves)

--declare @CrateMover int = 9000 --part 1
declare @CrateMover int = 9001 --part 2

while (@Move is not null)
begin
	if (select max(RowID) from #Crates where ColumnID = (select FromColumnID from #Moves where MoveOrder = @Move))
		< 
	   (select MoveAmount from #Moves where MoveOrder = @Move)	
	   throw 51000, 'Not enough boxes to complete move..', 1 
	--make room
	update c set c.RowID = c.RowID + r.MoveAmount from #Crates c
	inner join (select MoveAmount, ToColumnID from #Moves where MoveOrder = @Move) r on r.ToColumnID = c.ColumnID

	--move boxes
	update c set 
		 c.ColumnID = r.ToColumnID --change column
		,c.RowID = iif(@CrateMover = 9000,MoveAmount+1 - c.RowID,c.RowID) --invert order if CrateMover 9000
	from #Crates c
	inner join (select MoveAmount, ToColumnID, FromColumnID from #Moves where MoveOrder = @Move) r on r.FromColumnID = c.ColumnID and RowID <= MoveAmount

	--reorder from column
	update c set 
		c.RowID = c.RowID - MoveAmount
	from #Crates c
	inner join (select MoveAmount, FromColumnID from #Moves where MoveOrder = @Move) r on r.FromColumnID = c.ColumnID

set @Move = (select min(MoveOrder) from #Moves where MoveOrder > @Move)
end

--PART 1

select string_agg(CrateType,'') within group (order by ColumnID, RowID) from #Crates
where RowID = 1


