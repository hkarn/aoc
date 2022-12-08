drop table if exists #input_stage
create table #input_stage
(
  txt nvarchar(max)
)

--extract. Unsafe order import but fine for aoc
BULK INSERT #input_stage
FROM 'C:\GitHub\aoc\2022\Day8\input-example.txt'
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

drop table if exists #input
create table #input
(
  id  int           identity(1, 1) not null
, txt nvarchar(max) not null
)

insert into #input (txt) select txt from #input_stage s

go
drop function if exists dbo.AOCSepString
go
create function dbo.AOCSepString
(
  @str nvarchar(max)
)
returns nvarchar(max)
as
begin
  declare @result nvarchar(max)
  set @result =
  (
    select string_agg(substring(@str, n.number, 1), ';')within group(order by n.number)
    from master..spt_values n
    where n.type = 'P'
      and n.number
  between 1 and len(@str)
  )
  return @result
end

go

update #input set txt = dbo.AOCSepString(txt)

drop table if exists #grid
create table #grid
(
  RowID  int not null
, ColID  int not null
, Height int not null
    primary key (
                  RowID
                , ColID
                )
)

insert into #grid
(
  RowID
, ColID
, Height
)
select RowID  = id
     , ColID  = ordinal
     , Height = value
from #input
  cross apply string_split(txt, ';', 1)

--PART 1

;
with gridOffset
as (select RowID
         , ColID
         , Height
         , rowBefore = lag(Height, 1) over (partition by RowID order by ColID)
         , rowAfter  = lead(Height, 1) over (partition by RowID order by ColID)
         , colBefore = lag(Height, 1) over (partition by ColID order by RowID)
         , colAfter  = lead(Height, 1) over (partition by ColID order by RowID)
    from #grid)
   , gridMax
as (select RowID
         , ColID
         , Height
         , rowBeforeMax = max(rowBefore) over (partition by RowID
                                               order by ColID
                                               rows between unbounded preceding and current row
                                              )
         , rowAfterMax  = max(rowAfter) over (partition by RowID
                                              order by ColID
                                              rows between current row and unbounded following
                                             )
         , colBeforeMax = max(colBefore) over (partition by ColID
                                               order by RowID
                                               range between unbounded preceding and current row
                                              )
         , colAfterMax  = max(colAfter) over (partition by ColID
                                              order by RowID
                                              range between current row and unbounded following
                                             )
    from gridOffset)
select count(*)
from gridMax
where rowBeforeMax is null
   or rowAfterMax is null
   or colBeforeMax is null
   or colAfterMax is null
   or Height > rowBeforeMax
   or Height > rowAfterMax
   or Height > colBeforeMax
   or Height > colAfterMax

--PART 2

--look left
select *
from #grid         a
  inner join #grid b
    on a.RowID = b.RowID
   and a.ColID > b.ColID


go
drop function if exists dbo.AOCSepString