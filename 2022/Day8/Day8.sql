drop table if exists #input_stage
create table #input_stage
(
  txt nvarchar(max)
)

--extract. Unsafe order import but fine for aoc
BULK INSERT #input_stage
FROM 'C:\GitHub\aoc\2022\Day8\input.txt'
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
  RowID        int not null
, ColID        int not null
, Height       int not null
, VisibleRange int null
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

declare @col int =
        (
          select min(ColID)from #grid
        )
declare @row int =
        (
          select min(RowID)from #grid
        )
drop type if exists AOCrange
go
create type AOCrange as table
(
  ID     int
, Height int
)
go
drop function if exists dbo.AOCVisibleRange
go
create function dbo.AOCVisibleRange
(
  @seq AOCrange readonly
)
returns int
as
begin
  if (select count(*)from @seq) <= 1 return 0 --break: we are at a border

  declare @result int = 0
  declare @orgHeight int =
          (
            select top (1) Height from @seq order by ID
          )
  declare @heightMatchedFlag bit = 0
  declare @p int =
          (
            select min(ID)from @seq
          )
  set @p =
  (
    select min(ID)from @seq where ID > @p
  ) --start comparison one three out
  while @p is not null and @heightMatchedFlag = 0
  begin
    if @orgHeight <=
    (
      select Height from @seq where ID = @p
    )
      set @heightMatchedFlag = 1

    set @result = @result + 1
    set @p =
    (
      select min(ID)from @seq where ID > @p
    )
  end
  return @result
end
go
declare @AOCrange as AOCrange


declare @Col int =
        (
          select min(ColID)from #grid
        )
declare @Row int
declare @VisibleR int

while @Col is not null
begin
  set @Row =
  (
    select min(RowID)from #grid where ColID = @Col
  )
  while @Row is not null
  begin
    set @VisibleR = 1

    --move left
    delete from @AOCrange
    insert into @AOCrange
    (
      ID
    , Height
    )
    select ID = row_number() over (order by ColID desc)
         , Height
    from #grid
    where RowID = @Row
      and ColID <= @Col
    select @VisibleR = @VisibleR * dbo.AOCVisibleRange(@AOCrange)

    --move right
    delete from @AOCrange
    insert into @AOCrange
    (
      ID
    , Height
    )
    select ID = row_number() over (order by ColID asc)
         , Height
    from #grid
    where RowID = @Row
      and ColID >= @Col
    select @VisibleR = @VisibleR * dbo.AOCVisibleRange(@AOCrange)

    --move up
    delete from @AOCrange
    insert into @AOCrange
    (
      ID
    , Height
    )
    select ID = row_number() over (order by RowID desc)
         , Height
    from #grid
    where RowID <= @Row
      and ColID = @Col
    select @VisibleR = @VisibleR * dbo.AOCVisibleRange(@AOCrange)

    --move down
    delete from @AOCrange
    insert into @AOCrange
    (
      ID
    , Height
    )
    select ID = row_number() over (order by RowID asc)
         , Height
    from #grid
    where RowID >= @Row
      and ColID = @Col
    select @VisibleR = @VisibleR * dbo.AOCVisibleRange(@AOCrange)

    update #grid
    set VisibleRange = @VisibleR
    where ColID = @Col
      and RowID = @Row

    set @Row =
    (
      select min(RowID)from #grid where RowID > @Row and ColID = @Col
    )
  end
  set @Col =
  (
    select min(ColID)from #grid where ColID > @Col
  )
end

select max(VisibleRange)from #grid



go
drop function if exists dbo.AOCSepString
drop function if exists dbo.AOCVisibleRange
drop type if exists AOCrange