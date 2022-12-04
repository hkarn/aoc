drop table if exists #input
create table #input
(
  r1 nvarchar(max)
, r2 nvarchar(max)
)


BULK INSERT #input
FROM 'C:\GitHub\aoc\2022\Day4\input.txt'
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

--select * from #input 

declare @maxSection int =
        (
          select max(value)
          from
          (select r = r1 from #input union select r = r2 from #input) t
            cross apply string_split(r, '-')
        )

drop table if exists #numberFiller;
with cte
as (select n = 1
    union all
    select n = n + 1 from cte where n < @maxSection)
select n into #numberFiller from cte
option (maxrecursion 0)

drop table if exists #assignments
create table #assignments
(
  ElfPairID    int not null
, RangeID      int not null
, Section      int not null
, SectionStart int not null
, SectionEnd   int not null
)

--add elves
;
with withElfId
as (select ElfPairID = row_number() over (order by @maxSection) --dummy order
         , r1
         , r2
    from #input)
   --add range key
   , withRangeID
as (select ElfPairID
         , RangeID = RangeID
         , [Range] = RangeIDValue
    from withElfId e
      cross apply
    ( --unpivot using values
      values
        (1, r1)
      , (2, r2)
    )              c (RangeID, RangeIDValue) )
   --break down ranges
   , withRangeStartEnd
as (select ElfPairID
         , RangeID
         , SectionStart = substring([Range], 0, charindex('-', [Range]))
         , SectionEnd   = substring([Range], charindex('-', [Range]) + 1, len([Range]) - charindex('-', [Range]) + 1)
    from withRangeID r)
insert into #assignments
(
  ElfPairID
, RangeID
, Section
, SectionStart
, SectionEnd
)
select ElfPairID = r.ElfPairID
     , RangeID   = r.RangeID
     , Section   = n.n
     , SectionStart
     , SectionEnd
from withRangeStartEnd r
  inner join
  (
    select ElfPairID
         , RangeID
         , n
    from #numberFiller
      cross join withRangeID
  )                    n
    on n.ElfPairID = r.ElfPairID
   and n.RangeID   = r.RangeID
   and n           >= r.SectionStart
   and n           <= r.SectionEnd


--PART 1 answer
select count(*)
from
(
  select distinct
         a = a.ElfPairID
       , b = b.ElfPairID
  from #assignments         a
    inner join #assignments b
      on a.ElfPairID    = b.ElfPairID
     and a.SectionStart >= b.SectionStart
     and a.SectionEnd   <= b.SectionEnd
     and a.RangeID      <> b.RangeID
) t


