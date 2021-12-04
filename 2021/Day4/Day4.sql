drop table if exists #draw
drop table if exists #boards

create table #draw (n int, nr int)
insert into #draw (n,nr)
select n = row_number() over (order by (select null))
       ,nr = t.value
from string_split(
'7,4,9,5,11,17,23,2,0,14,21,24,10,16,13,6,15,25,12,22,18,20,8,19,3,26,1'
,',') t

create table #boards (BoardID int, RowID int, ColID int, nr int)
insert into #boards (
     BoardID
    ,RowID
    ,ColID
    ,nr)
select 
     BoardID    = BoardID
    ,RowID      = RowID
    ,ColID      = row_number() over (partition by RowID, BoardID order by n)
    ,nr         = nr
from (
    select   BoardID    = floor((n-1) / 25)
            ,RowID      = ntile(5) over (partition by floor((n-1) / 25) order by n)
            ,nr         = nr
            ,n          = n
    from (
        select 
        n = row_number() over (order by (select null))
        ,nr = v.val 
        from string_split(
        replace(
'22 13 17 11  0
 8  2 23  4 24
21  9 14 16  7
 6 10  3 18  5
 1 12 20 15 19

 3 15  0  2 22
 9 18 13 17  5
19  8  7 25 23
20 11 10 24  4
14 21 16 12  6

14 21 17 24  4
10 16 15  9 19
18  8 23 26 20
22 11 13  6  5
 2  0 12  3  7'
        ,char(13), ' ')
        , ' ') t
        cross apply (select val = replace(replace(t.value, char(10), ''), ' ','')) v(val)
    where v.val <> '') data(n, nr)
) withRowandBoard
order by n



--PART 1
declare @drawcount int = 0
declare @winsum int = null
while @drawcount < (select count(*) from #draw)
begin
     with wincounts as (
    select BoardID, RowID, ColID
    ,winrow = count(b.nr) over (partition by BoardID, RowID) 
    ,wincol = count(b.nr) over (partition by BoardID, ColID) 
    from #boards b
    inner join #draw d on d.nr = b.nr
    where d.n <= @drawcount
    )

    select @winsum = sum(b.nr) * d.nr
    from #boards b
    cross join (select nr = nr from #draw where n = @drawcount) d
    where exists (select 1 from wincounts where (winrow >= 5 or  wincol >= 5) and wincounts.BoardID = b.BoardID)
    and not exists (select 1 from #draw d where d.n <= @drawcount and d.nr = b.nr)
    group by d.nr
if (isnull(@winsum,0) <> 0) set @drawcount = (select count(*) from #draw)
set @drawcount =  @drawcount + 1
end

select @winsum


--PART 2
set @drawcount = (select count(*) from #draw)
set @winsum = null
while @drawcount > 0
begin
     with wincounts as (
    select BoardID, RowID, ColID
    ,winrow = count(b.nr) over (partition by BoardID, RowID) 
    ,wincol = count(b.nr) over (partition by BoardID, ColID) 
    from #boards b
    inner join #draw d on d.nr = b.nr
    where d.n <= @drawcount
    )

    select @winsum = sum(b.nr) * d.nr
    from #boards b
    cross join (select nr = nr from #draw where n = @drawcount+1) d
    where BoardID not in (select BoardID from wincounts where winrow >= 5 or wincol >= 5)
    and not exists (select 1 from #draw d where d.n <= @drawcount+1 and d.nr = b.nr)
    group by d.nr

    if (isnull(@winsum,0) <> 0) set @drawcount = 0
set @drawcount =  @drawcount - 1
end

select @winsum
