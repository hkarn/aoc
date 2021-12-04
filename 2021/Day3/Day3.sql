if object_id('maindb2.dbo.BinaryToDecimalTMP', 'FN') is not null
    drop function BinaryToDecimalTMP
if object_id('tempdb..#data', 'U') is not null
    drop table #data

--DATA IMPORT
create table #data (rownr smallint, base2val char(12))
insert into #data
(rownr,base2val)
select 
   rownr			= cast(row_number() over (order by (select null)) as int)
  ,base2val		    = cast(v.val as char(12))
  from STRING_SPLIT(
  '001111011011
.....
110011100110', char(13)) t
  cross apply (select val = replace(t.value, char(10), '')) v(val)

go

create function dbo.BinaryToDecimalTMP (@base2 varchar(max))
returns int
begin
    declare @length int = null
    declare @position int = 1
    declare @base10 int = 0
    set @length = (select len(@base2))
    while @position <= @length
        begin
        set @base10 = (@base10 * 2) + (select cast(substring(@base2,@position,1) as tinyint))
        set @position = @position + 1
        end
    return @base10
end

go

--PART 1
;with gammarate as (select gr = concat(convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 2048 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 1024 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 512 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 256 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 128 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 64 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 32 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 16 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 8 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 4 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 2 > 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 1 > 0 then 1.0 else 0.0 end),0))
 )
 from #data)

,epsilonrate as (select er = concat(convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 2048 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 1024 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 512 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 256 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 128 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 64 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 32 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 16 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 8 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 4 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 2 = 0 then 1.0 else 0.0 end),0))
,convert(int,round(avg(case when dbo.BinaryToDecimalTMP(base2val) & 1 = 0 then 1.0 else 0.0 end),0))
 )
 from #data)
--if you know a way to use bitwise to flip the result instead let me know, that worked fine on a tinyint but the signed ones got me :)


select  dbo.BinaryToDecimalTMP(gr) * dbo.BinaryToDecimalTMP(er)
from gammarate
cross join epsilonrate

--PART 2
declare @rowcount int = null
declare @position int = 2048
declare @filterox bit = null
declare @filterco2 bit = null
declare @resultsox table (val char(12))
insert into @resultsox (val) select base2val from #data
declare @resultsco2 table (val char(12))
insert into @resultsco2 (val) select base2val from #data
while @rowcount > 1 or @rowcount is null
begin
    set @filterox = (select convert(bit,round(avg(case when dbo.BinaryToDecimalTMP(val) & @position > 0 then 1.0 else 0.0 end),0)) from @resultsox)
    set @filterco2 = (select convert(bit,round(avg(case when dbo.BinaryToDecimalTMP(val) & @position > 0 then 1.0 else 0.0 end),0)) from @resultsco2)
    if (select count(*) from @resultsox) > 1
        delete from @resultsox
            where (dbo.BinaryToDecimalTMP(val) <> dbo.BinaryToDecimalTMP(val) | @position and @filterox = 1)
            or (dbo.BinaryToDecimalTMP(val) = dbo.BinaryToDecimalTMP(val) | @position and @filterox = 0)
    if (select count(*) from @resultsco2) > 1
        delete from @resultsco2
            where (dbo.BinaryToDecimalTMP(val) <> dbo.BinaryToDecimalTMP(val) | @position and ~@filterco2 = 1)
            or (dbo.BinaryToDecimalTMP(val) = dbo.BinaryToDecimalTMP(val) | @position and ~@filterco2 = 0)
    set @rowcount = (select max(c.c) from (select c = count(*) from @resultsox union all select c = count(*) from @resultsco2) c)
    if @position = 1 set @rowcount = 1 --break infinate loop
    set @position = @position / 2
end

select answer2 = dbo.BinaryToDecimalTMP(a.val) * dbo.BinaryToDecimalTMP(b.val)
from @resultsox a
cross join @resultsco2 b

if object_id('tempdb..#data', 'U') is not null
    drop table #data
if object_id('maindb2.dbo.BinaryToDecimalTMP', 'FN') is not null
    drop function BinaryToDecimalTMP
