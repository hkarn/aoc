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
--if you know a way to use bitwize not to flip the result instead let me know, that the smallint is signed and too long got me :)

select  dbo.BinaryToDecimalTMP(gr) * dbo.BinaryToDecimalTMP(er)
from gammarate
cross join epsilonrate

if object_id('tempdb..#data', 'U') is not null
    drop table #data
if object_id('maindb2.dbo.BinaryToDecimalTMP', 'FN') is not null
    drop function BinaryToDecimalTMP
