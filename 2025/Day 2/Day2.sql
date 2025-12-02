set xact_abort, nocount on;

drop table if exists #input_stage
create table #input_stage
(
	txt nvarchar(max)
)
--unsafe order import
bulk insert #input_stage
from '/input_data/input.txt'
with
(
  firstrow = 1,
  datafiletype = 'Char',
  --fieldterminator = '',
  rowterminator = ',',
  format = 'csv',
  tablock,
  keepnulls,
  keepidentity
)

drop table if exists #inputClean

select rangeStart	= cast(left(nowhitespace.txt,dashLocation.i-1) as bigint)
	  ,rangeEnd		= cast(substring(nowhitespace.txt,dashLocation.i+1,len(nowhitespace.txt)-dashLocation.i) as bigint)
into #inputClean
from #input_stage
cross apply (select txt = trim(translate(txt,char(13)+char(10)+char(9)+' ','    '))) nowhitespace
cross apply (select i = charindex('-',nowhitespace.txt)) dashLocation

drop table if exists #allIDs

select
     id = gs.value
	,id_len = len(gs.value)
into #allIDs
from #inputClean a
cross apply generate_series(a.rangeStart, a.rangeEnd, cast(1 as bigint)) as gs;


--Answer 1
select sum(id) 
from #allIDs
where id_len % 2 = 0
  and id % (power(10, id_len / 2) + 1) = 0

--Answer 2
go
create function dbo.IsInvalidID(@id bigint)
returns bit
as
begin
    declare @id_str nvarchar(20) = cast(@id as nvarchar(20));
    declare @length int = len(@id_str);
    declare @doubled nvarchar(40) = @id_str + @id_str;
    declare @pos int = 1;
    declare @pattern_len int
    declare @reps int
    while @pos < @length
    begin
        if substring(@doubled, @pos + 1, @length) = @id_str
        begin
            set @pattern_len= @pos;
            set @reps = @length / @pattern_len;

            if @length % @pattern_len = 0 and @reps >= 2
                return 1;
        end;
        set @pos = @pos + 1;
    end;

    return 0;
end;
go
select sum(id) 
from #allIDs
where dbo.IsInvalidID(id) = 1
go
drop function dbo.IsInvalidID;
