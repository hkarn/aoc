set xact_abort, nocount on;

drop table if exists #input_stage
create table #input_stage
(
	 LineID int not null identity(1,1) primary key
	,txt nvarchar(max) null
)
insert into #input_stage(txt)
select txt from 
openrowset(
	 bulk  '/input_data/input.txt'
	,fieldterminator = ''
	,rowterminator = '\r\n'
	,format = 'csv'
	,firstrow = 1
) with (txt nvarchar(max)
) input

drop table if exists #inputClean;

with numbers as (
  select n = 1
		,maxLn = (select max(len(txt)) from #input_stage)
  union all
  select n = n + 1
	    ,maxLn
  from numbers 
  where n < maxLn
)
select 
   yID = s.LineID
  ,xID = n
  ,Content = substring(s.txt, n, 1)
into #inputClean
from #input_stage s
cross join numbers 
where n <= len(s.txt)
option (maxrecursion 0);

with neighbours as (
select Orig_yID = o.yID
	  ,Orig_xID = o.xID
	  ,HasRoll = case when o.Content = '@' then 1 else 0 end
	  ,NeighbourHasRoll = case when p.Content = '@' then 1 else 0 end
from #inputClean o
inner join #inputClean p on (p.yID >= o.yID - 1 and p.yID <= o.yID + 1)
						and (p.xID >= o.xID - 1 and p.xID <= o.xID + 1)
						and not (p.yID = o.yID and p.xID = o.xID)
where o.Content = '@'
)
,nSum as (
select yID = Orig_yID
      ,xID = Orig_xID
	  ,rollSum = sum(NeighbourHasRoll)
from neighbours
--where HasRoll = 1
group by Orig_yID,Orig_xID
)
select count(*) from nSum
where rollSum < 4
