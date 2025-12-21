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
  fieldterminator = '',
  rowterminator = '\r\n',
  format = 'csv',
  tablock,
  keepnulls,
  keepidentity
)

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
   BankID = s.BankID
  ,BatteryID = n
  ,Joltage = substring(s.txt, n, 1)
into #inputClean
from (select BankID = row_number() over (order by (select 1)), txt from #input_stage) s
cross join numbers 
where n <= len(s.txt)
option (maxrecursion 0);

/*Answer 1*/

with mxLn as (
select 
	 BankID
	,Joltage
	,BatteryID
	,ln = max(BatteryID) over (partition by BankID)
from #inputClean 
)
,rnked as (
select 
	 BankID
	,Joltage
	,BatteryID
	,rnk = row_number() over (partition by BankID order by 		Joltage desc
										   ,BatteryID asc)
from mxLn 
where BatteryID < ln
)
,BatA as (
select BatA = Joltage
	  ,BatteryID = BatteryID
	  ,BankID = BankID
from rnked
where rnk = 1)
,BatB as (
select BatB = max(Joltage)
	  ,BankID = s.BankID
from #inputClean s
inner join BatA a on s.BankID = a.BankID and s.BatteryID > a.BatteryID
group by s.BankID)
select sum(cast(concat(BatA,BatB) as int))
from BatA a
inner join BatB b on a.BankID = b.BankID 

--Part 2
drop table if exists #State
create table #State( BankID int primary key, LastIx int)
drop table if exists #Result
create table #Result (BankID int, Pos int, Joltage int)

insert into #State (BankID, LastIx)
select distinct BankID, 0 from #inputClean 

/* GREEDY LOOP. All banks are 100 long. We need to preserver 12 digits. Look to <= 89 */
declare @Step int = 1
       ,@TotalSteps int = 12
       ,@TotalLength int = 100;

while @Step <= @TotalSteps
begin
with ranked as (
select
     i.BankID
    ,i.BatteryID
    ,i.Joltage
    ,rnk = row_number() over (partition by i.BankID order by i.Joltage desc, i.BatteryID asc)
from #inputClean i
join #State s on s.BankID = i.BankID
where i.BatteryID > s.LastIx
  and i.BatteryID <= @TotalLength - (@TotalSteps - @Step)
)
,best as (
select BankID, Joltage, BatteryID
from ranked where rnk = 1
)
merge #state as tgt
using best as src
on tgt.BankID = src.BankID
when matched then
update set LastIx = src.BatteryID
output src.BankID,@Step,src.Joltage into #Result(BankID, Pos, Joltage);

set @Step = @Step + 1;
end

with a as (
select
    BankID,
    BankValue = cast(string_agg(cast(Joltage as varchar(1)), '') within group (order by Pos) as bigint)
from #Result
group by BankID
)
select Part2_Answer = sum(BankValue)
from a



