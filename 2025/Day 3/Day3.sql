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
