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
  datafiletype = 'char',
  --fieldterminator = ' ',
  rowterminator = '\n',
  format = 'csv',
  tablock,
  keepnulls,
  keepidentity
)

drop table if exists #Reports

begin try
;with rowno as (
	select txt, id = row_number() over (order by (select 1))
	from #input_stage)
select ReportID = id
	  ,LeveLID  = ordinal
	  ,Val		= cast(replace(value,char(13),'') as int)
into #Reports
from rowno
cross apply string_split(txt,' ',1)
end try
begin catch
	;throw 51000, 'All numbers could not be converted to integrers', 1
end catch

--Answer 1

;with LastVal as (
	select
		 ReportID	= ReportID
		,LevelID	= LevelID
		,Val		= Val
		,LastVal	= lag(Val) over (partition by ReportID order by LevelID) 
	from #Reports)
,a as (select distinct 
	       ReportID
	      ,IncDec	= case when LastVal > Val then 'Inc' 
						   when LastVal < Val then 'Dec'
						   else 'Eq' end
		  ,IsValid  = case when abs(LastVal - Val) between 1 and 3 then 1 else 0 end
	from LastVal
	where LastVal is not null
)
select Day2_Answer1 = count(1/0) 
from (
	select ReportID 
	from a 
	where ReportID not in (select ReportID from a where IsValid = 0)
	group by ReportID 
	having count(*) = 1
	) t

