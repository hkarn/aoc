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
  rowterminator = '\n',
  format = 'csv',
  tablock,
  keepnulls,
  keepidentity
)

drop table if exists #inputClean

;with sep as (
	select txt = trim(replace(txt,'   ',';'))
	from #input_stage)
,sep2 as (
	select txt
		 , split = charindex(';', txt, 0)
	from sep)

select l = try_cast(trim(substring(txt,0,split)) as int)
	 , r = try_cast(trim(replace(substring(txt,split+1,len(txt)),char(13),'')) as int)
into #inputClean
from sep2

--Answer 1
;with ord as (
	select l	 = l
		 , r	 = r
		 , lSort = row_number() over (order by l)
		 , rSort = row_number() over (order by r)
	from #inputClean)
select Day1_Answer1 = sum(abs(o1.l - o2.r))
from ord o1
inner join ord o2 on o1.lSort = o2.rSort


--Answer 2
;with c as (
	select l	  = l
		 , r	  = r
		 , rCount = count(*) over (partition by r)
	from #inputClean)
select Day1_Answer2 = sum(c1.l*c2.rCount)
from c c1
inner join (select distinct r, rCount from c) c2 on c1.l = c2.r