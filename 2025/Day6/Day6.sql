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
) input;

/* select * from #input_stage */
drop table if exists #splitData
select LineID
	  ,ColID		= row_number() over (partition by LineID order by (select null)) 
	  ,Val			= ltrim(rtrim(value))
	  ,MaxLineID	= max(LineID) over ()
into #splitData
from #input_stage
cross apply string_split(replace(txt, char(9), ' '), ' ')
where ltrim(rtrim(value)) <> ''

drop table if exists #Numbers
create table #Numbers (LineID int not null, ColID int not null, Val bigint not null primary key(ColID,LineID))
drop table if exists #Operators
create table #Operators (LineID int not null, ColID int not null, Val char(1) not null primary key(ColID,LineID))

insert into #Numbers (LineID,ColID,Val)
select LineID
	  ,ColID
	  ,Val
from #splitData
where LineID < MaxLineID

insert into #Operators (LineID,ColID,Val)
select LineID
	  ,ColID
	  ,Val
from #splitData
where LineID = MaxLineID;

-- Part 1 --
with res as (
/* + */
select  ColID
	   ,Result = sum(Val)
from #Numbers n
where exists(select 1/0 from #Operators o where o.ColID = n.ColID and Val = '+')
group by ColID
union all
/* * */
select  ColID
	   ,Result = exp(sum(log(Val * 1e0)))
from #Numbers n
where exists(select 1/0 from #Operators o where o.ColID = n.ColID and Val = '*')
group by ColID
)
select sum(Result) from res

-- Part2 --
-- Columns per digit --
drop table if exists #DataSplit2
declare @MaxLen int = (select max(len(txt)) from #input_stage); /*trailing spaces lost on some rows, keep fixed lenght*/
with cols as (
select LineID
	  ,ColID		= g.value
	  ,Val			= substring(cast(txt as nvarchar(max)), g.value, 1)
	  ,MaxLineID	= max(LineID) over ()
from #input_stage
cross apply generate_series(1, @MaxLen) g)

select LineID
	  ,ColID		
	  ,Val			= case when LineID = MaxLineID then
							last_value(nullif(Val,'')) ignore nulls over(partition by LineID order by ColID) 
						else
							Val 
						end
	  ,MaxLineID	
into #DataSplit2
from cols;



with res0 as (
select  n.ColID
	   ,Result = cast(replace(rtrim(ltrim(string_agg(n.Val,'') within group (order by LineID) )), ' ','') as bigint)
	   ,Operator = o.Val
from #DataSplit2 n
inner join (select ColID, Val from #DataSplit2 where LineID = MaxLineID) o on o.ColID = n.ColID
where n.LineID <> n.MaxLineID
group by n.ColID,o.Val
)
,res1 as (
select  ColID
	   ,Result 
	   ,Operator
	   ,ProblemGroupID = sum(case when isnull(Result,0)  = 0 then 1 else 0 end) over (order by ColID)
from res0 n
)
,res2 as (
/* + */
select  ProblemGroupID
	   ,Result = sum(Result)
from res1
where Operator = '+'
  and Result <> 0
group by ProblemGroupID
union all
/* * */
select  ProblemGroupID
	   ,Result = exp(sum(log(Result * 1e0)))
from res1
where Operator = '*'
  and Result <> 0
group by ProblemGroupID
)
select sum(Result) from res2
