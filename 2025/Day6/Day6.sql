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

