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
--Add digitID--

/* TO DO, spaces need to be preserved as the digits are in columns and 
123
 25 
  5

can be different from

5
45
123

*/

drop table if exists #NumbersWDigit
create table #NumbersWDigit (LineID int not null, ColID int not null, DigitID int not null, Val tinyint not null primary key(DigitID,ColID,LineID))

insert into #NumbersWDigit (LineID,ColID,DigitID,Val)
select 
      LineID	 = s.LineID
    , ColID		 = s.ColID
    , DigitID    = (max(len(s.Val)) over (partition by s.ColID) - g.value) + 1
    , Val		 = substring(cast(s.Val as nvarchar(100)), g.value, 1)
from #Numbers s
cross apply generate_series(1, cast(len(s.Val) as int)) g;

select * from #NumbersWDigit where colid = 1

with res as (
select  n.ColID
	   ,DigitID
	   ,Result = cast(string_agg(n.Val,'') as bigint)
	   ,Operator = o.Val
	   ,n.val
from #NumbersWDigit n
inner join #Operators o on o.ColID = n.ColID
group by n.ColID,DigitID,o.Val
order by o.ColID,DigitID
)
--,res2 as (
/* + */
select  ColID
	   ,Result = sum(Result)
from res
where Operator = '+'
group by ColID
union all
/* * */
select  ColID
	   ,Result = exp(sum(log(Result * 1e0)))
from res
where Operator = '*'
group by ColID
)
select sum(Result) from res
