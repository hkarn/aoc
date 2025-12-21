set xact_abort, nocount on;

drop table if exists #input_stage
create table #input_stage
(
	 LineID int not null identity(1,1) primary key
	,txt1 nvarchar(max) null
	,txt2 nvarchar(max) null
)
insert into #input_stage(txt1,txt2)
select txt1,txt2 from 
openrowset(
	 bulk  '/input_data/input.txt'
	,fieldterminator = '-'
	,rowterminator = '\r\n'
	,format = 'csv'
	,firstrow = 1
) with (txt1 nvarchar(max), txt2 nvarchar(max)
) input

/* select * from #input_stage */

/* --- Day 5: Cafeteria --- */

drop table if exists #Ranges
create table #Ranges (RangeID int not null identity(1,1)  
					, FromIngredientID bigint not null
					, ToIngredientID bigint not null
					primary key (FromIngredientID, ToIngredientID,RangeID))
drop table if exists #Ingredients
create table #Ingredients (IngredientID bigint not null primary key)

insert into #Ranges(FromIngredientID,ToIngredientID)
select distinct
	 FromIngredientID = cast(trim(txt1) as bigint)
	,ToIngredientID = cast(trim(txt2) as bigint)
from #input_stage 
where LineID < (select LineID from #input_stage where txt1 is null)

insert into #Ingredients
select distinct 
	IngredientID = cast(trim(txt1) as bigint)
from #input_stage 
where LineID > (select LineID from #input_stage where txt1 is null)

/* --- Part 1 --- */
select count(*)
from #Ingredients i
where exists(select 1/0 from #Ranges r
						where r.FromIngredientID <= i.IngredientID
						  and r.ToIngredientID >= i.IngredientID)


/* --- Part 2 --- */
declare @WordDone bit = 1
while @WordDone = 1
begin
set @WordDone = 0
/* extend ranges ending inside another range */
update r1 set  r1.ToIngredientID = r2.ToIngredientID
/* select * --*/
from #Ranges r1
inner join #Ranges r2
						on r1.ToIngredientID >= r2.FromIngredientID
						  and r1.ToIngredientID <= r2.ToIngredientID
						  and r1.RangeID <> r2.RangeID
where r1.ToIngredientID <> r2.ToIngredientID

if rowcount_big() > 0 set @WordDone = 1

/* extend start of ranges starting inside another range */
update r1 set  r1.FromIngredientID = r2.FromIngredientID
/* select * --*/
from #Ranges r1
inner join #Ranges r2
						on r1.FromIngredientID <= r2.ToIngredientID
						  and r1.FromIngredientID >= r2.FromIngredientID
						  and r1.RangeID <> r2.RangeID
where r1.FromIngredientID <> r2.FromIngredientID

if rowcount_big() > 0 set @WordDone = 1
end

/* distinct ranges */
drop table if exists #RangesDist
select distinct
	FromIngredientID
	,ToIngredientID
into #RangesDist
from #Ranges


select sum(ToIngredientID-FromIngredientID+1) from 
#RangesDist r
