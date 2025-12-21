set xact_abort, nocount on;

drop table if exists #input_stage
create table #input_stage
(
	 RowID int not null identity(1,1) primary key
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

/* Day 7 */
drop table if exists #DataGrid

select RowID		= isnull(RowID,-1) /*force not null column*/
	  ,ColID		= isnull(g.value,-1) /*force not null column*/
	  ,Val			= cast(substring(txt, g.value, 1) as nchar(1))
into #DataGrid
from #input_stage
cross apply generate_series(1, cast(len(txt) as int)) g

alter table #DataGrid add primary key (RowID,ColID)

declare @i int = 2
declare @SplitterHitCount int = 0
while @i <= (select max(RowID) from #DataGrid)
begin
	/* stright downs if any */
	update currow set currow.Val = prevrow.Val  
	from #DataGrid currow
	inner join #DataGrid prevrow on currow.ColID = prevrow.ColID and currow.RowID = prevrow.RowID + 1
	where currow.RowID = @i
	  and prevrow.Val = 'S'
	  and currow.Val = '.';

	/* splits if any */
	set @SplitterHitCount = @SplitterHitCount + 
		(select count(*)
		from #DataGrid currow
			inner join #DataGrid prevrow on currow.ColID = prevrow.ColID and currow.RowID = prevrow.RowID + 1
			where currow.RowID = @i
				and prevrow.Val = 'S'
				and currow.Val = '^');

	with splitHit as (
	select currow.RowID, currow.ColID
	from #DataGrid currow
		inner join #DataGrid prevrow on currow.ColID = prevrow.ColID and currow.RowID = prevrow.RowID + 1
		where currow.RowID = @i
			and prevrow.Val = 'S'
			and currow.Val = '^'
			)
	update curow set curow.Val = 'S'
	from 
	#DataGrid curow 
	inner join splitHit on curow.RowID = splitHit.RowID 
					   and (curow.ColID = splitHit.ColID + 1 
						or curow.ColID = splitHit.ColID - 1)
	
	

	set @i = @i + 1
end

select @SplitterHitCount
