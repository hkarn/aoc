if object_id('tempdb..#input_stage', 'U') is not null  
	drop table #input_stage
if object_id('tempdb..#input', 'U') is not null  
	drop table #input
create table #input_stage
(
    txt nvarchar(max)
)

--extract. Unsafe order import but fine for aoc
BULK INSERT #input_stage
FROM 'C:\GitHub\aoc\2022\Day7\input.txt'
WITH
(
  FIRSTROW = 1,
  CODEPAGE = '65001', 
  DATAFILETYPE = 'Char',
  FIELDTERMINATOR = ';',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

create table #input
(
    id int identity(1,1) not null
   ,txt nvarchar(max) not null
)

insert into #input (txt)
select (txt) from #input_stage

drop function if exists [dbo].[AOCRemoveNonNumeric]

go
create function [dbo].[AOCRemoveNonNumeric](@temp nvarchar(max))
returns int
as
begin

    declare @keep as nvarchar(max)
    set @keep = '%[^0-9]%'
    while patindex(@keep, @temp) > 0
        set @temp = Stuff(@Temp, patindex(@keep, @temp), 1, '')

    return try_cast(@temp as int)
end
go

if object_id('tempdb..#withLevels', 'U') is not null  
	drop table #withLevels
select id,txt, t.cmdtype
		  ,reqdir = trim(r.reqdir)
		  ,moveLevel = m.moveLevel
		  ,dirlevel = sum(m.moveLevel) over (order by id rows unbounded preceding)
		  ,curDir = cast(null as nvarchar(max))
		  ,curPath = cast(null as nvarchar(max))
		  ,fileSize = iif(try_cast(left(txt,1) as int) is not null, [dbo].[AOCRemoveNonNumeric](txt), null)
into #withLevels
from #input
cross apply  (select cmdtype = case when left(txt,4) = N'$ cd' then 'cd'
						 when left(txt,4) = N'$ ls' then 'ls'
						 when left(txt,3) = N'dir' then 'dir'
						 when try_cast(left(txt,1) as int) is not null then 'file'
				    end) t(cmdtype)
cross apply  (select moveLevel = case when txt = N'$ cd ..' then -1
						 when left(txt,4) = N'$ cd' and right(txt,2) <> '..' then 1
					end) m(moveLevel)
cross apply (select reqdir = iif(t.cmdtype = 'cd',right(txt,len(txt) -4),null)) r(reqdir)
order by id

alter table #withLevels add primary key (id)

--fill fulldir and location
declare @pointer int = (select min(id) from #withLevels where cmdtype = 'cd')
declare @curPath nvarchar(max) = ''
declare @curDir nvarchar(max) = ''
declare @reqdir nvarchar(max)
declare @dirlevel int
declare @id int

declare cur cursor for 
	select id, reqdir, dirlevel from #withLevels where cmdtype = 'cd'

	open cur 
	fetch next from cur into @id, @reqdir, @dirlevel
	while @@fetch_status = 0
	begin
		if @reqdir = '..'
			set @curPath = (select left(@curPath,(len(@curPath) - charindex('\',reverse(@curPath)))))
		else
			set @curPath = @curPath + '\' + @reqdir

		set @curDir = (select right(@curPath,(charindex('\',reverse(@curPath))) -1 ))

		update #withLevels set curPath = @curPath, curDir = @curDir where id = @id

		fetch next from cur into @id, @reqdir, @dirlevel
	end
	close cur 
	deallocate cur

update l  set l.curPath = t.curPath
				,l.curDir = t.curDir
from #withLevels l
inner join (
select id
		,curPath = last_value(curPath) ignore nulls over (order by id)
		,curDir = last_value(curDir) ignore nulls over (order by id)
from #withLevels) t on t.id = l.id


if object_id('tempdb..#system', 'U') is not null  
	drop table #system 

create table #system (
	 dirID int identity(1,1) primary key
	,dirName nvarchar(max)
	,dirFullPath nvarchar(max)
	,parentDirID int
	,parentDirName nvarchar(max)
	,treeLevel int
	,bytesInDir int
)

insert into #system (dirName,dirFullPath,treeLevel)
select distinct curDir,curPath,dirlevel from #withLevels

update b set b.ParentDirID = a.dirID, b.parentDirName = a.dirName
from #system a
inner join #system b on b.treeLevel - 1 = a.treeLevel and left(b.dirFullPath,len(b.dirFullPath) - len(b.dirName) - 1) = a.dirFullPath


--PART1
--answer
select sum(fileSize) from (
select a.dirId,fileSize = sum(b.fileSize) from #system a 
inner join #withLevels b on charindex(a.dirFullPath,b.curPath,1) = 1
group by a.dirId
having sum(b.fileSize) <= 100000) t

--PART2
update s set s.bytesInDir = t.fileSize
from #system s
inner join 
(select a.dirId,fileSize = sum(b.fileSize) from #system a 
inner join #withLevels b on charindex(a.dirFullPath,b.curPath,1) = 1
group by a.dirId) t on t.dirId = s.dirID

declare @diskSize int = 70000000
declare @minFree int = 30000000
declare @diskFree int = @diskSize - (select bytesInDir from #system where dirID = 1)

declare @minToDelete int = -(@diskFree - @minFree)

--answer
select bytesInDir from #system where dirID in (
select top 1 dirId from #system 
where bytesInDir - @minToDelete >= 0
order by abs(bytesInDir - @minToDelete))

drop function if exists [dbo].[AOCRemoveNonNumeric]
