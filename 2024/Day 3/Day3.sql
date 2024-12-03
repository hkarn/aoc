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
  rowterminator = '\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\nABCDEFGHIJKLMNOPQRST0312456789#\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n',
  --some nonense to not break rows, linebreak will still give a garbage char in the feed
  tablock,
  keepnulls,
  keepidentity
)

if (select count(*) from #input_stage) > 1
	throw 51000, 'More then 1 row imported, check rowterminator', 1

drop table if exists #firstClean
drop table if exists #rawValues

--Part 1

select YetToProcess = replace([value],'mul(','')
	  ,ValidStart = case when substring([value], 1 ,5) like 'mul([0-9]' then 1 else 0 end
	  ,ValidComa = case when charindex(',',[value],4) between 6 and 8 then 1 else 0 end
	  ,FirstValue = substring([value], 5 ,greatest(charindex(',',[value],4) - 5,0))
	  ,ComaPosition = charindex(',',[value],4) - 3	  
into #firstClean
from #input_stage
cross apply string_split(replace(txt,'mul(','|mul('),'|') --'|' is not part of usefull input

select FirstValue  = FirstValue
	  ,SecondValue = substring(YetToProcess,ComaPosition, charindex(')',YetToProcess,1) - ComaPosition)
into #rawValues
from #firstClean
where ValidStart = 1
  and ValidComa = 1
  --contains valid second part
  and (substring(YetToProcess, ComaPosition , 4) like '[0-9])%'
        or substring(YetToProcess, ComaPosition , 4) like '[0-9][0-9])%'
		or substring(YetToProcess, ComaPosition , 4) like '[0-9][0-9][0-9])%')

select Day3_Answer1 = sum(try_cast(FirstValue as int)*try_cast(SecondValue as int)) 
from #rawValues
where try_cast(FirstValue as int) is not null
  and try_cast(SecondValue as int) is not null


--Part 2

drop table if exists #dontRemoved
--split on do() drop the line after possible don't()
select txt = substring([value],1,case when charindex('don''t()',[value],3) = 0 then len([value]) else  charindex('don''t()',[value],3) - 1 end)
into #dontRemoved
from #input_stage
cross apply string_split(replace(txt,'do()','|do()'),'|') --'|' is not part of usefull input


--from here use Part 1 solution, it works on multiple rows as well
drop table if exists #firstClean2
drop table if exists #rawValues2

select YetToProcess = replace([value],'mul(','')
	  ,ValidStart = case when substring([value], 1 ,5) like 'mul([0-9]' then 1 else 0 end
	  ,ValidComa = case when charindex(',',[value],4) between 6 and 8 then 1 else 0 end
	  ,FirstValue = substring([value], 5 ,greatest(charindex(',',[value],4) - 5,0))
	  ,ComaPosition = charindex(',',[value],4) - 3	  
into #firstClean2
from #dontRemoved
cross apply string_split(replace(txt,'mul(','|mul('),'|') --'|' is not part of usefull input

select FirstValue  = FirstValue
	  ,SecondValue = substring(YetToProcess,ComaPosition, charindex(')',YetToProcess,1) - ComaPosition)
into #rawValues2
from #firstClean2
where ValidStart = 1
  and ValidComa = 1
  --contains valid second part
  and (substring(YetToProcess, ComaPosition , 4) like '[0-9])%'
        or substring(YetToProcess, ComaPosition , 4) like '[0-9][0-9])%'
		or substring(YetToProcess, ComaPosition , 4) like '[0-9][0-9][0-9])%')

select Day3_Answer2 = sum(try_cast(FirstValue as int)*try_cast(SecondValue as int)) 
from #rawValues2
where try_cast(FirstValue as int) is not null
  and try_cast(SecondValue as int) is not null