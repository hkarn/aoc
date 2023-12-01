drop table if exists #input_stage
drop table if exists #input
create table #input_stage
(
    txt nvarchar(max)
)
--unsafe order import
BULK INSERT #input_stage
FROM '/input_data/input.txt'
WITH
(
  FIRSTROW = 1,
  DATAFILETYPE = 'Char',
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

--AOC 2023. Day 1, part 1
select sum(n) from (
	select try_cast(concat(
				substring(txt,patindex('%[0-9]%', txt),1)
			   ,substring(revtext,patindex('%[0-9]%', revtext),1)
			   ) as int)
	from #input_stage
	cross apply (select revtext = reverse(txt)) r(revtext)
) t(n)

--AOC 2023. Day 1, part 2
;with replaced as ( 
select txt = 
	replace(
		replace(
			replace(
				replace(
					replace(
						replace(
							replace(
								replace(
									replace(txt,'one','one1one')
								,'two','two2two')
							,'three','three3three')
						,'four','four4four')
					,'five','five5five')
				,'six','six6six')
			,'seven','seven7seven')
		,'eight','eight8eight')
	,'nine','nine9nine')
from #input_stage
)

select sum(n) from (
	select try_cast(concat(
				substring(txt,patindex('%[0-9]%', txt),1)
			   ,substring(revtext,patindex('%[0-9]%', revtext),1)
			   ) as int)
	from replaced
	cross apply (select revtext = reverse(txt)) r(revtext)
) t(n)