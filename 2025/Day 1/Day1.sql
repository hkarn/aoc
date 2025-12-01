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

select Dir = cast(left(txt,1) as nchar(1))
	  ,Dist = cast(trim(char(13) FROM trim(char(10) FROM trim(substring(txt,2,len(txt))))) as int)
	  ,Ord = row_number() over (order by (select 1))
into #inputClean
from #input_stage;


--Answer 1
with positions as (
	select Ord = cast(0 as int)
		  ,Dist = 50
		  ,Zeros = 0
union all
select Ord = cast(s.ord as int)
	  ,Dist = case when s.Dir = 'L' then (p.dist - s.dist) % 100
				   when s.Dir = 'R' then (p.dist + s.dist) % 100
				end
	  ,Zeros = p.Zeros + CASE 
						when ((case when s.Dir = 'L' then (p.dist - s.dist) % 100
				   when s.Dir = 'R' then (p.dist + s.dist) % 100
				end) + 100) % 100 = 0 then 1 else 0 end
from positions p
join #inputClean s on s.ord = p.ord + 1

)
select top 1 [Password] = Zeros from positions order by ord desc
option (maxrecursion 0)
