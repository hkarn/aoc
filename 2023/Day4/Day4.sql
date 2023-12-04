set nocount on;

drop table if exists #input_stage
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
  --FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

--Day 4
--ETL
drop table if exists #Card

;with orderd as (
	select   CardID	= substring(txt,6, charindex(N':',txt) - 6)
			,txt	= trim(right(txt,len(txt) - charindex(N':',txt))) 
	from #input_stage)
,wo as (
select CardID	= CardID
	  ,SetType	= case when ordinal = 1 then N'Winners' when ordinal = 2 then N'Own' end
	  ,NrSet	= trim(replace(replace(value,char(10),N''),char(13),N''))
from orderd
cross apply string_split(txt,N'|', 1))

select CardID,SetType,Nr=cast(trim(value) as int), NrOrder = ordinal 
into #Card
from wo
cross apply string_split(replace(NrSet,N'  ',N' '),N' ', 1)

select sum(Score) from (
select distinct CardID
,Score = power(2,count(Nr) over (partition by CardID) - 1)
from (
		select distinct 
				CardID = c1.CardID
			  , Nr = c1.Nr from #Card c1
		inner join #Card c2 on c1.CardID = c2.CardID
						   and c1.SetType <> c2.SetType
						   and c1.Nr = c2.Nr
		) t
) t