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

--Part 1 Answer:

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

drop table if exists #Cards
create table #Cards (
		 ID int not null identity(1,1) primary key
		,CardID int not null
		,Processed bit not null default(0)
	   )
	   
drop table if exists #Numbers
create table #Numbers (
		CardID int not null
	   ,SetType nvarchar(20)  not null
	   ,Nr int not null
	   primary key(CardID,SetType,Nr))

set identity_insert #Cards on

insert into #Cards (ID, CardID, Processed)
select distinct ID = CardID,CardID, Processed = 0 from #Card

set identity_insert #Cards off

insert into #Numbers (
		CardID
	   ,SetType
	   ,Nr)
select CardID 
	,SetType
	,Nr 
from #Card


drop table if exists #WinMatrix
create table #WinMatrix (
	 ID int not null primary key
	,CardID int not null
	,Wins int not null
	,WinStartCardID int not null
	,WinEndCardID int not null
)

--loop lazy
while exists(select 1 from #Cards where Processed = 0) 
begin
	truncate table #WinMatrix

	insert into #WinMatrix (
		 ID
		,CardID
		,Wins
		,WinStartCardID
		,WinEndCardID)
	select ID				= c.ID
		 , CardID			= c.CardID
		 , Wins				= count(n1.Nr) 
		 , WinStartCardID	= c.CardID + 1
		 , WinEndCardID		= c.CardID + count(n1.Nr) 	 
	from #Cards c
	inner join #Numbers n1 on c.CardID = n1.CardID
	inner join #Numbers n2 on n1.CardID = n2.CardID
							   and n1.SetType <> n2.SetType
							   and n1.Nr = n2.Nr
	where c.Processed = 0
	  and n1.SetType = 'Winners'
	group by c.ID,c.CardID

	update #Cards set Processed = 1


	insert #Cards (CardID, Processed)
	select CardID = c.CardID
		  ,Processed = 0
	from #WinMatrix w
	inner join #Cards c on w.WinStartCardID <= c.ID and w.WinEndCardID >= c.ID
	


end

--Answer Part 2

select count(*) from #Cards