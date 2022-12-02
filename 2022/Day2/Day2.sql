drop table if exists #input
create table #input
(
   Opponent nvarchar(max)
  ,Own nvarchar(max)
)


BULK INSERT #input
FROM 'C:\GitHub\aoc\2022\Day2\input.txt'
WITH
(
  FIRSTROW = 1,
  CODEPAGE = '65001', 
  DATAFILETYPE = 'Char',
  FIELDTERMINATOR = ' ',
  ROWTERMINATOR = '\n',
  TABLOCK,
  KEEPNULLS
)

--select * from #input

drop table if exists #Rules
create table #Rules (
  OpponentMove char(1)
 ,OwnMove char(1)
 ,Score int
)

insert into #Rules (
	 OpponentMove
	,OwnMove
	,Score
)
values 
 ('A','X',3)
,('A','Y',6)
,('A','Z',0)
,('B','X',0)
,('B','Y',3)
,('B','Z',6)
,('C','X',6)
,('C','Y',0)
,('C','Z',3)

--Opponent: A for Rock, B for Paper, and C for Scissors
--Own: X for Rock, Y for Paper, and Z for Scissors

drop table if exists #OwnMoveScore
create table #OwnMoveScore (
  OwnMove char(1)
 ,Score int
)
insert into #OwnMoveScore (
	 OwnMove
	,Score
)
values 
 ('X',1)
,('Y',2)
,('Z',3)


--Part 1
select sum(r.Score) + sum(s.Score) from #input i
inner join #Rules r on i.Own = r.OwnMove and i.Opponent = r.OpponentMove
inner join #OwnMoveScore s on s.OwnMove = i.Own

--Part 2
--X means you need to lose, Y means you need to end the round in a draw and Z means you need to win.

select sum(r.Score) + sum(s.Score) from #input i 
inner join #Rules r on i.Opponent = r.OpponentMove and case i.Own when 'X' then 0
																  when 'Y' then 3
																  when 'Z' then 6
																  end
																  = r.Score
inner join #OwnMoveScore s on s.OwnMove = r.OwnMove