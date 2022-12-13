drop table if exists #input_stage
create table #input_stage
(
  dir nchar(1) not null
, dist int not null
)

--extract. Unsafe order import but fine for aoc
BULK INSERT #input_stage
FROM 'C:\GitHub\aoc\2022\Day9\input.txt'
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

drop table if exists #moves
create table #moves
(
  id  int           identity(1, 1) not null
, dir nchar(1) not null
, dist int not null
)

insert into #moves (dir,dist) select dir,dist from #input_stage


drop table if exists #tailTrace
create table #tailTrace (
		 x int not null
		,y int not null
)

drop table if exists #loc
create table #loc (
		 HTs nchar(1) not null primary key
		,x int not null
		,y int not null
)

insert into #loc (HTs,x,y)
values   ('s',0,0)
		,('H',0,0)
		,('T',0,0)

declare  @move int = (select min(id) from #moves) 
		,@step int
		,@dist int
		,@dir nchar(1)
		,@Hx int
		,@Hy int
		,@Tx int
		,@Ty int
while @move is not null
begin
	set @step = 0
	set @dir = (select dir from #moves where id = @move) 
	while @step < (select dist from #moves where id = @move) 
	begin
		--log tail location
		insert into #tailTrace (x,y)
		select x,y from #loc where HTs = 'T'

		--move head
		update #loc set x = case @dir 
								when 'L' then x - 1 
								when 'R' then x + 1
								else x
								end
					   ,y = case @dir 
								when 'U' then y + 1 
								when 'D' then y - 1
								else y
								end
				where HTs = 'H'
		
		--move tail
		select @Hx = h.x
			  ,@Hy = h.y
			  ,@Tx = t.x
			  ,@Ty = t.y
			from (select x, y from #loc where HTs = 'T') t
			cross join (select x, y from #loc where HTs = 'H') h

		if @Hx - @Tx > 1 and abs(@Hy - @Ty) < 1
			set @Tx = @Tx + 1

		if @Hx - @Tx < -1 and abs(@Hy - @Ty) < 1
			set @Tx = @Tx - 1

		if @Hy - @Ty > 1 and abs(@Hx - @Tx) < 1
			set @Ty = @Ty + 1

		if @Hy - @Ty < -1 and abs(@Hx - @Tx) < 1
			set @Ty = @Ty - 1

		if abs(@Hx - @Tx) >= 2 and abs(@Hy - @Ty) >= 1
		begin
			set @Tx = @Tx + (@Hx - @Tx) + iif((@Hx - @Tx) > 1,-1,+1)
			set @Ty = @Ty + (@Hy - @Ty)
		end

		if abs(@Hy - @Ty) >= 2 and abs(@Hx - @Tx) >= 1
		begin
			set @Ty = @Ty + (@Hy - @Ty) + iif((@Hy - @Ty) > 1,-1,+1)
			set @Tx = @Tx + (@Hx - @Tx)
		end

		update #loc set x = @Tx, y = @Ty where HTs = 'T'

		--select * from #loc
		set @step = @step + 1
	end
	set @move = (select min(id) from #moves where id > @move) 
end

--PART 1 answer
select count(*) from (
	select distinct * from #tailTrace) t


