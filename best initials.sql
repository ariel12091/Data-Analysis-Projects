select initals, total_players, total_above_20, cast(cast(total_above_20 as decimal (7,2))  / total_players as decimal (7,2)) as rate
from (
select initals, count (distinct(player)) as total_players 
from (
select CONCAT(SUBSTRING(player,1,1), SUBSTRING(player, CHARINDEX(' ', player)+1,1)) as initals, player, pts_per_g
from [pbp].[dbo].[df_per_game_1]) a
group by initals) a 
inner join 
(select initials, count(distinct(player)) as total_above_20
from (select CONCAT(SUBSTRING(player,1,1), SUBSTRING(player, CHARINDEX(' ', player)+1,1)) as initials, player, pts_per_g
from [pbp].[dbo].[df_per_game_1]
where pts_per_g > 18 or ast_per_g > 9 or trb_per_g > 12) b
group by initials) c
on a.initals = c.initials
where total_players > 10
order by total_players desc
