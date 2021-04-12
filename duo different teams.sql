select duo, count(distinct substring(roster,1,3)) as dis_teams
from (
select b.player as player_a, a.player as player_b, concat(b.player,' ', a.player) as duo, a.roster
from (
select player, concat(case when team_id = 'NOK' then 'NOH' when team_id = 'NJN' then 'BRK' when team_id = 'NOP' then 'NOH' when team_id = 'WSB' then 'WAS' else team_id end, year) as roster
from [pbp].[dbo].[df_per_game_1] a 
where year > 1990
and team_id != 'TOT') a 
inner join 
(select player, concat(case when team_id = 'NOK' then 'NOH' when team_id = 'NJN' then 'BRK' when team_id = 'NOP' then 'NOH'  when team_id = 'WSB' then 'WAS' else team_id end, year) as roster
from [pbp].[dbo].[df_per_game_1]
where year > 1990
and team_id != 'TOT') b
on a.roster = b.roster and a.player != b.player) c 
group by duo
order by dis_teams desc
