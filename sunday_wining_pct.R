
df_visit <- df_games %>% mutate (has_won = visit_won) %>% rename(team = visitor_team_name, pts = visitor_pts, opp_pts = home_pts, opp_team = home_team_name)

df_home <- df_games  %>% mutate (has_won = !visit_won) %>% rename(team = home_team_name, pts = home_pts, opp_pts = visitor_pts, opp_team = visitor_team_name)

df_untied <- rbind(df_visit, df_home) %>% select(-visit_won) %>% mutate (adv = abs(pts - opp_pts)) 

library(lubridate)

bos_days <- df_untied %>% drop_na(pts) %>% mutate(date = as_date(substr(game_id,1,8)),game_start_time = hm(game_start_time)) %>%
  mutate (week_day = weekdays(date), time_of_day = case_when(game_start_time <= hm("4:00p") ~"Noon",TRUE~"Else")) %>% 
  mutate(year = substr(game_id, 1,4), month = substr(game_id,5,6), day = substr(game_id,7,8)) %>% 
  mutate(league_year = case_when(month %in% c("10","11","12")~paste(year,as.numeric(year)+1,sep = "-"),
                                 month %in% c("01","02","03","04","05")~paste(as.numeric(year)-1,year,sep = "-"))) %>% 
  group_by(league_year, team, week_day, time_of_day) %>% summarise(wins = sum(has_won,na.rm = TRUE),games = n()) %>% mutate (pct_success = wins / games) %>%
  filter (time_of_day == "Noon") %>% filter(games >= 5, league_year == "2020-2021") %>%
  arrange(pct_success, desc(games))


##table

library(kableExtra)
    
bos_days %>% rename("Season" = "league_year") %>% kable("html", col.names = c("Season", "Team", "Day of Week", "Time of Day", "Wins", "Games", "Win Percent")) %>% 
kable_styling("striped") %>% row_spec(row = 12, bold = TRUE, color = "green")
