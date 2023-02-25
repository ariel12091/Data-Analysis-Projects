df_leads_all <- pbp1 %>%
  select(game_id, team_id, home_team_id, home_team_name,
         away_team_id, away_team_name, 
         home_score, away_score,start_game_seconds_remaining,
         end_game_seconds_remaining,clock_minutes,clock_seconds, season, season_type) %>%
  mutate(lead = home_score-away_score) %>%
  mutate(leading_team = case_when(lead < 0~away_team_name, lead > 0~home_team_name, lead ==0~"even")) %>%
  filter(abs(lead) >= 15) %>%
  colnames()


df_one_poss_all <- pbp1 %>%
  select(game_id, team_id, home_team_id, home_team_name,
         away_team_id, away_team_name, 
         home_score, away_score,start_game_seconds_remaining,
         end_game_seconds_remaining,clock_minutes,clock_seconds, season, season_type) %>%
  mutate(lead = home_score-away_score) %>%
  mutate(leading_team = case_when(lead < 0~away_team_name, lead > 0~home_team_name, lead ==0~"even")) %>%
  filter(abs(lead) <=3) 

df_comebacks <- 

df_one_poss_all %>%
  colnames()
    
    
game_ids_comeback_all <- inner_join(df_leads_all, df_one_poss_all, by = c("game_id","season", "season_type")) %>%
  filter(start_game_seconds_remaining.x > start_game_seconds_remaining.y) %>%
  mutate(time_comeback = start_game_seconds_remaining.x - start_game_seconds_remaining.y) %>%
  group_by(game_id) %>%
  mutate(min_time = min(time_comeback)) %>%
  distinct(game_id, season, season_type)
  filter(game_id == "401468019") %>%
  view()


df_comback_compare <- game_ids_comeback_all %>%
  filter(season_type == 2) %>%
  group_by(season) %>%
  summarise(n_comeback = n_distinct(game_id)) %>%
  inner_join(df_leads_all %>%
               filter(season_type==2) %>%
               group_by(season) %>%
               summarise(n_total_leads = n_distinct(game_id)),by = c("season")) %>%
  inner_join(pbp1 %>%
               filter(season_type==2) %>%
               group_by(season) %>%
               summarise(n_total = n_distinct(game_id)),by = c("season")) %>%
  
  df_comback_compare
  view()

  
df_comback_compare %>%
  mutate(comeback_pct = n_comeback / n_total_leads, 
         lead_pct = n_total_leads / n_total) %>%
  arrange(desc(comeback_pct)) %>%
  view()
  
game_ids_comeback_all %>%
  filter(game_id == "401468019") %>%
  select(season_type) %>%
  distinct()
  
  