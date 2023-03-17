pbp <- pbp1 %>%
  filter(season == 2023)

library(future)
library(tidyverse)
library(hoopR)

plan("multisession")
pbp <- load_nba_pbp(c(2008:2023))


df_leads_all %>%
  distinct(game_id)


df_leads_all %>%
  head() %>%
  view()
 
df_leads_all <- pbp %>%
  select(qtr, game_id, team_id, home_team_id, home_team_name,
         away_team_id, away_team_name, 
         home_score, away_score,start_game_seconds_remaining,
         end_game_seconds_remaining,clock_minutes,clock_seconds, season, season_type) %>%
  mutate(lead = home_score-away_score) %>%
  mutate(leading_team = case_when(lead < 0~away_team_name, lead > 0~home_team_name, lead ==0~"even")) %>%
  filter(abs(lead) >= 15) %>%
  filter(qtr < 3) %>%
  colnames()


pbp %>%
  select(home_favorite,sequence_number)

df_one_poss_all <- pbp %>%
  select(game_id, team_id, type_id, home_team_id, home_team_name, sequence_number,
         away_team_id, away_team_name, home_favorite, 
         home_score, away_score,start_game_seconds_remaining,
         end_game_seconds_remaining,clock_minutes,clock_seconds, season, season_type) %>%
  mutate(lead = home_score-away_score) %>%
  mutate(leading_team = case_when(lead < 0~as.character(away_team_id), 
                                  lead > 0~as.character(home_team_id), 
                                  lead ==0~"even")) %>%
  mutate(winning_team = case_when(type_id == 402~leading_team)) %>%
  fill(winning_team, .direction = "up") %>%
  mutate(favorite_team = case_when(home_favorite == TRUE~home_team_id,
                                   TRUE~away_team_id)) %>%
  filter(abs(lead) <=3) 

pbp %>%
  mutate(sequence_number = as.numeric(sequence_number)) %>%
  filter(game_id == 401468023) %>%
  arrange(desc(sequence_number)) %>%
  view()

df_comebacks <- 

df_one_poss_all %>%
  colnames()
    
    
game_ids_comeback_all %>%
  slice_head(n = 250) %>%
  view()

all_game_stat <- game_ids_comeback_all %>%
  distinct(season, game_id, is_comeback) %>%
  filter(season == 2023) %>%
  view()
  group_by(season, is_comeback) %>%
  summarise(n = n_distinct(game_id), n_sum = sum(is_comeback)) %>%
  ungroup() %>%
  group_by(season) %>%
  mutate(freq = n / lead(n, order_by = n)) %>%
  view()


  
game_ids_comeback_all <- left_join(df_leads_all, df_one_poss_all, by = c("game_id","season", "season_type")) %>%
  mutate(is_comeback = start_game_seconds_remaining.x > start_game_seconds_remaining.y) %>%
  
  
  
  mutate(time_comeback = start_game_seconds_remaining.x - start_game_seconds_remaining.y) %>%
  group_by(game_id) %>%
  mutate(min_time = min(time_comeback)) %>%
  distinct(game_id, .keep_all = TRUE) %>%
  filter(is.na(min_time))
  filter(time_comeback == min_time) %>%
  distinct(game_id, .keep_all = TRUE) %>%
  view()  
  distinct(game_id, season, season_type, leading_team.x, leading_team.y, 
           lead.x,)
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
  
  