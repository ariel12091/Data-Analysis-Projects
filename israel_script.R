library(tidyverse)
library(jsonlite)
library("digest")
library(lubridate)
library(httr2)

#scraping


israel_schedule <- map(json_data[[1]]$games, ~as.data.frame(.x)) %>%
  list_rbind()

israel_schdule_clean_links <- israel_schedule %>%
  mutate(game_id_count = str_count(pbp_link, "game_id")) %>%
  mutate(pbp_link = case_when(game_id_count > 1~str_remove(pbp_link,"\\?game_id.*?(?=\\?game_id)"),
                                   TRUE~pbp_link)) %>%
  mutate(pbp_link =  str_remove_all(pbp_link, "&lang=he(?=.*&lang=he)"))




httr2::req_perform_parallel()
  

pbp_israel$result$gameInfo$homeTeam <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62428",simplifyVector = TRUE)

pbp_israel2 <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62426", simplifyVector = TRUE) %>%
  
json_data <- jsonlite::read_json("https://basket.co.il/pbp/json/games_all.json?1753473308320") 



#initial clean

pbp_action_data_clean <- pbp_israel$result$actions %>%
  unnest(parameters, names_sep = "_") %>%
  janitor::clean_names() %>%
  filter(type != "clock") %>% 
  mutate(end_quarter_seconds_remaining = lubridate::period_to_seconds(ms(quarter_time))) %>%
  mutate(end_game_seconds_remaining = case_when(quarter < 5 ~ end_quarter_seconds_remaining + (4-quarter)*600,
                                                quarter >= 5 ~ end_quarter_seconds_remaining)
         

#count points and possesions         
         
df_pts_poss <- pbp_action_data_clean %>%
   mutate(pct_ft = parameters_free_throw_number / parameters_free_throws_awarded) %>%
   mutate(parent_action_id = case_when(parent_action_id == 0~id,
                                       TRUE~parent_action_id)) %>%
   group_by(parent_action_id) %>%
   mutate(end_poss = case_when(type == "shot" & parameters_made == "made"~ TRUE,
                               lead(parameters_type) == "defensive" & lead(type) == "rebound"~TRUE,
                               type == "freeThrow" & parameters_made == "made" & pct_ft == 1~TRUE,
                               type == "turnover"~TRUE)) %>%
   mutate(sum_poss_poss = sum(end_poss, na.rm = TRUE),
          sum_block = sum(parameters_made=="blocked", na.rm = TRUE),
          sum_tech = sum(parameters_type == "technical", na.rm = TRUE)) %>%
   mutate(final_end_poss = case_when(sum_poss_poss >=2 & id == parent_action_id~NA,
                                     sum_block >= 1~lead(end_poss),
                                     sum_tech >= 1 ~ NA,
                                     TRUE~end_poss)) %>%
   ungroup() %>%
   mutate(parameters_points = case_when(parameters_made == "made" & type == "freeThrow" ~ 1, 
                                        TRUE~parameters_points)) %>%
   mutate(team_score = case_when(parameters_made=="made"~parameters_points))


#lineups

players_data <- bind_rows(pbp_israel$result$gameInfo$homeTeam$players %>%
  mutate(team_id = pbp_israel$result$gameInfo$homeTeam$id),
pbp_israel$result$gameInfo$awayTeam$players %>% mutate(team_id = pbp_israel$result$gameInfo$awayTeam$id)) %>% 
  rename(player_id = id)


all_players_in_roster <- players_data %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)

all_players_in_game <- pbp_action_data_clean %>%
  filter(type == "substitution") %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)

substiution_data <- pbp_action_data_clean %>%
  filter(type == "substitution") %>%
  mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
         parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))


df_lineups <- all_players_in_roster %>%
  cross_join(substiution_data) %>%
  filter(team_id.x == team_id.y) %>%
  mutate(is_on = case_when(player_id.x==parameters_player_in~TRUE,
                           player_id.x == parameters_player_out~FALSE,
                           TRUE~NA)) %>%
  arrange(quarter, desc(quarter_time), user_time) %>%
  group_by(player_id.x) %>%
  fill(is_on, .direction = "down") %>%
  ungroup() %>%
  group_by(quarter, quarter_time, end_game_seconds_remaining,
           end_quarter_seconds_remaining, player_id.x,team_id.x) %>%
  summarise(is_on_verdict = last(is_on), .groups = "drop") %>%
  pivot_wider(id_cols = c(quarter, quarter_time, end_game_seconds_remaining,
                          end_quarter_seconds_remaining,
                          team_id.x), names_from = player_id.x,
              values_from = is_on_verdict) %>%
  arrange(quarter, desc(quarter_time)) %>%
  replace(is.na(.), 0) %>%
  mutate(across(.cols = matches("\\d"), ~.x > 0)) %>%
  mutate(row_sum = rowSums(across(.cols = matches("\\d")))) %>%
  rename(team_id = team_id.x) %>%
  group_by(team_id) %>%
  mutate(start_segment = end_game_seconds_remaining, 
         end_segment = coalesce(lead(end_game_seconds_remaining),0)) %>%
  ungroup() %>%
  relocate(c(start_segment, end_segment, end_quarter_seconds_remaining, end_game_seconds_remaining),  .after = team_id)

lineups_lookup <- df_lineups %>%
  rowwise() %>%
  mutate(lineup_id = paste(sort(names(.)[c_across(player_cols)]),collapse = "_"),
         lineup_hash = digest::digest(lineup_id, algo = "xxhash64"))

lineups_hush <- lineups_lookup %>%
  select(-matches("\\d"))  


stints <- lineups_hush %>%
  cross_join(lineups_hush, suffix = c("_offense", "_defense")) %>%
  filter(team_id_offense != team_id_defense) %>%
  mutate(final_start_seg = pmin(start_segment_offense, start_segment_defense),
         final_end_seg = pmax(end_segment_offense, end_segment_defense)) %>%
  relocate(c(final_start_seg, final_end_seg), .after = team_id_offense) %>%
  filter(final_end_seg < final_start_seg) %>%
  group_by(final_start_seg, final_end_seg) %>%
  mutate(segment_id = cur_group_id()) %>%
  relocate(segment_id, .after = final_end_seg) %>%
  ungroup() %>%
  rename(team_id = team_id_offense) %>%
  select(team_id, final_start_seg, final_end_seg, segment_id,
         lineup_hash_offense, lineup_hash_defense, team_id_defense,
         row_sum_defense, row_sum_offense)

#add stints to ppp


by <- join_by(team_id, between(end_game_seconds_remaining, final_end_seg, final_start_seg, bounds = "(]"))

df_pts_poss_lineups <- left_join(df_pts_poss, stints, by)

df_pts_poss_lineups_longer <- df_pts_poss_lineups %>%
  pivot_longer(c(lineup_hash_offense, lineup_hash_defense)) %>%
  mutate(type = str_remove(name, "lineup_hash_")) %>%
  mutate(final_team_id = case_when(type == "offense"~team_id,
                                   type == "defense"~team_id_defense)) %>%
  group_by(final_team_id, type, value) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1))


#on/off calculation
df_pts_poss_lineups_longer %>%
  filter(!is.na(value)) %>%
  left_join(lineups_lookup %>% filter(`31189` == TRUE) %>%
              select(team_id, lineup_hash, lineup_id) %>%
              distinct(), by = c("value" = "lineup_hash",
                                 "final_team_id" = "team_id")) %>%
  mutate(is_on = !is.na(lineup_id)) %>%
  group_by(type, is_on, final_team_id) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE))  %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  view()





map(pbp_israel$result$gameInfo$awayTeam, as.data.frame)  
  
jsonlite::read_json(pbp_israel$result$gameInfo$awayTeam)  
 %>%
  view()









  
stints %>%
  view()
  
  view()
  writexl::write_xlsx("cross_result.xlsx")
  view()
   %>%
    ungroup()

  
df_pts_poss %>%
  mutate(home_team_id = pbp_israel$result$gameInfo$homeTeam$id, 
          away_team_id = pbp_israel$result$gameInfo$awayTeam$id) %>%
  mutate(team_offense = team_id,
         team_defense = case_when(team_id == home_team_id~away_team_id,
                                  team_id == away_team_id~home_team_id,
                                  TRUE~NA)) %>%
  left_join(df_lineups, by = c("team_id", "quarter", "quarter_time")) %>%
  group_by(team_id) %>%
  filter(team_id != 0) %>%
  fill(c(matches("\\d"), stint), .direction = "down") %>%
  ungroup() %>%
  fill(joined_stint, .direction = "down") %>%
  filter(quarter == 3) %>%
  relocate(c(stint, joined_stint), .after = team_score) %>%
  group_by(team_id, joined_stint) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  view()
  
  filter(stint %in% c("stint16", "stint17", "stint18"), team_id == 24) %>%

  view()
  
  group_by(team_id, stint) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  view()
  
  
  pivot_longer(c(matches("\\d"))) %>%
  group_by(team_id, stint, name, value) %>%
  summarise()
  view()



df_pts_poss %>%
  filter(quarter == 1) %>%
  left_join(df_lineups, by = c("team_id", "quarter", "quarter_time")) %>%
  view()


  mutate(lubridate::period_to_seconds(quarter_time))
  mutate(end_of_game_seconds_remaining = 
           end_of_quarter_seconds_remaining)

  df_lineups %>%
    view()
  

cbind(pbp_israel$result$gameInfo$homeTeam$players, pbp_israel$result$gameInfo$homeTeam$id

  


  
  
  
  relocate(player_id, .after = parameters_player_out) %>%
  pivot_wider(id_cols = c(quarter_time), names_from = "player_id",names_prefix = "player_", values_from = player_id) %>% 
  view()
 