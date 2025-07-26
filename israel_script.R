pbp_israel <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62428",simplifyVector = TRUE)

pbp_israel2 <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62426", simplifyVector = TRUE) %>%
  
json_data <- jsonlite::read_json("https://basket.co.il/pbp/json/games_all.json?1753473308320") 

israel_schedule <- map(json_data[[1]]$games, ~as.data.frame(.x)) %>%
  list_rbind()



library(jsonlite)


map(pbp_israel$result$gameInfo$awayTeam, as.data.frame)  
  
jsonlite::read_json(pbp_israel$result$gameInfo$awayTeam)  

pbp_israel$result$actions %>%
  unnest(parameters, names_repair = "universal") %>%
  
  janitor::clean_names() %>%
  filter(type_26 != "clock") %>%
  mutate(pct_ft = free_throw_number / free_throws_awarded) %>%
  mutate(parent_action_id = case_when(parent_action_id == 0~id,
                                      TRUE~parent_action_id)) %>%
  group_by(parent_action_id) %>%
  mutate(end_poss = case_when(type_26 == "shot" & made == "made"~ TRUE,
                              lead(type_4) == "defensive" & lead(type_26) == "rebound"~TRUE,
                              type_26 == "freeThrow" & made == "made" & pct_ft == 1~TRUE,
                              type_26 == "turnover"~TRUE)) %>%
  mutate(sum_poss_poss = sum(end_poss, na.rm = TRUE),
         sum_block = sum(made=="blocked", na.rm = TRUE)) %>%
  mutate(final_end_poss = case_when(sum_poss_poss >=2 & id == parent_action_id~NA_real_,
                                    sum_block == 1~lead(end_poss),
         TRUE~end_poss)) %>%
  ungroup() %>%
  mutate(points = case_when(made == "made" & type_26 == "freeThrow"~1, 
                            TRUE~points)) %>%
  group_by(team_id) %>%
  mutate(team_score = case_when(made=="made"~points)) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) 
            

pbp_israel$result$actions %>%
  unnest(parameters, names_sep = "_") %>%
  janitor::clean_names() %>%
  filter(type != "clock") %>%
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
  group_by(team_id) %>%
  mutate(team_score = case_when(parameters_made=="made"~parameters_points)) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  view()



substiution_data %>%
  view()
  group_by(parameters_player_in, lead_change = lead(parameters_player_out), quarter_time,quarter) %>%
  summarise(n = n(), .groups = "drop") %>%
  filter(parameters_player_in == lead_change)


all_players_in_game %>%
  cross_join(substiution_data) %>%
  filter(team_id.x == team_id.y) %>%
  mutate(is_on = case_when(player_id.x==parameters_player_in~TRUE,
                           player_id.x == parameters_player_out~FALSE,
                           TRUE~NA)) %>%
  arrange(quarter, desc(quarter_time), user_time) %>%
  group_by(player_id.x) %>%
  fill(is_on, .direction = "down") %>%
  ungroup() %>%
  group_by(quarter, quarter_time, player_id.x,team_id.x) %>%
  summarise(is_on_verdict = last(is_on)) %>%
  pivot_wider(id_cols = c(quarter, quarter_time, team_id.x), names_from = player_id.x,
              values_from = is_on_verdict) %>%
  arrange(quarter, desc(quarter_time)) %>%
  replace(is.na(.), 0) %>%
  #mutate(across(.cols = matches("\\d"), ~ replace(.x, is.infinite(.x), 0))) %>%
  mutate(across(.cols = matches("\\d"), ~.x > 0)) %>%
  mutate(row_sum = rowSums(across(.cols = matches("\\d")))) %>%
  view()


df_test_pivot <- df_test %>%
  pivot_longer(-c(quarter, quarter_time, team_id.x))


?is.infinite
  
df_test_pivot %>%
  inner_join(df_test_pivot, by = c("name", "team_id.x")) %>%
  filter(quarter_time.x == "00:50", quarter_time.x != quarter_time.y) %>%
  mutate(change_value = value.x != value.y,) %>%
  relocate(value.y, .after = value.x) %>%
  view()

cbind(pbp_israel$result$gameInfo$homeTeam$players, pbp_israel$result$gameInfo$homeTeam$id

all_players_in_game <- pbp_israel$result$actions %>%
  unnest(parameters, names_sep = "_") %>%
  janitor::clean_names() %>%
  filter(type != "clock") %>%
  filter(type == "substitution") %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)
  

substiution_data <- pbp_israel$result$actions %>%
  unnest(parameters, names_sep = "_") %>%
  janitor::clean_names() %>%
  filter(type != "clock") %>%
  filter(type == "substitution") %>%
  mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
         parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))




  
  
  relocate(player_id, .after = parameters_player_out) %>%
  pivot_wider(id_cols = c(quarter_time), names_from = "player_id",names_prefix = "player_", values_from = player_id) %>% 
  view()
 

options(digits = 4)
                              
                                 
  view()

  view()