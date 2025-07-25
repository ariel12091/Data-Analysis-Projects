pbp_israel <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62428",simplifyVector = TRUE)

pbp_israel2 <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62426", simplifyVector = TRUE)



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
                              

?unnest

pbp_israel2$result$actions %>%
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
  mutate(final_end_poss = case_when(sum_poss_poss >=2 & id == parent_action_id~NA_real_,
                                    sum_block >= 1~lead(end_poss),
                                    sum_tech >= 1 ~ NA_real_,
                                    TRUE~end_poss)) %>%
  ungroup() %>%
  mutate(parameters_points = case_when(parameters_made == "made" & type == "freeThrow" ~ 1, 
                            TRUE~parameters_points)) %>%
  group_by(team_id) %>%
  mutate(team_score = case_when(parameters_made=="made"~parameters_points)) %>%
  view()
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  view()




options(digits = 4)
                              
                                 
  view()

  view()