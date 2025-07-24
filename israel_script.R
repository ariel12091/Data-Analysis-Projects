pbp_israel <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62428",simplifyVector = TRUE)

pbp_israel$result$actions %>%
  unnest(parameters, names_repair = "universal") %>%
  janitor::clean_names() %>%
  filter(type_26 == "substitution") %>%
  filter(type_26 != "clock") %>%
  mutate(pct_ft = free_throw_number / free_throws_awarded) %>%
  mutate(end_poss = case_when(type_26 == "shot" & made == "made" ~ TRUE,
                              lead(type_4) == "defensive" & lead(type_26) == "rebound"~TRUE,
                              type_26 == "freeThrow" & made == "made" & pct_ft == 1~TRUE,
                              type_26 == "turnover"~TRUE)) %>%
  mutate(points = case_when(made == "made" & type_26 == "freeThrow"~1, 
                            TRUE~points)) %>%
  group_by(team_id) %>%
  mutate(team_score = case_when(made=="made"~points)) %>%
  group_by(team_id) %>%
  summarise(total_poss = as.numeric(sum(end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  view()
                              

options(digits = 4)
                              
                                 
  view()

  view()