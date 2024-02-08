
progressr::with_progress({
  nba_pbp <- hoopR::load_nba_pbp(seasons = c(2008:2024))
})




test <- nba_pbp %>%
  filter(qtr <= 4) %>%
  group_by(game_id, season, season_type) %>%
  mutate(total_score = home_score+away_score, 
         score_diff = total_score - lag(total_score)) %>%
  mutate(stint_type = case_when(score_diff == 0 & lag(score_diff) != 0~ "start_stint", 
                                score_diff !=0 & lag(score_diff) == 0~"end_stint")) %>%
  
  check <- test %>%  
    select(game_id,game_date, season, season_type,
           home_team_abbrev, away_team_abbrev,
           away_score, home_score, total_score,
           score_diff, 
           end_game_seconds_remaining, 
           stint_type, 
           qtr) %>%
    filter(!is.na(stint_type)) %>%
    mutate(time_no_score = abs(end_game_seconds_remaining - lag(end_game_seconds_remaining))) %>%
    ungroup() %>%
    filter(time_no_score != 300.0) %>%
    slice_max(n = 100,order_by = time_no_score) %>%
    mutate(rank_time = dense_rank(desc(time_no_score))) %>%
    view()
  

  check %>%
    filter(season_type == 3) %>%
    view()
  
  check %>%
    filter(season == 2024) %>%
    arrange(desc(time_no_score)) %>%
    slice_max(n = 100, order_by = time_no_score) %>%
    mutate(rank_time = dense_rank(desc(time_no_score))) %>%
    view()
  