library(tidyverse)
library(jsonlite)
library("digest")
library(lubridate)
library(httr2)
library(DBI)
library(duckdb)
library(duckplyr)
library(furrr)

con <- dbConnect(duckdb(),
                 dbdir = "israel_pbp.duckdb",read_only = FALSE)

dbExecute(con, paste0("PRAGMA threads=", parallel::detectCores()))


dbWriteTable(conn = con, "actions_clean", pbp_clean__games, append = TRUE)



dbExecute(con, "CREATE TABLE IF NOT EXISTS actions_clean (
            quarter                          TINYINT,
            parameters_team                  INT,
            parameters_player                INT,
            parameters_type                  VARCHAR,
            parameters_quarter               TINYINT,
            parameters_player_in             INT,
            parameters_player_out            INT,
            parameters_current_quarter       TINYINT,
            parameters_current_quarter_time  VARCHAR,
            parameters_coord_x               SMALLINT,
            parameters_coord_y               SMALLINT,
            parameters_points                SMALLINT,
            parameters_fast_break            BOOLEAN,
            parameters_second_chance_points  BOOLEAN,
            parameters_points_from_turnover  BOOLEAN,
            parameters_made                  VARCHAR,
            parameters_kind                  VARCHAR,
            parameters_fouled_on             INT,
            parameters_free_throws           SMALLINT,
            parameters_free_throws_awarded   SMALLINT,
            parameters_free_throw_number     SMALLINT,
            id                               BIGINT,
            parent_action_id                 BIGINT,
            user_time                        TIMESTAMP,
            quarter_time                     VARCHAR,
            type                             VARCHAR,
            player_id                        INT,
            team_id                          INT,
            score                            VARCHAR,
            total_player_points              SMALLINT,
            game_id                          INT,
            end_quarter_seconds_remaining    INT,
            end_game_seconds_remaining       INT,
            parameters_is_coach_foul         BOOLEAN,
            parameters_is_bench_foul         BOOLEAN
          );")


dbGetQuery(con, "SELECT * 
           FROM actions_clean")

actions <- tbl(con, "actions_clean")


#scraping

israel_schedule <- map(json_data[[1]]$games, ~as.data.frame(.x)) %>%
  list_rbind()

israel_schdule_clean_links <- israel_schedule %>%
  mutate(game_id_count = str_count(pbp_link, "game_id")) %>%
  mutate(pbp_link = case_when(game_id_count > 1~str_remove(pbp_link,"\\?game_id.*?(?=\\?game_id)"),
                                   TRUE~pbp_link)) %>%
  mutate(pbp_link =  str_remove_all(pbp_link, "&lang=he(?=.*&lang=he)"))


req_list <- map(links, ~request(.x) %>% req_headers(Accept = "application/json"))

json_resp <- httr2::req_perform_parallel(req_list[1:10], on_error = "continue", progress = TRUE) %>%
  
  
json_resp <- future_map(links, ~jsonlite::read_json(.x, simplifyVector = TRUE))




links <- glue::glue("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id={israel_schdule_clean_links$ExternalID}")

jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/gameStats.php?game_id=62428&lang=he")  

pbp_israel$result$gameInfo$homeTeam <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62428",simplifyVector = TRUE)

pbp_israel2 <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62426", simplifyVector = TRUE) %>%
  
json_data <- jsonlite::read_json("https://basket.co.il/pbp/json/games_all.json?1753473308320") 

df_list <- map(json_resp, ~list(colnames(.x$result$actions %>% unnest(parameters, names_sep = "_")),
                              .x$result$gameInfo$gameId))



  
  
json_resp[[1]]$

#initial clean

json_resp[[1]]$result$actions

israel_schedule$game_year

initial_clean <- function(pbp) {

pbp_action_data_clean <- pbp$result$actions %>%
  unnest(parameters, names_sep = "_") %>%
  janitor::clean_names() %>%
  mutate(game_id = pbp$result$gameInfo$gameId) %>%
  filter(type != "clock") %>% 
  mutate(end_quarter_seconds_remaining = lubridate::period_to_seconds(ms(quarter_time))) %>%
  mutate(end_game_seconds_remaining = case_when(quarter < 5 ~ end_quarter_seconds_remaining + (4-quarter)*600,
                                                quarter >= 5 ~ end_quarter_seconds_remaining))

  return(pbp_action_data_clean)

}


pbp_clean__games <- future_map(json_resp, ~initial_clean(.x)) %>%
  list_rbind()


#count points and possesions  

pts_poss <- function(pbp_cleaned) {
         
poss <- actions %>%
   mutate(pct_ft = parameters_free_throw_number / parameters_free_throws_awarded) %>%
   mutate(parent_action_id = case_when(parent_action_id == 0~id,
                                       TRUE~parent_action_id)) %>%
   group_by(game_id, parent_action_id) %>%
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

return (df_pts_poss)

}

compute(poss, name = "possessions", temporary = FALSE, overwrite = TRUE, cte = TRUE)
compute(players_data_with_games, 
        name = "full_rosters",temporary = FALSE, overwrite = TRUE)

dbWriteTable(con, "full_rosters", players_data_with_games)




dbGetQuery(con, "SELECT *
           FROM full_rosters")


DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_poss_gtt ON possessions(game_id, team_id, end_game_seconds_remaining);")

poss <- tbl(con, "possessions")

poss %>%
  filter(team_id != 0) %>%
  group_by(game_id, team_id) %>%
  summarise(total_poss = sum(final_end_poss, na.rm = TRUE),
            total_points = sum(team_score, na.rm = TRUE)) %>%
  mutate(ppp = total_points / total_poss) %>%
  arrange(desc(ppp)) %>%
  show_query()

poss %>%
  filter(game_id == "62466") %>%
  relocate(c(end_poss, final_end_poss),.after = score) %>%
  arrange(user_time) %>%
  view()


df_poss <- as.data.frame(poss %>% filter(game_id == ))


#lineups

players_data <- bind_rows(pbp_israel$result$gameInfo$homeTeam$players %>%
  mutate(team_id = pbp_israel$result$gameInfo$homeTeam$id),
pbp_israel$result$gameInfo$awayTeam$players %>% mutate(team_id = pbp_israel$result$gameInfo$awayTeam$id)) %>% 
  rename(player_id = id)

israel_schedule %>%
  filter(ExternalID == "62424") %>%
  view()

dbExecute(con, "DROP TA")


all_players_in_roster <- players_data %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)

roster_helper <- function (x) {
  if(is.null(x$result$gameInfo$homeTeam)|is.null(x$result$gameInfo$awayTeam)) {
    return(tibble::tibble(player_id = character(), team_id = character()))
  }
  else {
  players_roster <- bind_rows(x$result$gameInfo$awayTeam$players %>%
  mutate(team_id = x$result$gameInfo$awayTeam$id),
  x$result$gameInfo$homeTeam$players %>%
      mutate(team_id = x$result$gameInfo$homeTeam$id)) %>% 
  rename(player_id = id) %>%
  distinct(player_id, team_id)
  }
  
  return(players_roster)
}
  
  
  game_helper <- function (x) {
    if(is.null(x$result$gameInfo$homeTeam)|is.null(x$result$gameInfo$awayTeam)) {
      return(tibble::tibble(game_id = character(), team_id = character()))
    }
    else {
      game_teams <- as.data.frame(rbind(c(x$result$gameInfo$gameId,x$result$gameInfo$awayTeam$id),
                                  c(x$result$gameInfo$gameId, 
                                    x$result$gameInfo$homeTeam$id))) 
    }

  return(game_teams)}

  
df_games_teams <- map(json_resp, ~game_helper(.x))  %>%
  list_rbind() %>%
  select(-c(game_id, team_id)) %>%
  rename(game_id = "V1", 
         team_id = "V2") %>%
  group_by(team_id) %>%
  summarise(n = n()) %>%
  view()

character()
is.null(json_resp[[104]]$result$gameInfo$homeTeam)

players_data_full <- furrr::future_map(json_resp, ~roster_helper(.x))  %>%
  list_rbind() %>%
  distinct() %>%
  mutate(is_on = FALSE)



players_data_with_games <- players_data_full %>%
  distinct() %>%
  inner_join(df_games_teams, by = c("team_id"))

compute(players_data_with_games, name = "full_rosters",temporary = FALSE, overwrite = TRUE)

players_data_full %>%
  group_by(team_id) %>%
  summarise(n = n_distinct(player_id)) %>%
  view()


roster_helper <- function(.x) {
  bind_rows(
    tibble::as_tibble(.x$result$gameInfo$awayTeam$players) %>%
      dplyr::mutate(team_id = .x$result$gameInfo$awayTeam$id),
    tibble::as_tibble(.x$result$gameInfo$homeTeam$players) %>%
      dplyr::mutate(team_id = .x$result$gameInfo$homeTeam$id)
  ) %>%
    dplyr::rename(player_id = id) %>%
    dplyr::distinct(player_id, team_id)
}

all_players_in_game <- pbp_action_data_clean %>%
  filter(type == "substitution") %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)



substiution_data <-  pbp_clean__games %>%
  filter(type == "substitution") %>%
  mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
         parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))

compute(substiution_data, name = "subs", temporary = FALSE, overwrite = TRUE, cte = TRUE)


players_data_full %>%
  inner_join(israel_schedule %>% select(ExternalID, id, ), by = c("team_id" = "ExternalID"))

israel_schdule_clean_links %>%
  view()

pbp_clean__games %>%
  filter()

DBI::dbGetQuery(con, "SHOW TABLES")

DBI::dbExecute(con, "DROP TABLE IF EXISTS subs")

full_rosters <- tbl(con, "full_rosters")

dbGetQuery(con, "SELECT *
           FROM full_rosters")

subs <- tbl(con, "subs")


class(subs)

tictoc::tic()
  df_lineups <- players_data_with_games %>%
    mutate(team_id = as.integer(team_id)) %>%
    inner_join(substiution_data, by = c("team_id", "game_id")) %>%
    #filter(team_id.x == team_id.y) %>%
    mutate(is_on = case_when(player_id.x==parameters_player_in~TRUE,
                             player_id.x == parameters_player_out~FALSE,
                             TRUE~NA)) %>%
    arrange(quarter, desc(quarter_time), user_time) %>%
    group_by(game_id, player_id.x) %>%
    fill(is_on, .direction = "down") %>%
    ungroup() %>%
    group_by(game_id, quarter, quarter_time, end_game_seconds_remaining,
             end_quarter_seconds_remaining, player_id.x,team_id) %>%
    summarise(is_on_verdict = last(is_on), .groups = "drop") %>%
    pivot_wider(id_cols = c(game_id, quarter, quarter_time, end_game_seconds_remaining,
                            end_quarter_seconds_remaining,
                            team_id), names_from = player_id.x,
                values_from = is_on_verdict) %>%
    arrange(quarter, desc(quarter_time)) %>%
    replace(is.na(.), 0) %>%
    mutate(across(.cols = matches("\\d"), ~.x > 0)) %>%
    mutate(row_sum = rowSums(across(.cols = matches("\\d")))) %>%
    #rename(team_id = team_id.x) %>%
    group_by(game_id, team_id) %>%
    mutate(start_segment = end_game_seconds_remaining, 
           end_segment = case_when(lead(quarter) > quarter & lead(quarter) >= 5 ~ 0,
                                   TRUE~coalesce(lead(end_game_seconds_remaining),0))) %>%
    ungroup() %>%
    relocate(c(start_segment, end_segment, end_quarter_seconds_remaining, end_game_seconds_remaining, row_sum),  .after = team_id)
    tictoc::toc()

  
df_lineups %>%
  filter(game_id == '64899') %>%
  view()

  
lineup_data <- tbl(con, 'df_lineups')


  
stints %>%
  filter(game_id == '64899') %>%
    view()
  view()

substiution_data %>%
  filter(game_id == "64899") %>%
  view()
  
player_cols <- colnames(df_lineups %>%
                          select(matches("\\d")))

lineups_lookup <- df_lineups %>%
  rowwise() %>%
  mutate(lineup_id = paste(sort(player_cols[c_across(player_cols)]),collapse = "_"),
         lineup_hash = digest::digest(lineup_id, algo = "xxhash64")) %>%
  
sort(player_cols[5:11])
    
lineups_hush <- lineups_lookup %>%
  select(-matches("\\d"))  



stints %>%
  filter(id == "648990732") %>%
  view()

stints <- lineups_hush %>%
  inner_join(lineups_hush, by = c("game_id", "quarter"), suffix = c("_offense", "_defense")) %>%
  filter(team_id_offense != team_id_defense) %>%
  mutate(final_start_seg = pmin(start_segment_offense, start_segment_defense),
         final_end_seg = pmax(end_segment_offense, end_segment_defense)) %>%
  relocate(c(final_start_seg, final_end_seg), .after = team_id_offense) %>%
  filter(final_end_seg < final_start_seg) %>%
  group_by(game_id, quarter, final_start_seg, final_end_seg) %>%
  mutate(segment_id = cur_group_id()) %>%
  relocate(segment_id, .after = final_end_seg) %>%
  ungroup() %>%
  rename(team_id = team_id_offense) %>%
  select(quarter, team_id, game_id, final_start_seg, final_end_seg, segment_id,
         lineup_hash_offense, lineup_hash_defense, team_id_defense,
         row_sum_defense, row_sum_offense)

#add stints to ppp


stints %>%
  filter(game_id == "64899") %>%
  arrange(quarter, desc(final_start_seg)) %>%
  view()


df_pts_poss_lineups %>%
  group_by()

by <- join_by(team_id, game_id, quarter, between(end_game_seconds_remaining, final_end_seg, final_start_seg, bounds = "(]"))

DBI::dbWriteTable(con, "stints", stints, overwrite = TRUE)

stints_tbl <- tbl(con, "stints")

df_pts_poss_lineups <- left_join(poss, stints_tbl, by)

df_pts_poss_lineups %>%
  collect() %>%
  nrow()

compute(df_pts_poss_lineups, "pws", temporary = FALSE, overwrite = TRUE)


dbGetQuery(con, "SELECT *
           FROM pws")



df_pts_poss_lineups %>%
  filter(id == "648990732") %>%
  select(game_id)
  view()

df_pts_poss_lineups %>%
  filter(parent_action_id != id)  %>%
  filter(quarter < 5) %>%
  group_by(id) %>%
  summarise(n = n(), .groups = "drop") %>%
  filter(n > 1) %>%
  collect() %>%
  nrow()
  


df_pts_poss_lineups_longer <- df_pts_poss_lineups %>%
  pivot_longer(c(lineup_hash_offense, lineup_hash_defense)) %>%
  mutate(type = str_remove(name, "lineup_hash_")) %>%
  mutate(team_id = case_when(type == "offense"~team_id,
                                   type == "defense"~team_id_defense)) %>%
  group_by(team_id, type, value) %>%
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
 