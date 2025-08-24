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


dbWriteTable(conn = con, "actions_clean", pbp_clean__games, overwrite = TRUE)

?dbWriteTable



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

json_box

json_resp[[1]]$result$gameInfo$homeTeam$players

f

json_box <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=62428", simplifyVector = TRUE)

links_box <- glue::glue("https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id={israel_schdule_clean_links$ExternalID}")

links_box <- glue::glue("")

links <- glue::glue("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id={israel_schdule_clean_links$ExternalID}")

jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/gameStats.php?game_id=62428&lang=he")  

pbp_israel$result$gameInfo$homeTeam <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62428",simplifyVector = TRUE)

pbp_israel2 <- jsonlite::read_json("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=62426", simplifyVector = TRUE) %>%
  
json_data <- jsonlite::read_json("https://basket.co.il/pbp/json/games_all.json?1753473308320") 

df_list <- map(json_resp, ~list(colnames(.x$result$actions %>% unnest(parameters, names_sep = "_")),
                              .x$result$gameInfo$gameId))


json_box <- future_map(links_box, ~jsonlite::read_json(.x, simplifyVector = TRUE))

  
json_box[[1]]$result$boxscore$homeTeam


json_box[[1]]$result$boxscore$homeTeam$players %>% 
mutate(team_id = json_box[[1]]$result$boxscore$gameInfo$homeTeamId)


json_box[[1]]$result$boxscore$awayTeam$players


json_box[[1]]$result$boxscore$homeTeam$players %>%
  mutate(json_box[[1]]$result$boxscore$gameInfo$homeTeamId,)


json_box[[1]]$result$boxscore$gameInfo$homeTeamId


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


tictoc::tic()
pbp_clean__games <- map(json_resp, ~initial_clean(.x)) %>%
  list_rbind()
tictoc::toc()



#count points and possesions  

pts_poss <- function(pbp_cleaned) {

  
poss <- actions %>%
   mutate(pct_ft = parameters_free_throw_number / parameters_free_throws_awarded) %>%
   mutate(parent_action_id = case_when(parent_action_id == 0~id,
                                       TRUE~parent_action_id)) %>%
  mutate(q_bucket=  if_else(quarter < 5, 0L, quarter)) %>%
   group_by(game_id, parent_action_id) %>%
  dbplyr::window_order(id, quarter, desc(end_game_seconds_remaining), user_time) %>%
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

dbGetQuery(con,
           "SHOW INDEX")


json_box$result$boxscore$homeTeam$players

  json_box$result$boxscore$awayTeam$players

dbGetQuery(con, "SELECT *
           FROM stints")


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



all_players_in_roster <- players_data %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)

roster_helper <- function (x) {
  if(is.null(x$result$gameInfo$homeTeam)|is.null(x$result$gameInfo$awayTeam)) {
    return(tibble::tibble(player_id = character(), team_id = character()))
  }
  else {
  players_roster <- bind_rows(x$result$gameInfo$awayTeam$players %>%
  mutate(team_id = x$result$gameInfo$awayTeam$id, 
         game_id = x$result$gameInfo$gameId),
  x$result$gameInfo$homeTeam$players %>%
      mutate(team_id = x$result$gameInfo$homeTeam$id, 
             game_id = x$result$gameInfo$gameId)) %>% 
  rename(player_id = id)
  }
  return(players_roster)
}



poss %>%
  distinct(team_id, game_id) %>%
  filter(team_id != 0) %>%
  summarise(n = n_distinct(game_id))
  
  
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



df_games_teams

DBI::dbExecute(con, "SHOW TABLES")



character()
is.null(json_resp[[104]]$result$gameInfo$homeTeam)

players_data_full <- furrr::future_map(json_resp, ~roster_helper(.x))  %>%
  list_rbind() %>%
  distinct() %>%
  mutate(is_on = FALSE)


poss %>%
  distinct(game_id) %>%
  tally()


poss %>%
  filter(game_id == 62522)

players_data_full %>%
  filter(game_id == "62522")

df_games_teams %>%
  filter(game_id == "62522")

players_data_with_games <- players_data_full %>%
  distinct() %>%
  inner_join(df_games_teams, by = c("team_id"))

copy_to(con, players_data_full, "full_rosters", overwrite = TRUE, temporary = FALSE)

DBI::dbExecute(con, "TRUNCATE TABLE full_rosters;")

dbWriteTable(con, "full_rosters", players_)

players_data_full %>%
  janitor::clean_names() %>%
  mutate(api_id = player_id) %>%
  select(-c(in_roster)) %>%
  distinct(player_id, team_id, .keep_all = TRUE) %>%
  group_by(player_id) %>%
  summarise(n = n(), .groups = "drop",
            jersey_num_count = n_distinct(jersey_number)) %>%
  filter(n > 1)
  
  arrange(team_id) %>%
  view()
  distinct(player_id, firstName, lastName, )
  group_by(team_id) %>%
  summarise(n = n_distinct(player_id)) %>%
  view()



json_resp[[3]]$result$gameInfo$awayTeam$players

roster_helper <- function(.x) {
  bind_rows(
    tibble::as_tibble(.x$result$gameInfo$awayTeam$players) %>%
      dplyr::mutate(team_id = .x$result$gameInfo$awayTeam$id),
    tibble::as_tibble(.x$result$gameInfo$homeTeam$players) %>%
      dplyr::mutate(team_id = .x$result$gameInfo$homeTeam$id)
  ) %>%
    dplyr::rename(player_id = id)
}

all_players_in_game <- pbp_action_data_clean %>%
  filter(type == "substitution") %>%
  distinct(player_id, team_id) %>%
  mutate(is_on = FALSE)

substiution_data <- actions %>%
  filter(type == "substitution") %>%
  mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
         parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))


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

players_data_with_games %>%
  filter(game_id == "62522")

DBI::dbWriteTable(con, "sub_data", substiution_data, temporary = TRUE)

DBI::dbWriteTable(con, "full_rosters", players_data_with_games, temorary = TRUE)

subs <- tbl(con, "sub_data")

full_rosters <- tbl(con, "full_rosters")

DBI::dbGetQuery(con, "show tables")
  
  full_rosters %>%
    filter(player_id == "19461")



dbGetQuery(con, "SELECT *
           FROM pws") %>%
  view()
  
test_r <- full_rosters %>%
  collect() %>%
  mutate(team_id = as.integer(team_id)) %>%
  distinct(player_id, game_id, team_id) %>%
  inner_join(subs %>% collect(), by = c("team_id", "game_id")) %>%
  #filter(team_id.x == team_id.y) %>%
  mutate(is_on = case_when(player_id.x==parameters_player_in~TRUE,
                           player_id.x == parameters_player_out~FALSE,
                           TRUE~NA)) %>%
  #mutate(str_change = !is.na(is_on)) %>%
  filter(game_id == "62428") %>%
  group_by(game_id, player_id.x) %>%
  arrange(id, quarter, desc(end_game_seconds_remaining), user_time) %>%
  fill(is_on, .direction = "down") %>%
  ungroup() %>%
  filter(player_id.x == "2081") %>%
  filter(end_game_seconds_remaining == 600) %>%
  group_by(game_id, quarter, quarter_time, end_game_seconds_remaining,
           end_quarter_seconds_remaining, player_id.x,team_id) %>%
  summarise(is_on_verdict = last(is_on), .groups = "drop") %>%
  filter(player_id.x == 2081) %>%
  view()

dbGetQuery(con, "SHOW TABLES")  

full_rosters %>%
  filter(player_id == "32331") %>%
  view()

subs %>%
  filter(game_id == "62466", quarter == 1, end_game_seconds_remaining == 1954) %>%
  view()


df_lineups <- full_rosters %>%
  mutate(team_id = as.integer(team_id)) %>%
  select(player_id, game_id, team_id) %>%
  inner_join(subs, by = c("team_id", "game_id")) %>%
  #filter(team_id.x == team_id.y) %>%
  mutate(is_on = case_when(player_id.x==parameters_player_in~TRUE,
                           player_id.x == parameters_player_out~FALSE,
                           TRUE~NA)) %>%
  group_by(game_id, player_id.x) %>%
  dbplyr::window_order(id, quarter, desc(end_game_seconds_remaining), user_time) %>%
  fill(is_on, .direction = "down") %>%
  mutate(.ord = row_number()) %>%
  ungroup() %>%
  slice_max(
    order_by = .ord, n = 1, with_ties = FALSE,
    by = c(game_id, quarter, quarter_time, end_game_seconds_remaining,
           end_quarter_seconds_remaining, player_id.x, team_id)
  ) %>%
  select (id, game_id, player_id = player_id.x, team_id,
            quarter, quarter_time, end_game_seconds_remaining,
            end_quarter_seconds_remaining,
            is_on_verdict = is_on) %>%
  mutate(
    lineup_id = dbplyr::sql("
    string_agg(
      DISTINCT CASE WHEN is_on_verdict IS TRUE THEN CAST(player_id AS VARCHAR) END,
      '_' ORDER BY CAST(player_id AS BIGINT)
    )
    OVER (
      PARTITION BY game_id, team_id, quarter, end_game_seconds_remaining
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  "),
    n_on = dbplyr::sql("
    sum(CASE WHEN is_on_verdict IS TRUE THEN 1 ELSE 0 END)
    OVER (PARTITION BY game_id, team_id, quarter, end_game_seconds_remaining)
  ")) %>% 
  mutate(lineup_hash = dbplyr::sql("md5(lineup_id)")) %>%
  view("test")
  tally()
  
compute(con, df_lineups, "lineups_lookup")


lineups_segments <- df_lineups %>%
  distinct(id, game_id, team_id, quarter, quarter_time, end_game_seconds_remaining, end_quarter_seconds_remaining, 
           lineup_id, lineup_hash) %>%
  group_by(game_id, team_id) %>%
  dbplyr::window_order(quarter, desc(end_game_seconds_remaining),
                       id) %>%
  left_join(poss %>%
               select(id, game_id, quarter, team_id) %>%
               group_by(quarter, game_id, team_id) %>%
               summarise(last_id = max(id)) %>%
                           filter(quarter >= 4), by = c("game_id", "quarter", "team_id")) %>%
  mutate(start_segment = end_game_seconds_remaining, 
         end_segment = case_when(lead(quarter) > quarter & lead(quarter) >= 5 ~ 0,
                                 TRUE~coalesce(lead(end_game_seconds_remaining),0)),
         start_id = id,
         end_id = coalesce(lead(id), last_id)) %>%
          ungroup() %>% select(-last_id)



poss %>%
 filter(game_id == "62437") %>%
  view()

inner_join(poss %>%
             select(id, game_id) %>%
             group_by(game_id) %>%
             summarise(last_id = last(id)), by = c("game_id"))

df_pts_poss_lineups


# Materialize to staging table
df_lineups %>%
  compute(name = sql('"lineups lookup_stage"'), temporary = FALSE)

## 2) Create FINAL table, physically ordered for your common partitions
DBI::dbExecute(con, '
  CREATE OR REPLACE TABLE "lineups lookup" AS
  SELECT *
  FROM "lineups lookup_stage"
  ORDER BY game_id, team_id, quarter, end_game_seconds_remaining, player_id;
')

# Drop stage
DBI::dbExecute(con, 'DROP TABLE IF EXISTS "lineups lookup_stage";')

## 3) Indexes on "lineups lookup" (minimal, high-impact)
# a) Slice index (helps slice/window probes if needed)
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineups_lookup_slice
  ON "lineups lookup"(game_id, team_id, quarter, end_game_seconds_remaining);
')

# b) Hash index — hot path: poss ↔ lineups_lookup by lineup_hash
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineups_lookup_hash
  ON "lineups lookup"(lineup_hash);
')

# c) Player-centric lookups (on/off)
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineups_lookup_player_on
  ON "lineups lookup"(player_id, is_on_verdict);
')
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineups_lookup_player_game
  ON "lineups lookup"(player_id, game_id);
')

## 4) Helper tables for fastest joins/lookups

# A) Dictionary: lineup_hash → lineup_id (5-man string)
DBI::dbExecute(con, '
  CREATE OR REPLACE TABLE lineup_dim AS
  SELECT DISTINCT lineup_hash, lineup_id
  FROM "lineups lookup";
')
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineup_dim_hash
  ON lineup_dim(lineup_hash);
')

# B) Inverted index: which players are ON in each lineup
DBI::dbExecute(con, '
  CREATE OR REPLACE TABLE lineup_players AS
  SELECT DISTINCT lineup_hash, player_id
  FROM "lineups lookup"
  WHERE is_on_verdict IS TRUE;
')
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineup_players_hash
  ON lineup_players(lineup_hash);
')
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_lineup_players_player
  ON lineup_players(player_id);
')

## 5) (Optional) If poss joins by lineup_hash, index it too
DBI::dbExecute(con, '
  CREATE INDEX IF NOT EXISTS idx_poss_lineup_hash
  ON pws(lineup_hash);
')


## 6) Refresh stats for good plans
DBI::dbExecute(con, 'ANALYZE;')

DBI::dbExecute(con, 'ALTER TABLE "lineups lookup" RENAME TO lineups_lookup;')

dbGetQuery(con, "SHOW TABLES")

dbGetQuery(con, "SELECT *
           FROM lineup_players")


df_pts_poss_lineups_longer %>%
  group_by(lineup_hash) %>%
  summarise(total_poss = sum(final_end_poss, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(is.na(total_poss)) %>%
  inner_join(df_pts_poss_lineups_longer, by = c("lineup_hash")) %>%
  group_by(lineup_hash) %>%
  dbplyr::window_order(lineup_hash, id, quarter, desc(end_game_seconds_remaining), user_time) %>%
  filter(lineup_hash == "9940098e2c71973811abdb0d993f5fe9") %>%
  view()
  
df_pts_poss_lineups %>%
  filter(quarter == 1, game_id == "62437") %>%
  view()

stints %>%
  filter(game_id == "62437") %>%
  view()


  filter(lineup_hash_defense == "91a5d0e24fc509163ec4d70f2e546970"|
           lineup_hash_offense == "91a5d0e24fc509163ec4d70f2e546970") %>%
  view("lineup_df")

df_pts_poss_lineups_longer %>%
  filter(segment_id == 31 & game_id == 62437) %>%
  view()
  filter(lineup_hash == "91a5d0e24fc509163ec4d70f2e546970") %>%
  view()

pws %>%
  group_by(lineup_hash_offense) %>%
  
  
stints <- lineups_segments %>%
  ungroup() %>%
  dbplyr::window_order(quarter, desc(start_segment)) %>%
  inner_join(lineups_segments, by = c("game_id"), suffix = c("_offense", "_defense")) %>%
  filter(team_id_offense != team_id_defense,
         quarter_offense < 5 & quarter_defense < 5 |
           quarter_offense >=5 & quarter_defense >= 5 & quarter_offense == quarter_defense) %>%
  mutate(final_start_seg = pmin(start_segment_offense, start_segment_defense),
         final_end_seg = pmax(end_segment_offense, end_segment_defense),
         quarter_min = pmin(quarter_offense, quarter_defense),
         quarter_max = pmax(quarter_offense, quarter_defense),
         final_start_id = pmax(start_id_offense, start_id_defense),
         final_end_id = pmin(end_id_offense, end_id_defense)) %>%
  mutate(q_bucket = if_else(quarter_min < 5 & quarter_min < 5, 0L, quarter_min)) %>%
  relocate(c(final_start_seg, final_end_seg), .after = team_id_offense) %>%
  #filter(final_end_seg < final_start_seg) %>%
  filter(final_end_id > final_start_id) %>%
  filter(final_end_seg == final_start_seg) %>%
  filter(game_id == 62514) %>%
  view()
  group_by(game_id, final_start_id, final_end_id) %>%
  mutate(segment_id = dbplyr::sql("DENSE_RANK() OVER (
  PARTITION BY game_id
  ORDER BY final_start_id, final_end_seg)")) %>%
  relocate(segment_id, .after = final_end_id) %>%
  ungroup() %>%
  relocate(c(quarter_min, quarter_max, quarter_defense, q_bucket), .after = quarter_offense) %>%
  dbplyr::window_order(quarter_offense, desc(final_start_seg)) %>%
  rename(team_id = team_id_offense) %>%
  select(team_id, game_id, final_start_seg, final_end_seg, segment_id,
         lineup_hash_offense, lineup_hash_defense, team_id_defense, q_bucket,
         final_start_id, final_end_id) %>%
  window_order(desc(final_start_id)) %>%
  view()


  
subs %>%
  filter(game_id == 62447, quarter == 4) %>%
  view()

df_lineups %>%
  filter(game_id == 62452, end_game_seconds_remaining <= 600, team_id == 11) %>%
  arrange(desc(end_game_seconds_remaining)) %>%
  view()
  filter(is.na(lineup_hash)) %>%
  view()



actions %>%
  group_by(game_id) %>%
  summarise(n = n_distinct(end_game_seconds_remaining), .groups = "drop") %>%
  filter(n < 300) %>%
  ggplot(aes(x=n)) + geom_histogram()


  

actions %>%
  filter(game_id == "62567") %>%
  arrange(desc(end_game_seconds_remaining), user_time) %>%
  view()
  
tictoc::tic("step 1")
  full_rosters %>%
    mutate(team_id = as.integer(team_id)) %>%
    distinct(player_id, game_id, team_id) %>%
    inner_join(subs, by = c("team_id", "game_id")) %>%
    #filter(team_id.x == team_id.y) %>%
    mutate(is_on = case_when(player_id.x==parameters_player_in~TRUE,
                             player_id.x == parameters_player_out~FALSE,
                             TRUE~NA)) %>%
    #mutate(str_change = !is.na(is_on)) %>%
    filter(game_id == "62428") %>%
    group_by(game_id, player_id.x) %>%
    dbplyr::window_order(quarter, desc(end_game_seconds_remaining), user_time) %>%
    fill(is_on, .direction = "down") %>%
    ungroup() %>%
    filter(player_id.x == "2700") %>%
    group_by(game_id, quarter, quarter_time, end_game_seconds_remaining,
             end_quarter_seconds_remaining, player_id.x,team_id, user_time) %>%
    filter(end_game_seconds_remaining == "600") %>%
    summarise(is_on_verdict = last(is_on), .groups = "drop") %>%
    view()
    show_query()
    filter(player_id.x == "31244") %>%
    view()
    
    group_by(game_id, quarter, quarter_time, end_game_seconds_remaining,
             end_quarter_seconds_remaining,
             team_id)
    
      pivot_wider(id_cols = c(game_id, quarter, quarter_time, end_game_seconds_remaining,
                              end_quarter_seconds_remaining,
                              team_id, user_time), names_from = player_id.x,
                  values_from = is_on_verdict) %>%
    #select(end_game_seconds_remaining, team_id,  c('2515', '31430')) %>%
    view()
    group_by(game_id, quarter, quarter_time, end_game_seconds_remaining,
             end_quarter_seconds_remaining,
             team_id) %>%
    dbplyr::window_order(quarter, desc(quarter_time), user_time) %>%
    filter(player_id.x == "19461") %>%
    view()

    
    
    
    pivot_wider(id_cols = c(game_id, quarter, quarter_time, end_game_seconds_remaining,
                            end_quarter_seconds_remaining,
                            team_id), names_from = player_id.x,
                values_from = is_on_verdict) %>%
    show_query()
    group_by(game_id, quarter, quarter_time, end_game_seconds_remaining,
                            end_quarter_seconds_remaining,
                           team_id) %>%
    dbplyr::window_order(quarter, desc(quarter_time)) %>%
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
  tictoc::tic()
    
    
    
    
    
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
  filter(game_id == '62490') %>%
  view()

  
lineup_data <- tbl(con, 'df_lineups')
  
stints %>%
  filter(game_id == '64899') %>%
  arrange(quarter, desc(final_start_seg)) %>%
    view()

substiution_data %>%
  filter(game_id == "64899") %>%
  view()
  

poss %>%
  group_by(game_id, team_id) %>%
  summarise(total_pts = sum(team_score, na.rm = TRUE),
            total_poss = sum(final_end_poss, na.rm = TRUE)) %>%
  mutate(pts_per_poss = total_pts / total_poss) %>%
  filter(team_id != 0) %>%
  arrange(game_id) 

df_lineups %>%
  tally()
   
  
player_cols <- colnames(df_lineups %>%
                          select(matches("\\d")))

lineups_lookup <- df_lineups %>%
  rowwise() %>%
  mutate(lineup_id = paste(sort(player_cols[c_across(player_cols)]),collapse = "_"),
         lineup_hash = digest::digest(lineup_id, algo = "xxhash64")) %>%
  ungroup()
  
sort(player_cols[5:11])
    
lineups_hush <- lineups_lookup %>%
  select(-matches("\\d"))  


df_lineups %>%
  summarise(n = n_distinct(game_id))


stints %>%
  filter(id == "648990732") %>%
  view()

stints <- lineups_hush %>%
  arrange(quarter, desc(start_segment)) %>%
  inner_join(lineups_hush, by = c("game_id"), suffix = c("_offense", "_defense")) %>%
  filter(team_id_offense != team_id_defense,
         quarter_offense < 5 & quarter_defense < 5 |
           quarter_offense >=5 & quarter_defense >= 5 & quarter_offense == quarter_defense) %>%
  mutate(final_start_seg = pmin(start_segment_offense, start_segment_defense),
         final_end_seg = pmax(end_segment_offense, end_segment_defense),
         quarter_min = pmin(quarter_offense, quarter_defense),
         quarter_max = pmax(quarter_offense, quarter_defense)) %>%
  mutate(q_bucket = if_else(quarter_min < 5 & quarter_min < 5, 0L, quarter_min)) %>%
  relocate(c(final_start_seg, final_end_seg), .after = team_id_offense) %>%
  filter(final_end_seg < final_start_seg) %>%
  group_by(game_id, final_start_seg, final_end_seg) %>%
  mutate(segment_id = cur_group_id()) %>%
  relocate(segment_id, .after = final_end_seg) %>%
  ungroup() %>%
  relocate(c(quarter_min, quarter_max, quarter_defense, q_bucket), .after = quarter_offense) %>%
  arrange(quarter_offense, desc(final_start_seg)) %>%
  rename(team_id = team_id_offense) %>%
  select(team_id, game_id, final_start_seg, final_end_seg, segment_id,
         lineup_hash_offense, lineup_hash_defense, team_id_defense,
         row_sum_defense, row_sum_offense, q_bucket) %>%
  arrange(desc(final_start_seg)) %>%
  view()

stints %>%
  filter(game_id == "62490" ) %>%
  arrange(quarter, desc(final_start_seg)) %>%
  view()

#add stints to ppp

by <- join_by(team_id, game_id, q_bucket, between(end_game_seconds_remaining, final_end_seg, final_start_seg, bounds = "(]"))

class

# overwrite safely
DBI::dbExecute(con, "DROP TABLE IF EXISTS stints;")

stints %>%
  compute(name = "stints", temporary = FALSE)


stints_tbl <- tbl(con, "stints")

df_pts_poss_lineups <- left_join(poss %>%
                                   mutate(end_game_seconds_remaining = if_else(end_game_seconds_remaining == 0,  
                                                                               end_game_seconds_remaining + 0.00001,        
                                                                               end_game_seconds_remaining)), stints_tbl, by)



class(df_lineups)

df_lineups %>%
  summarise(n = n_distinct(game_id))


class(df_pts_poss_lineups)

df_pts_poss_lineups %>%
  filter(id == "625220113") %>%
  view()

  
df_pts_poss_lineups %>%
  filter(team_id != 0) %>%
  filter(is.na(lineup_hash_defense)) %>%
  filter(game_id %in% c('62447', '62452', '62534')) %>%
  view()


df_pts_poss_lineups %>%
  filter(round(end_game_seconds_remaining,1) == 0) %>%
  distinct(game_id)

df_pts_poss_lineups %>%

df_pts_poss_lineups %>%
  collect() %>%
  nrow()

DBI::dbGetQuery(con, "SELECT COUNT(*) FROM possessions")

DBI::dbGetQuery(con, "SELECT COUNT(*) FROM pws")

df_pts_poss_lineups %>%
  tally()



compute(df_pts_poss_lineups, "pws", temporary = FALSE, overwrite = TRUE)

df_pts_poss_lineups %>%


  
df_games_teams %>%
  filter(game_id == "62490")

df_pts_poss_lineups %>%
  colnames()

games_check <- df_pts_poss_lineups %>%
  arrange(quarter, desc(end_game_seconds_remaining)) %>%
  filter(round(end_game_seconds_remaining, 1) == 0) %>%
  filter(is.na(team_id_defense)) %>%
  filter(!is.na(team_score)) %>%
  select(quarter, game_id, end_game_seconds_remaining, final_start_seg, final_end_seg) %>%
  view()
  filter(!game_id %in% c('62527', '62541', '62522')) %>%
  view()
  select(team_id, game_id) %>%
  distinct() %>%
  left_join(players_data_with_games %>%
              mutate(test = TRUE), by = c("team_id", "game_id")) %>%
  distinct(game_id, test)
  distinct(game_id)
  view()
  
poss %>%
  filter(game_id == "62439") %>%
  view()
  
stints %>%
  filter(game_id == "62439") %>%
  arrange(desc(final_start_seg)) %>%
  view("check")
  
df_pts_poss_lineups %>%
  filter(parent_action_id != id) %>%
  group_by(id) %>%
  summarise(n = n(), .groups = "drop") %>%
  filter(n > 1) %>%
  tally()
  
dbGetQuery(con, "SHOW TABLES")




compute(df_games_teams, "games_teams", temporary = TRUE)

players_data_with_games <- copy_to(con, df_games_teams, temporary = TRUE)

DBI::dbWriteTable(con, name = "lineup_lookup",
                  )
library(DBI)
library(dplyr)

## Build the lazy UNION ALL while keeping all original columns
offense <- df_pts_poss_lineups %>%
  filter(!game_id %in% c("62527","62541","62522")) %>%
  mutate(
    type = "offense",
    lineup_hash = lineup_hash_offense
  ) %>%
  select(-c(lineup_hash_defense, team_id_defense, lineup_hash_offense))

defense <- df_pts_poss_lineups %>%
  filter(!game_id %in% c("62527","62541","62522")) %>%
  mutate(
    type = "defense",
    lineup_hash = lineup_hash_defense,
    team_id = team_id_defense
  ) %>%
  select(-c(lineup_hash_offense, team_id_defense, lineup_hash_defense))

df_pts_poss_lineups_longer <- union_all(offense, defense) %>%
  filter(!is.na(lineup_hash))

## Transaction wrapper: all-or-nothing
tryCatch({
  dbExecute(con, "BEGIN;")
  
  # Clean old stage/final
  dbExecute(con, "DROP TABLE IF EXISTS df_pts_poss_lineups_longer_stage;")
  # Do NOT drop the final yet—only replace after stage succeeds
  
  # Materialize stage
  df_pts_poss_lineups_longer %>%
    compute(name = sql("df_pts_poss_lineups_longer_stage"), temporary = FALSE)
  
  # Create final ordered table
  dbExecute(con, '
    CREATE OR REPLACE TABLE df_pts_poss_lineups_longer AS
    SELECT * FROM df_pts_poss_lineups_longer_stage
    ORDER BY game_id, team_id, type, lineup_hash;
  ')
  dbExecute(con, "DROP TABLE IF EXISTS df_pts_poss_lineups_longer_stage;")
  
  # Indexes tuned for your hot paths
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_dfppl_lineup_hash
      ON df_pts_poss_lineups_longer(lineup_hash);
    CREATE INDEX IF NOT EXISTS idx_dfppl_team_game
      ON df_pts_poss_lineups_longer(team_id, game_id);
    ANALYZE;
  ')
  
  dbExecute(con, "COMMIT;")
}, error = function(e) {
  message("Error occurred: ", conditionMessage(e))
  try(dbExecute(con, "ROLLBACK;"), silent = TRUE)
  stop(e)
})

df_pts_poss_lineups_longer %>%
  group_by(lineup_hash, type) %>%
  summarise(total_pts = sum(team_score, na.rm = TRUE),
            total_poss = sum(final_end_poss, na.rm = TRUE),
            .groups = "drop") %>%
  filter(total_poss >= 100) %>%
  mutate(pts_per_poss = total_pts / total_poss) %>%
  ungroup() %>%
  view()
  inner_join(lineups_lookup %>%
  distinct(player_id, team_)
    
    
df_pts_poss_lineups_longer %>%
  filter(lineup_hash == "bd7aa10465bec18687d05e2da3311968") %>%
  group_by(team_id, type, lineup_hash, game_id, quarter) %>%
  summarise(total_poss = sum(final_end_poss, na.rm = TRUE),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  ungroup() %>%
  view()

  lineups_lookup %>%
          distinct(player_id, team_id, quarter, is_on_verdict, 
                        lineup_hash) %>%
                        filter(is_on_verdict == TRUE) %>%
    filter(lineup_hash == "bd7aa10465bec18687d05e2da3311968") %>%
    inner_join(full_rosters, by = c("player_id")) %>%
    view()
    
    
    filter(team_id == 246) %>%
    inner_join(df_pts_poss_lineups_longer %>%
                 select(id, team_score, final_end_poss, lineup_hash, type), by = c("lineup_hash")) %>%
    group_by(team_id, lineup_hash, type) %>%
    summarise(total_pts = sum(team_score, na.rm = TRUE),
              total_poss = sum(final_end_poss, na.rm = TRUE)) %>%
    inner_join(full_rosters %>%
                 distinct(player_id, firstNameLocal, lastNameLocal), by = c("player_id"))
    ungroup() %>%
    arrange(desc(total_poss)) %>%
    view()
    
segments <- stints_tbl %>%
  filter(lineup_hash_offense == "bd7aa10465bec18687d05e2da3311968") %>%
  filter(game_id == 62602) %>%
  distinct(segment_id, final_start_seg, final_end_seg) %>%
  arrange(desc(segment_id)) %>%
  pull(segment_id) 

df_pts_poss_lineups %>%
  filter(game_id == 62602) %>%
  filter(segment_id %in% segments) %>%
  view()
  group_by(team_id, type) %>%
  summarise(total_pts = sum(team_score, na.rm = TRUE),
            total_poss = sum(final_end_poss, na.rm = TRUE)) %>%
  arrange(team_id, type)

  distinct(game_id)

  

lineups_lookup <- tbl(con, 'lineups lookup')

dbGetQuery(con, "SELECT *
           FROM 'lineups lookup'")

dbGetQuery(con, "SHOW TABLES")
dbGetQuery(con, 
           "SELECT *
            FROM df_pts_poss_lineups_longer 
            ORDER BY game_id, id") %>%
  view()


  df_pts_poss_lineups_longer <- df_pts_poss_lineups %>%
    filter(!game_id %in% c('62527', '62541', '62522')) %>%
    pivot_longer(c(lineup_hash_offense, lineup_hash_defense)) %>%
    mutate(type = str_remove(name, "lineup_hash_")) %>%
    mutate(team_id = case_when(type == "offense"~team_id,
                                     type == "defense"~team_id_defense)) %>%
    select(-c(team_id_defense, name)) %>%
    rename(lineup_hash = value)
  
  group_by(team_id, type, value) %>%
  summarise(total_poss = as.numeric(sum(final_end_poss, na.rm = TRUE)),
            total_pts = sum(team_score, na.rm = TRUE)) %>%
  mutate(pts_per_poss = round((total_pts / total_poss) * 100,1)) %>%
  filter(total_poss >= 100) %>%
  left_join(lineups_lookup %>%
              select(c(matches("\\d"), lineup_hash, lineup_id)) %>%
                       distinct(), by = c("lineup_hash"))

inner_join(lineups_lookup %>%
  select(lineup_id, lineup_hash, team_id, matches("\\d")) %>%
  pivot_longer(-c(lineup_id, lineup_hash, team_id)) %>%
  distinct() %>%
  filter(value == TRUE) %>%
  arrange(lineup_hash), by = c("lineup_hash")) %>%
  inner_join()
  

full_rosters - 
  player_id, player_name, team_id, team_name, season





dbGetQuery(con, "select *
           from full_rosters")

  pivot_longer()
  select(-matches("\\d")) %>%
  colnames()


lineups_lookup

  arrange(value)
  
  view()
  
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
 