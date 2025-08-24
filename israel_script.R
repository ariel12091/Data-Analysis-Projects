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


actions <- tbl(con, "actions_clean")


#scraping

json_data <- jsonlite::read_json("https://basket.co.il/pbp/json/games_all.json?1753473308320") 

israel_schedule <- map(json_data[[1]]$games, ~as.data.frame(.x)) %>%
  list_rbind()

israel_schdule_clean_links <- israel_schedule %>%
  mutate(game_id_count = str_count(pbp_link, "game_id")) %>%
  mutate(pbp_link = case_when(game_id_count > 1~str_remove(pbp_link,"\\?game_id.*?(?=\\?game_id)"),
                                   TRUE~pbp_link)) %>%
  mutate(pbp_link =  str_remove_all(pbp_link, "&lang=he(?=.*&lang=he)"))

links_pbp <- glue::glue("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id={israel_schdule_clean_links$ExternalID}")

json_resp <- future_map(links, ~jsonlite::read_json(.x, simplifyVector = TRUE))

links_box <- glue::glue("https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id={israel_schdule_clean_links$ExternalID}")

json_box <- future_map(links_box, ~jsonlite::read_json(.x, simplifyVector = TRUE))

#initial clean

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


compute(poss, name = "possessions", temporary = FALSE, overwrite = TRUE, cte = TRUE)
DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_poss_gtt ON possessions(game_id, team_id, end_game_seconds_remaining);")


dbWriteTable(con, "full_rosters", players_data_with_games)

compute(players_data_with_games, name = "full_rosters",temporary = FALSE, overwrite = TRUE)

poss <- tbl(con, "possessions")


#lineups

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

players_data_full <- furrr::future_map(json_resp, ~roster_helper(.x))  %>%
  list_rbind() %>%
  distinct() %>%
  mutate(is_on = FALSE)

copy_to(con, players_data_full, "full_rosters", overwrite = TRUE, temporary = FALSE)

substiution_data <- actions %>%
  filter(type == "substitution") %>%
  mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
         parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))


substiution_data <-  pbp_clean__games %>%
  filter(type == "substitution") %>%
  mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
         parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))

compute(substiution_data, name = "subs", temporary = FALSE, overwrite = TRUE, cte = TRUE)

full_rosters <- tbl(con, "full_rosters")

subs <- tbl(con, "subs")

subs <- tbl(con, "sub_data")

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
  mutate(lineup_hash = dbplyr::sql("md5(lineup_id)")) 
dbGetQuery(con, "SHOW TABLES")  
    
compute(con, df_lineups, "lineups_lookup")


lineups_segments <- df_lineups %>%
  distinct(id, game_id, team_id, quarter, quarter_time, end_game_seconds_remaining, end_quarter_seconds_remaining, 
           lineup_id, lineup_hash) %>%
  group_by(game_id, team_id) %>%
  dbplyr::window_order(quarter, desc(end_game_seconds_remaining),
                       id) %>%
  left_join(poss %>%
               select(id, game_id, quarter, team_id) %>%
               group_by(quarter, game_id) %>%
               summarise(last_id = max(id)) %>%
                           filter(quarter >= 4), by = c("game_id", "quarter")) %>%
  mutate(start_segment = end_game_seconds_remaining, 
         end_segment = case_when(lead(quarter) > quarter & lead(quarter) >= 5 ~ 0,
                                 TRUE~coalesce(lead(end_game_seconds_remaining),0)),
         start_id = id,
         end_id = coalesce(lead(id), last_id)) %>%
          ungroup() %>% select(-last_id)



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
  group_by(game_id, final_start_id, final_end_id) %>%
  mutate(segment_id = dbplyr::sql("DENSE_RANK() OVER (
  PARTITION BY game_id
  ORDER BY final_start_id, final_end_id)")) %>%
  relocate(segment_id, .after = final_end_id) %>%
  ungroup() %>%
  relocate(c(quarter_min, quarter_max, quarter_defense, q_bucket), .after = quarter_offense) %>%
  dbplyr::window_order(quarter_offense, desc(final_start_seg)) %>%
  rename(team_id = team_id_offense) %>%
  select(team_id, game_id, final_start_seg, final_end_seg, segment_id,
         lineup_hash_offense, lineup_hash_defense, team_id_defense, q_bucket,
         final_start_id, final_end_id) %>%
  window_order(desc(final_start_id))


tryCatch({
  dbExecute(con, "BEGIN;")
  
  # 1) Materialize to staging table
  df_lineups %>%
    compute(name = sql('"lineups_lookup_stage"'), temporary = FALSE)
  
  # 2) Create FINAL table, physically ordered
  dbExecute(con, '
    CREATE OR REPLACE TABLE "lineups_lookup" AS
    SELECT *
    FROM "lineups_lookup_stage"
    ORDER BY game_id, team_id, quarter, end_game_seconds_remaining, player_id;
  ')
  
  # Drop stage
  dbExecute(con, 'DROP TABLE IF EXISTS "lineups_lookup_stage";')
  
  # 3) Indexes on "lineups_lookup"
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineups_lookup_slice
    ON "lineups_lookup"(game_id, team_id, quarter, end_game_seconds_remaining);
  ')
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineups_lookup_hash
    ON "lineups_lookup"(lineup_hash);
  ')
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineups_lookup_player_on
    ON "lineups_lookup"(player_id, is_on_verdict);
  ')
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineups_lookup_player_game
    ON "lineups_lookup"(player_id, game_id);
  ')
  
  # 4) Helper tables
  dbExecute(con, '
    CREATE OR REPLACE TABLE lineup_dim AS
    SELECT DISTINCT lineup_hash, lineup_id
    FROM "lineups_lookup";
  ')
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineup_dim_hash
    ON lineup_dim(lineup_hash);
  ')
  
  dbExecute(con, '
    CREATE OR REPLACE TABLE lineup_players AS
    SELECT DISTINCT lineup_hash, player_id
    FROM "lineups_lookup"
    WHERE is_on_verdict IS TRUE;
  ')
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineup_players_hash
    ON lineup_players(lineup_hash);
  ')
  dbExecute(con, '
    CREATE INDEX IF NOT EXISTS idx_lineup_players_player
    ON lineup_players(player_id);
  ')
  
  # 5) (Optional) If poss joins by lineup_hash, index it too
  # dbExecute(con, '
  #   CREATE INDEX IF NOT EXISTS idx_poss_lineup_hash
  #   ON pws(lineup_hash);
  # ')
  
  # 6) Refresh stats
  dbExecute(con, 'ANALYZE;')
  
  dbExecute(con, "COMMIT;")
  message("✅ lineups_lookup and helper tables rebuilt successfully.")
}, error = function(e) {
  message("❌ Error during rebuild: ", conditionMessage(e))
  try(dbExecute(con, "ROLLBACK;"), silent = TRUE)
})

lineup_data <- tbl(con, 'df_lineups')

#add stints to ppp

by <- join_by(team_id, game_id, q_bucket, between(id, final_start_id, final_end_id, bounds = "[)"))

# overwrite safely
DBI::dbExecute(con, "DROP TABLE IF EXISTS stints;")

stints %>%
  compute(name = "stints", temporary = FALSE)


stints_tbl <- tbl(con, "stints")

df_pts_poss_lineups <- left_join(poss %>%
                                   mutate(end_game_seconds_remaining = if_else(end_game_seconds_remaining == 0,  
                                                                               end_game_seconds_remaining + 0.00001,        
                                                                               end_game_seconds_remaining)), stints_tbl, by)

)
library(DBI)
library(dplyr)

## Build the lazy UNION ALL while keeping all original columns
offense <- df_pts_poss_lineups %>%
  filter(!game_id %in% c("62527","62541","62522")) %>%
  mutate(
    type_lineup = "offense",
    lineup_hash = lineup_hash_offense
  ) %>%
  select(-c(lineup_hash_defense, team_id_defense, lineup_hash_offense))

defense <- df_pts_poss_lineups %>%
  filter(!game_id %in% c("62527","62541","62522")) %>%
  mutate(
    type_lineup = "defense",
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
    ORDER BY game_id, team_id, type_lineup, lineup_hash;
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
  filter(lineup_hash == "32eaf93e199bfb1bd3db6bc30745313c") %>%
  view()
  group_by(lineup_hash) %>%
  summarise(total_poss = sum(final_end_poss, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(is.na(total_poss)) %>%
  inner_join(df_pts_poss_lineups_longer, by = c("lineup_hash")) %>%
  group_by(lineup_hash) %>%
  dbplyr::window_order(lineup_hash, id, quarter, desc(end_game_seconds_remaining), user_time) %>%
  select(type, game_id) %>%
  filter(lineup_hash == "32eaf93e199bfb1bd3db6bc30745313c") %>%
  view()
  filter(type == "shot") %>%
  view()
 %>%


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
  filter(type !=  "substitution") %>%
  filter(!game_id %in% c('62447', '62452', '62534')) %>%
  tally()
  view()


compute(df_pts_poss_lineups, "pws", temporary = FALSE, overwrite = TRUE)


df_pts_poss_lineups %>%
  filter(!game_id %in% c('62527', '62541', '62522')) %>%
  filter(team_id != 0, type != 'substitution') %>%
  filter(is.na(lineup_hash_defense)) %>%
  filter(game_id == 62534) %>%
  view()

stints_tbl %>%
  filter(game_id == "62469") %>%
  view()

poss %>%
  filter(game_id == 62469) %>%
  group_by(team_id) %>%
  summarise(final_id = max(id))
  
  summarise(n = n_distinct(game_id))
  arrange(game_id)
  view()

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
  group_by(team_id, type_lineup, lineup_hash, game_id, quarter) %>%
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
