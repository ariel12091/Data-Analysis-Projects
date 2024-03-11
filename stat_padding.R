library(rvest)
library(dplyr)
library(tidyverse)
library("hoopR")

future::plan("multisession")
pbp1 <- hoopR::load_nba_pbp(seasons = c(2024))



list_gamed_ids <- pbp %>%
  distinct(game_id) %>%
  pull()

all_box_score <- load_nba_player_box(seasons = c(2024))

  trip_dub_pbp <- all_box_score %>%
  filter(as.numeric(points) >= 10 & as.numeric(rebounds) == 10 &
           as.numeric(assists) >=10) %>%
  inner_join(pbp1, by = c("athlete_id" = "athlete_id_1", "game_id")) %>%
  filter(type_id %in% c('155', '156')) %>%
  group_by(athlete_id, game_id) %>%
  mutate(num_reb = row_number()) %>%
  relocate(c(num_reb, end_game_seconds_remaining, qtr, time, clock_minutes, clock_seconds, game_id), .after = athlete_display_name) %>%
  ungroup() %>%
  filter(num_reb == 10) %>%
  mutate(under_minute = end_game_seconds_remaining < 50) %>%
  #  filter(under_minute == TRUE & athlete_id == 3907387) %>%
  #  relocate(home_team_name, .after = home_score) %>%
  #  view()
  group_by(under_minute, athlete_display_name, athlete_id) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  ungroup() %>%
  group_by(athlete_display_name, athlete_id) %>%
  mutate(freq = n / sum(n)) %>%
  ungroup() %>%
  filter(under_minute == TRUE) %>%
  inner_join(all_box_score %>%
               filter(as.numeric(points) >= 10 & as.numeric(rebounds) >=10 &
                        as.numeric(assists) >= 10) %>%
               group_by(athlete_id, athlete_display_name) %>%
               summarise(n_total_trips = n_distinct(game_id)) %>%
               ungroup() %>%
               arrange(desc(n_total_trips)), by = c("athlete_id", "athlete_display_name")) %>%
  mutate(freq_under_minute = n / n_total_trips) %>%
  view()

group_by(qtr, clock_minutes, clock_seconds, athlete_display_name) %>%
  summarise(n = n()) %>%
  view()
