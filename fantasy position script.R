box_player <- hoopR::load_nba_team_box(seasons = 2024)

pbp_teams <- hoopR::load_nba_pbp(seasons = c(2022:2024))

dunk_game_ids <- pbp_teams %>%
  filter(qtr == 2) %>%
  group_by(game_id) %>%
  mutate(row_num = row_number()) %>%
  mutate(end_half = max(row_num)) %>%
  ungroup() %>%
  filter(end_half == row_num) %>%
  select(game_id, away_team_id, home_team_id, home_score, away_score, away_team_abbrev, home_team_abbrev) %>%
  select(-c(away_team_id, home_team_id)) %>%
  pivot_longer(c(away_team_abbrev, home_team_abbrev)) %>%
  filter(value == "POR") %>%
  mutate(gap = case_when(name == "away_team_abbrev"~away_score-home_score,TRUE~home_score-away_score)) %>%
  filter(between(gap, -20, -15)) %>%
  select(game_id, gap)


dunk_game_ids %>%
inner_join(pbp_teams, by = c("game_id")) %>%
  filter(qtr <= 2) %>%
  filter(team_id == 22) %>%
  filter(str_detect(text, "dunk")) %>%
  view("dunk")



library(tidyverse)



getwd()


library(readxl)

proj_mon <- readxl::read_xls("BBM_Projections(1).xls")


library(tidyverse)

proj_mon %>%
  janitor::clean_names() %>%
  separate(col = "pos", into = c("pos1", "pos2", "pos3", "pos4"), sep = "\\/") %>%
  select(round, rank, value, name, pos1, pos2, pos3, pos4) %>%
  pivot_longer(-c(round, rank, value, name), values_to = "pos", names_to = "type", values_drop_na = TRUE) %>%
  mutate(cut_bins = cut(rank, breaks = seq(0, 120, 12))) %>%
  group_by(cut_bins, pos) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  filter(!is.na(cut_bins)) %>%
  group_by(cut_bins) %>%
  mutate(freq = n / 12) %>%
  ggplot(aes(x = cut_bins, y = freq, fill = pos)) + geom_bar(stat = "identity", position = position_dodge(width = 0.7)) + 
  scale_y_continuous(labels = scales::percent_format(), breaks = seq(0, 0.7, 0.1)) +
  xlab(label = "Rank Bin") +
  ylab(label = "% Of Rank Bin") +
  ggtitle("Position Distribution Across Fantasy Rank Groups",
          subtitle = "By Basketball Monster Projections")



proj_mon %>%
  janitor::clean_names() %>%
  mutate(cut_bins = cut(rank, breaks = seq(0, 96, 12))) %>%
  group_by(cut_bins) %>%
  mutate(across(.cols = c(ends_with("_v"), x3v), .names = "sum_{.col}", ~median(.x))) %>%
  select(cut_bins, starts_with("sum")) %>%
  distinct() %>% 
  filter(!is.na(cut_bins)) %>%
  select(-sum_fr_v) %>%
  rename("Points" = sum_p_v,
         "Blocks" = sum_b_v,
         "FG pct" = sum_fg_percent_v,
         "FT pct" = sum_ft_percent_v,
         "Rebounds" = sum_r_v,
         "Steals" = sum_s_v,
         "Assists" = sum_a_v,
         "3PM" = sum_x3v,
         "Turnovers" = sum_to_v) %>%
  pivot_longer(-c(cut_bins)) %>%
  ggplot(aes(x = cut_bins, y = value, fill = name)) + geom_bar(stat = "identity", position = position_dodge(width = 0.7)) +
  ggthemes::scale_fill_tableau("Tableau 10") + ggthemes::theme_fivethirtyeight() +
  ggtitle("Median player Z-score for each category in each round",
          subtitle = "By Basketball Monster Projections")


  
  

kyle_proj <- proj_mon %>% janitor::clean_names()
josh_proj <- readxl::read_xls("josh_proj.xls") %>%
  janitor::clean_names()


kyle_proj %>%
  colnames()

final_comp <- kyle_proj %>%
  inner_join(josh_proj, by = c("name", "team", "pos")) %>%
  mutate(diff_kyle = value.x - value.y) %>%
  arrange(desc(diff_kyle)) %>%
  relocate(c("value.y", "round.y", "rank.y"), .after = "value.x") %>%
  relocate(name, .before = "round.x") %>%
  relocate("diff_kyle", .after = rank.y) %>%
  select(-c(own.x, adv_percent.x, adv.x)) %>%
  select(name, team, pos, diff_kyle, 
         ends_with(".x")) %>%
  rename_with(.cols = everything(), .fn = ~str_remove(., pattern = ".x")) %>%
  view()

final_comp %>%
  mutate(type = "kyle") %>%
  bind_rows(josh_proj %>% mutate(type = "josh")) %>%
  group_by(name, pos, team) %>%
  fill(diff_kyle, .direction = "down") %>%
  filter(name %in% vec_players) %>%
  view("kyle_josh")


vec_players <- c('O.G. Anunoby',
                 'Giannis Antetokounmpo',
                 'Alperen Sengun',
                 'Nicolas Claxton',
                 'Ja Morant',
                 'Jonathan Kuminga',
                 'Caris LeVert',
                 'Vince Williams Jr.',
                 'Clint Capela',
                 'Miles McBride',
                 'R.J. Barrett',
                 'Scoot Henderson',
                 'Ben Sheppard')
)

  