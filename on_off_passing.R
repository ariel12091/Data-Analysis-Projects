pass_loss_data <- scrape_nba_stats(referer = "https://stats.nba.com/stats/leaguedashptstats",
  url = "https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=L&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Team&PlayerPosition=&PtMeasureType=Passing&Season=&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=",
  season = c(2020:2023))
)


pass_loss_data <- scrape_nba_stats(referer = "https://stats.nba.com/stats/leaguedashptstats",
                                   url = url_pass_links[65],
                                   season = c(2023))
)

pass_win_data <- scrape_nba_stats(referer = "https://stats.nba.com/stats/leaguedashptstats",
                                   url = glue::glue("https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={vec_date[9]}&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Team&PlayerPosition=&PtMeasureType=Passing&Season=&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight="),
                                   season = c(2023))
)

url_pass_links <- glue::glue("https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom={vec_date}&DateTo={vec_date}&Division=&DraftPick=&DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Team&PlayerPosition=&PtMeasureType=Passing&Season=2023-24&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=")


url_pass_links_player <- replace(url_pass_links, "PlayerOrTeam=Team", "PlayerOrTeam=Player")

url_pass_links_player[1]


url_pass_links_player <- str_replace(url_pass_links, "PlayerOrTeam=Team", "PlayerOrTeam=Player")


a <- c(1:3)


length(url_pass_links[9:12])

length(url_pass_links[i:(i+3)])


1:3



length(url_pass_links)


list_dates[[7]]$url

list_dates_player <- NULL

for (i in seq(1, length(url_pass_links_player), 4)) {
  all_dates_passing <- crul::Async$new(urls = url_pass_links_player[i:(i+3)],
                                       headers = headers)$get()
  
  list_dates_player <- c(list_dates_player, all_dates_passing)
  
  Sys.sleep(time = 20)
}

c(NULL, list(6,5,6))

all_dates_passing <- crul::Async$new(urls = url_pass_links[65], headers = headers)$get()

HttpRequest$new(url = "https://httpbin.org/get")$get()

list_req <- list(crul::HttpRequest$new(url = url_pass_links[65])$get())

all_df_final_player <- NULL

for (i in 1:length(list_dates_player)) {
  json_resp <- jsonlite::fromJSON(read_html(list_dates_player[[i]]$content) %>%
    html_elements(xpath = "//p") %>%
    html_text())
    
col_names <- json_resp$resultSets$headers[[1]]  
df <- as.data.frame(json_resp$resultSets$rowSet[[1]])

if (nrow(df) > 0) {

  colnames(df) <- col_names
  url <- list_dates_player[[i]]$url

  df_final <- df %>%
    mutate(url = url)
}
  else {
    next
  }

all_df_final_player <- bind_rows(all_df_final_player, df_final)
  
}

library(tidyverse)
library(lubridate)

cleaned_team_passing_df <- all_df_final %>%
  janitor::clean_names() %>%
  mutate(game_date = ymd(str_extract(url, "\\d{4}-\\d{2}-\\d{2}"))) %>%
  mutate(across(-c(team_id, team_abbreviation, team_name,url, game_date), ~as.numeric(.x))) %>%
  mutate(game_date_test = ymd(game_date)) %>%
  select(-url) %>%
  group_by(team_id, team_abbreviation, team_name) %>%
  arrange(game_date) %>%
  mutate(num_wins = cumsum(w)) %>%
  ungroup() %>%
  arrange(passes_made) %>%
  view()
  filter(str_detect(team_name, "Chi")) %>%
  filter(passes_made > 100) %>%
  ggplot() +
  geom_line(aes(x = game_date, y = passes_made, col = team_name, group = team_name)) +
    geom_line(aes(x = game_date, y = num_wins, col = team_name, group = team_name))
  

  
  
  
  cleaned_player_passing_df <- all_df_final_player %>%
    janitor::clean_names() %>%
    mutate(game_date = ymd(str_extract(url, "\\d{4}-\\d{2}-\\d{2}"))) %>%
    mutate(across(-c(player_name, team_abbreviation, team_id, player_id,
                     ,url, game_date), ~as.numeric(.x))) %>%
    mutate(game_date_test = ymd(game_date)) %>%
    select(-url) %>%
    group_by(player_id, player_name, team_id, team_abbreviation) %>%
    arrange(game_date) %>%
    mutate(num_wins = cumsum(w)) %>%
    ungroup()  
  
  
  cleaned_team_passing_df %>%
    left_join(cleaned_player_passing_df, 
              by = c("team_id", "game_date", "team_abbreviation")) %>%
    filter(team_id == 1610612737) %>%
    view()
    slice_head(n = 300) %>%
    view()
    

db_players_played <- cleaned_player_passing_df %>%
  distinct(team_id, team_abbreviation, player_id, player_name, game_date, passes_made) %>%
  filter(str_detect(player_name, "Lowry")) %>%
  view()
  
  pivot_wider(names_from = game_date, values_from = passes_made) %>%
  pivot_longer(-c(team_id, team_abbreviation, player_id, player_name),
               values_drop_na = FALSE) %>%
  rename(game_date = name) %>%
  mutate(game_date = ymd(game_date))
  
  
players_passing <- db_players_played %>%
  group_by(player_id, player_name, team_id, team_abbreviation) %>%
  summarise(ind_passes_game = mean(value, na.rm = TRUE)) %>%
  ungroup()

db_players_played %>%
  view()

library(tidyverse)

lm_data <- lm_data %>%
  mutate(team_id_factor = as.factor(team_id)) %>%
  summary()

fit <- lm(off_rating~passes_per_100_poss*team_id_factor, lm_data)

filter(lm_data, passes_per_100_poss < 300)

summary(fit)

bucks_data <- lm_data %>%
  filter(team_id == 1610612740) %>%
  ggplot(aes(x = passes_per_100_poss, y = off_rating)) +
  geom_point()

summary(fit)
fit <- lm(off_rating~team_name*passes_per_100_poss, lm_data)

lm_data %>%
  filter(str_detect(team_name, "Peli")) %>%
  group_by(passes_per_100_poss > 280) %>%
  summarise(mean(off_rating), n = n_distinct(game_id))

  
  
lm_data %>%
  mutate(team_id_factor = as.factor(as.character(team_id))) %>%
  levels()


str(lm_data)

cor.test(lm_data$off_rating, lm_data$passes_per_100_poss)

cor(lm_data$passes_per_100_poss, lm_data$off_rating)

passing_on_off_chart_data %>%
  colnames()

passing_on_off_chart_data <- cleaned_team_passing_df %>%
  filter(passes_made > 200) %>%
  inner_join(clean_game_log, by = c("team_id", "team_name", "game_date", "team_abbreviation")) %>%
  inner_join(db_players_played, by = c("team_id", "team_abbreviation", "game_date")) %>%
  mutate(pace_per_min = (pace_per40 / 40)*min.y) %>%
  select(game_id, team_id, team_name, def_rating, matchup, 
         pace, e_pace, pace_per_min,
         off_rating, passes_made, min.y, min.x,
         game_date, player_id, player_name, value) %>%
  mutate(passes_per_100_poss = (passes_made / pace_per_min)*100) %>%
  filter(value <= 3) %>%
  group_by(player_name, player_id) %>%
  summarise(n = n_distinct(game_id)) %>%
  arrange(desc(n)) %>%
  view()
  filter(str_detect(team_name, "Peli")) %>%
  arrange(desc(passes_made)) %>%
  view()
  group_by(team_id, team_name, player_id, player_name, has_played = !is.na(value)) %>%
  summarise(mean_passes = mean(passes_per_100_poss, na.rm = TRUE), games_played = n()) %>%
  ungroup() %>%
  filter(str_detect(player_name, "Royce O'Neale|Schroder|Siakam|Kyle Lowry|Buddy Hield|Dinwiddie", negate = TRUE)) %>%
  pivot_wider(names_from = has_played,
              values_from = c(mean_passes, games_played)) %>%
  janitor::clean_names() %>%
  mutate(on_off_passes = mean_passes_true - mean_passes_false) %>%
  arrange(on_off_passes) %>%
  inner_join(players_passing, by = c("team_id", "player_id", "player_name")) %>%
  filter(ind_passes_game >= 35, games_played_false >= 5, games_played_true > 20) %>%
  mutate(rank_passing = dense_rank(desc(on_off_passes))) %>%
  filter(rank_passing %in% c(1:15)) %>%
  ggplot() + geom_segment(aes(x = mean_passes_true,
                          xend = mean_passes_false,
                          y = fct_reorder(player_name, on_off_passes),
                          yend = fct_reorder(player_name, on_off_passes))) +
  geom_point(aes(y = player_name, x = mean_passes_false), col = "red") +
  geom_point(aes(y = player_name, x = mean_passes_true), col = "green") +
  ggrepel::geom_label_repel(aes(x = mean_passes_true, 
                                fct_reorder(player_name, on_off_passes),
    label = paste("+", round(on_off_passes, 1), sep = ""))) +
  ggthemes::theme_fivethirtyeight() +
  labs(x = NULL,
       y = NULL,
       title = "<span style = 'color: green;'>On</span>/<span style = 'color: red;'>Off</span> 
       team passes per game", 
       subtitle = "Based on comparison between games played and missed\nmin 5 games missed and 35 passes per game") +
  theme(plot.title = element_markdown()) 
  
install.packages("ggtext")
library(ggtext)
scale_color_manual()
  
passing_on_off_chart_data %>%
  filter(str_detect(team_name, "Mia")) %>%
  view()

  passing_on_off_chart_data %>%
    filter(str_detect(team_name, "Peli")) %>%
    view()
    filter(ind_passes_game >= 35, games_played_false >= 5, games_played_true > 20) %>%
    mutate(rank_passing = dense_rank(desc(on_off_passes))) %>%
    filter(rank_passing %in% c((nrow(.)-15):nrow(.))) %>%
    ggplot() + geom_segment(aes(x = mean_passes_true,
                                xend = mean_passes_false,
                                y = fct_reorder(player_name, desc(on_off_passes)),
                                yend = fct_reorder(player_name, desc(on_off_passes)))) +
    geom_point(aes(y = player_name, x = mean_passes_false), col = "red") +
    geom_point(aes(y = player_name, x = mean_passes_true), col = "green") +
    ggrepel::geom_label_repel(aes(x = mean_passes_true, 
                                  fct_reorder(player_name, on_off_passes),
                                  label = paste(round(on_off_passes, 1), sep = "")), nudge_x = 5) +
    ggthemes::theme_fivethirtyeight() +
    labs(x = NULL,
         y = NULL,
         title = "<span style = 'color: green;'>On</span>/<span style = 'color: red;'>Off</span> 
         team passes per 100 possesions.", 
         subtitle = "Top 15 Players in team passes difference on Games <span style = 'color: green;'>Played</span> and <span style = 'color: red;'>Missed</span> <br>Min. 5 games missed and 35 passes per game") +
    theme(plot.title = element_markdown(),
          plot.subtitle = element_markdown(),
          axis.title.x = element_text()) +
    xlab("Team Passes Per 100 Poss.")





passing_on_off_chart_data %>%
  filter(ind_passes_game >= 35, games_played_false >= 5, games_played_true > 20) %>%
  mutate(rank_passing = dense_rank(desc(on_off_passes))) %>%
  filter(rank_passing %in% c(1:15)) %>%
  ggplot() + geom_segment(aes(x = mean_passes_true,
                              xend = mean_passes_false,
                              y = fct_reorder(player_name, on_off_passes),
                              yend = fct_reorder(player_name, on_off_passes))) +
  geom_point(aes(y = player_name, x = mean_passes_false), col = "red") +
  geom_point(aes(y = player_name, x = mean_passes_true), col = "green") +
  ggrepel::geom_label_repel(aes(x = mean_passes_true, 
                                fct_reorder(player_name, on_off_passes),
                                label = paste("+",round(on_off_passes, 1), sep = "")), nudge_x = +5) +
  ggthemes::theme_fivethirtyeight() +
  labs(x = NULL,
       y = NULL,
       title = "<span style = 'color: green;'>On</span>/<span style = 'color: red;'>Off</span> 
         team passes per 100 possesions.", 
       subtitle = "Top 15 Players in team passes difference on Games <span style = 'color: green;'>Played</span> and <span style = 'color: red;'>Missed</span> <br>Min. 5 games missed and 35 passes per game") +
  theme(plot.title = element_markdown(),
        plot.subtitle = element_markdown(),
        axis.title.x = element_text()) +
  xlab("Team Passes Per 100 Poss.")

  


  ggplot(aes(y = fct_reorder(player_name, on_off_passes), 
             x = on_off_passes)) + geom_bar(stat = "identity") +
  ggthemes::theme_fivethirtyeight()
  #filter(str_detect(player_name, "Embi")) %>%
  view("passes_on_off")

  
save.image(file = "on_off_passing.RData")

length(read_html(all_dates_passing[[1]]$content)

  jsonlite::fromJSON(read_html(content(url_check[[1]], "text"))
                   
jsonlite::fromJSON(read_html(res$content), "text")


first_json <- jsonlite::fromJSON(read_html(url_check[[1]]$content) %>%
  html_elements(xpath = "//p") %>%
  html_text())

first_json$resultSets


as.data.frame(as.character(read_html(res$content))) %>%
  view()
  
url_check[[1]]


res <- httr::GET(url_pass_links_player[5], httr::add_headers(.headers = headers))
json_resp <- jsonlite::fromJSON(content(res, "text"))
df <- data.frame(json_resp$resultSets$rowSet[1])
year <- rep(ses, nrow(df))
df <- cbind(df, year)
df_final <- rbind(df_final, df)
col_names <- json_resp[["resultSets"]][["headers"]][[1]]

df

library(rvest)


headers = c(
  `Connection` = 'keep-alive',
  `Accept` = 'application/json, text/plain, */*',
  `x-nba-stats-token` = 'true',
  `X-NewRelic-ID` = 'VQECWF5UChAHUlNTBwgBVw==',
  `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
  `x-nba-stats-origin` = 'stats',
  `Sec-Fetch-Site` = 'same-origin',
  `Sec-Fetch-Mode` = 'cors',
  `Referer` = referer,
  `Accept-Encoding` = 'gzip, deflate, br',
  `Accept-Language` = 'en-US,en;q=0.9'
)


inner_join(pass_loss_data,pass_win_data,
           by = c("TEAM_ID", "TEAM_NAME", "year")) %>%
  janitor::clean_names() %>%
  select(team_id, team_name,w_x, w_y,
         passes_made_x, passes_made_y,
         potential_ast_x, potential_ast_y, 
         year) %>%
  filter(year == "2023-24") %>%
  mutate(across(.cols = -c(team_id, team_name, year), ~as.numeric(.x))) %>%
  mutate(pass_change = passes_made_x - passes_made_y,
         pot_ast_change = potential_ast_x - potential_ast_y,
         pot_pass_win = potential_ast_y / passes_made_y,
         pot_pass_loss = potential_ast_x / passes_made_x) %>%
  mutate(rank_pass_win = dense_rank(desc(passes_made_y)),
         rank_pass_loss = dense_rank(desc(passes_made_x)), 
         pat_pass_ratio_change = round(pot_pass_loss - pot_pass_win,3)*100) %>%
  arrange(pass_change) %>%
  view()
  ggplot(aes(x = ))
  view()
  
  library(lubridate)
  
lubridate::interval(start = mdy("10/25/2023"),end = mdy("03/20/2024"))/days(1)

seq(as.date("10/25/2023"), as.date("03/20/2023"), )
  

as.Date(as.Date("10/25/2023"):as.Date("03/20/2024"),origin="01-01-1970")

vec_date <- seq(mdy('10/25/2023'),mdy("03/20/2024"),by = "days")
 
vec_date[1]

glue::glue("hello", mdy(as.character(vec_date[1])))


glue::glue()  
  