scrape_nba_stats <- function (referer, url, season) {
  
  library(stringr)
  library(httr)
  
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
  
  url <- url
  df_final <- data.frame(NULL)
  
  for (i in 1:length(season)) {
    
    ses <- paste(season[i],substr(season[i]+1,3,4),sep = "-")
    
    str1 <- str_split(url, pattern = "(?<=Season=)(.*)",n = 2)[[1]][1]
    str2<-str_extract(url, pattern = "(?<=Season=)(.*)")
    url_season <- paste0(paste0(str1,ses),str2)
    
    res <- httr::GET(url_season, httr::add_headers(.headers = headers))
    json_resp <- jsonlite::fromJSON(content(res, "text"))
    df <- data.frame(json_resp$resultSets$rowSet[1])
    year <- rep(ses, nrow(df))
    df <- cbind(df, year)
    df_final <- rbind(df_final, df)
    col_names <- json_resp[["resultSets"]][["headers"]][[1]]
    
  }
  
  colnames(df_final) <- c(col_names, "year")
  return(df_final)
  
}

library(tidyverse)

matchup_data_2023 <- scrape_nba_stats("https://stats.nba.com/stats/leagueseasonmatchups",
                                      "https://stats.nba.com/stats/leagueseasonmatchups?DefPlayerID=&DefTeamID=&LeagueID=00&OffPlayerID=&OffTeamID=&PerMode=Totals&Season=&SeasonType=Regular+Season",
                                      2021:2024) %>% 
  

matchup_join_bio <- 
  
  matchup_best <- 
  
  
  p1 <- matchup_data_2023 %>%
  left_join(bio_data_2023 %>% select(year, PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE,
                                     PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES), by = c("OFF_PLAYER_ID" = "PLAYER_ID", "year")) %>%
  left_join(bio_data_2023 %>% select(year, PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE,
                                     PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES), by = c("DEF_PLAYER_ID" = "PLAYER_ID", "year")) %>%
  mutate(across(-c(year, OFF_PLAYER_NAME, DEF_PLAYER_NAME, MATCHUP_MIN, PLAYER_NAME.x, PLAYER_NAME.y,
                   PLAYER_HEIGHT.x, PLAYER_HEIGHT.y, TEAM_ABBREVIATION.x, TEAM_ABBREVIATION.y),~as.numeric(.x))) %>%
  
  p1 %>%
  filter(year == "2023-24") %>%
  mutate(height_diff = PLAYER_HEIGHT_INCHES.x - PLAYER_HEIGHT_INCHES.y) %>%
  mutate(height_bin = cut(PLAYER_HEIGHT_INCHES.x, breaks = seq(67, 92, 5))) %>%
  mutate(is_diff_up = height_diff >= 5,
         is_diff_down = height_diff <= -5) %>%
  group_by(TEAM_ID.x, TEAM_ABBREVIATION.x,
    year, height_bin, OFF_PLAYER_ID, OFF_PLAYER_NAME, is_diff_down) %>%
  summarise(across(.cols = c(6:19, 24:25),
                       .fns = sum,
                       .names = "sum_{.col}")) %>%
  select(-contains("PCT")) %>%
  filter(sum_PARTIAL_POSS >= 150) %>%
  mutate(sum_MATCHUP_FG2A = sum_MATCHUP_FGA - sum_MATCHUP_FG3A,
         sum_MATCHUP_FG2M = sum_MATCHUP_FGM - sum_MATCHUP_FG3M) %>%
  mutate(matchup_fg_pct = sum_MATCHUP_FGM / sum_MATCHUP_FGA,
         matchup_2pt_pct = sum_MATCHUP_FG2M / sum_MATCHUP_FG2A,
         matchup_3pt_pct = sum_MATCHUP_FG3M / sum_MATCHUP_FG3A,
         pts_per_poss_player = sum_PLAYER_PTS/ sum_PARTIAL_POSS,
         pts_per_poss_team = sum_TEAM_PTS / sum_PARTIAL_POSS, 
         freq = sum_PARTIAL_POSS / sum(sum_PARTIAL_POSS)) %>%
  ungroup() %>%
  filter(is_diff_down == TRUE) %>%
  arrange(desc(matchup_fg_pct)) %>%
  #filter(TEAM_ID.x == 1610612750, year == "2023-24") %>%
  arrange(desc(freq)) %>%
  slice_max(order_by = freq, n = 100) %>%
  arrange(desc(pts_per_poss_player)) %>%
  view()
  
  
  
  ggplot(aes(x = height_diff, y = matchup_3pt_pct, 
             color = height_bin)) + geom_line() + 
            scale_y_continuous(limit = c(0.2, 0.4))
  
  
tolower()  
  
  mutate(across(.cols = -c(height_diff), 
                       .fns = ~(.x / sum(.x))*100,
         .names = 'freq_{.col}')) %>%
  view()
  view()

sum(across())
  
  
  mutate(above_69_def = PLAYER_HEIGHT_INCHES.y > 81, 
         above_611_off = PLAYER_HEIGHT_INCHES.x > 82) %>%
  ##filter(OFF_PLAYER_ID == "1626157") %>%
  #filter(above_611_off == TRUE) %>%
  group_by(OFF_PLAYER_ID, OFF_PLAYER_NAME, best_defender) %>%
  summarise(total_poss = sum(PARTIAL_POSS), total_pts = sum(PLAYER_PTS), 
            total_team_pts = sum(TEAM_PTS), total_fga = sum(MATCHUP_FGA), total_fgm = sum(MATCHUP_FGM),
            total_fg3a = sum(MATCHUP_FG3A), total_fg3m = sum(MATCHUP_FG3M),
            total_ast = sum(MATCHUP_AST), total_tov = sum(MATCHUP_TOV),
            total_fta = sum(MATCHUP_FTA),
            ts_shot_att = 2*(total_fga + 0.44*total_fta)) %>%
  ungroup() %>%
  mutate(pts_per_poss = total_pts / total_poss, fga_per_poss = total_fga / total_poss,
         team_pts_poss = total_team_pts / total_poss, fg3a_per_poss = total_fg3a/total_poss,
         fg3m_per_poss = total_fg3m/total_poss,
         total_fg3a, total_fg3m, pct = total_fgm / total_fga,
         ast_pct = total_ast / total_poss, tov_pct = total_tov / total_poss,
         fta_pct = total_fta / total_poss,
         ts_pct = total_pts / ts_shot_att,
         ts_att_per_poss = ts_shot_att / total_poss) %>%
  group_by(OFF_PLAYER_ID, OFF_PLAYER_NAME) %>%
  mutate(pts_false = lag(pts_per_poss, order_by = best_defender),
         fga_false = lag(fga_per_poss, order_by = best_defender),
         teampts_false = lag(team_pts_poss, order_by = best_defender),
         pct_false = lag(pct, order_by = best_defender),
         ast_false = lag(ast_pct, order_by = best_defender),
         tov_false = lag(tov_pct, order_by = best_defender),
         fta_false = lag(fta_pct, order_by = best_defender),
         ts_false = lag(ts_pct, order_by = best_defender),
         ts_att_per_poss_false = lag(ts_att_per_poss, order_by = best_defender),
         pts_prev = pts_per_poss - pts_false,
         fga_prev = fga_per_poss - fga_false,
         teampts_prev = team_pts_poss - teampts_false,
         pct_prev = pct - pct_false,
         ast_prev = ast_pct - ast_false,
         tov_prev = tov_pct - tov_false,
         fta_prev = fta_pct - fta_false,
         ts_prev = ts_pct - ts_false,
         ts_att_prev = ts_att_per_poss - ts_att_per_poss_false) %>%
  arrange(desc(pts_prev)) %>% filter(best_defender == TRUE, total_poss > 1500) %>% 
  #filter(str_detect(OFF_PLAYER_NAME, "Jokic|Embiid")) %>%
  ggplot() + geom_link(aes(x = pts_false, xend = pts_per_poss, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME))


p2 <- matchup_data_2023 %>%
  left_join(bio_data_2023 %>% select(PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE,
                                     PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES), by = c("OFF_PLAYER_ID" = "PLAYER_ID")) %>%
  left_join(bio_data_2023 %>% select(PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE,
                                     PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES, best_defender), by = c("DEF_PLAYER_ID" = "PLAYER_ID")) %>%
  mutate(across(-c(year, OFF_PLAYER_NAME, DEF_PLAYER_NAME, MATCHUP_MIN, PLAYER_NAME.x, PLAYER_NAME.y,
                   PLAYER_HEIGHT.x, PLAYER_HEIGHT.y, TEAM_ABBREVIATION.x, TEAM_ABBREVIATION.y, best_defender),~as.numeric(.x))) %>%
  mutate(above_69_def = PLAYER_HEIGHT_INCHES.y > 81, 
         above_611_off = PLAYER_HEIGHT_INCHES.x > 82) %>%
  ##filter(OFF_PLAYER_ID == "1626157") %>%
  #filter(above_611_off == TRUE) %>%
  group_by(OFF_PLAYER_ID, OFF_PLAYER_NAME, best_defender) %>%
  summarise(total_poss = sum(PARTIAL_POSS), total_pts = sum(PLAYER_PTS), 
            total_team_pts = sum(TEAM_PTS), total_fga = sum(MATCHUP_FGA), total_fgm = sum(MATCHUP_FGM),
            total_fg3a = sum(MATCHUP_FG3A), total_fg3m = sum(MATCHUP_FG3M),
            total_ast = sum(MATCHUP_AST), total_tov = sum(MATCHUP_TOV),
            total_fta = sum(MATCHUP_FTA),
            ts_shot_att = 2*(total_fga + 0.44*total_fta)) %>%
  ungroup() %>%
  mutate(pts_per_poss = total_pts / total_poss, fga_per_poss = total_fga / total_poss,
         team_pts_poss = total_team_pts / total_poss, fg3a_per_poss = total_fg3a/total_poss,
         fg3m_per_poss = total_fg3m/total_poss,
         total_fg3a, total_fg3m, pct = total_fgm / total_fga,
         ast_pct = total_ast / total_poss, tov_pct = total_tov / total_poss,
         fta_pct = total_fta / total_poss,
         ts_pct = total_pts / ts_shot_att,
         ts_att_per_poss = ts_shot_att / total_poss) %>%
  group_by(OFF_PLAYER_ID, OFF_PLAYER_NAME) %>%
  mutate(pts_false = lag(pts_per_poss, order_by = best_defender),
         fga_false = lag(fga_per_poss, order_by = best_defender),
         teampts_false = lag(team_pts_poss, order_by = best_defender),
         pct_false = lag(pct, order_by = best_defender),
         ast_false = lag(ast_pct, order_by = best_defender),
         tov_false = lag(tov_pct, order_by = best_defender),
         fta_false = lag(fta_pct, order_by = best_defender),
         ts_false = lag(ts_pct, order_by = best_defender),
         ts_att_per_poss_false = lag(ts_att_per_poss, order_by = best_defender),
         pts_prev = pts_per_poss - pts_false,
         fga_prev = fga_per_poss - fga_false,
         teampts_prev = team_pts_poss - teampts_false,
         pct_prev = pct - pct_false,
         ast_prev = ast_pct - ast_false,
         tov_prev = tov_pct - tov_false,
         fta_prev = fta_pct - fta_false,
         ts_prev = ts_pct - ts_false,
         ts_att_prev = ts_att_per_poss - ts_att_per_poss_false) %>%
  arrange(desc(pts_prev)) %>% filter(best_defender == TRUE, total_poss > 1500) %>% 
  #filter(str_detect(OFF_PLAYER_NAME, "Jokic|Embiid")) %>%
  ggplot() + geom_link(aes(x = ts_false, xend = ts_pct, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME))


df_comet <- matchup_data_2023 %>%
  left_join(bio_data_2023 %>% select(PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE.x,
                                     PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES), by = c("OFF_PLAYER_ID" = "PLAYER_ID")) %>%
  left_join(bio_data_2023 %>% select(PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE.x,
                                     PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES, best_defender), by = c("DEF_PLAYER_ID" = "PLAYER_ID")) %>%
  mutate(across(-c(year, OFF_PLAYER_NAME, DEF_PLAYER_NAME, MATCHUP_MIN, PLAYER_NAME.x, PLAYER_NAME.y,
                   PLAYER_HEIGHT.x, PLAYER_HEIGHT.y, TEAM_ABBREVIATION.x, TEAM_ABBREVIATION.y, best_defender),~as.numeric(.x))) %>%
  mutate(above_69_def = PLAYER_HEIGHT_INCHES.y > 81, 
         above_611_off = PLAYER_HEIGHT_INCHES.x > 82) %>%
  ##filter(OFF_PLAYER_ID == "1626157") %>%
  #filter(above_611_off == TRUE) %>%
  group_by(OFF_PLAYER_ID, OFF_PLAYER_NAME, best_defender) %>%
  summarise(total_poss = sum(PARTIAL_POSS), total_pts = sum(PLAYER_PTS), 
            total_team_pts = sum(TEAM_PTS), total_fga = sum(MATCHUP_FGA), total_fgm = sum(MATCHUP_FGM),
            total_fg3a = sum(MATCHUP_FG3A), total_fg3m = sum(MATCHUP_FG3M),
            total_ast = sum(MATCHUP_AST), total_tov = sum(MATCHUP_TOV),
            total_fta = sum(MATCHUP_FTA),
            ts_shot_att = 2*(total_fga + 0.44*total_fta)) %>%
  ungroup() %>%
  mutate(pts_per_poss = total_pts / total_poss, fga_per_poss = total_fga / total_poss,
         team_pts_poss = total_team_pts / total_poss, fg3a_per_poss = total_fg3a/total_poss,
         fg3m_per_poss = total_fg3m/total_poss,
         total_fg3a, total_fg3m, pct = total_fgm / total_fga,
         ast_pct = total_ast / total_poss, tov_pct = total_tov / total_poss,
         fta_pct = total_fta / total_poss,
         ts_pct = total_pts / ts_shot_att,
         ts_att_per_poss = ts_shot_att / total_poss) %>%
  group_by(OFF_PLAYER_ID, OFF_PLAYER_NAME) %>%
  mutate(pts_false = lag(pts_per_poss, order_by = best_defender),
         fga_false = lag(fga_per_poss, order_by = best_defender),
         teampts_false = lag(team_pts_poss, order_by = best_defender),
         pct_false = lag(pct, order_by = best_defender),
         ast_false = lag(ast_pct, order_by = best_defender),
         tov_false = lag(tov_pct, order_by = best_defender),
         fta_false = lag(fta_pct, order_by = best_defender),
         ts_false = lag(ts_pct, order_by = best_defender),
         ts_att_per_poss_false = lag(ts_att_per_poss, order_by = best_defender),
         pts_prev = pts_per_poss - pts_false,
         fga_prev = fga_per_poss - fga_false,
         teampts_prev = team_pts_poss - teampts_false,
         pct_prev = pct - pct_false,
         ast_prev = ast_pct - ast_false,
         tov_prev = tov_pct - tov_false,
         fta_prev = fta_pct - fta_false,
         ts_prev = ts_pct - ts_false,
         ts_att_prev = ts_att_per_poss - ts_att_per_poss_false) %>%
  filter(str_detect(OFF_PLAYER_NAME, "Draymond")) %>%
  view()
arrange(desc(pts_prev)) %>% filter(best_defender == TRUE, total_poss > 1500) %>% 
  #filter(str_detect(OFF_PLAYER_NAME, "Jokic|Embiid")) %>%
  ggplot() + geom_link(aes(x = ts_att_per_poss_false, xend = ts_att_per_poss, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME)) + theme_fivethirtyeight() + guides(size = "none")

p1 <- df_comet  %>% ggplot() + geom_link(aes(x = pts_false, xend = pts_per_poss, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME)) + 
  theme_fivethirtyeight() + guides(size = "none") + ggtitle("PTS Change Per Possesion") 

df_comet %>%
  f

ggsave("p1.png", p1)

ggsave(p1,filename="p1.png",width=15.6,height=8.05,limitsize = FALSE) 

p2 <- df_comet %>% ggplot() + geom_link(aes(x = ts_false, xend = ts_pct, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME)) +
  theme_fivethirtyeight() + guides(size = "none")

p3 <- df_comet %>% ggplot() + geom_link(aes(x = ts_att_per_poss_false, xend = ts_att_per_poss, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME)) + 
  theme_fivethirtyeight() + guides(size = "none")

p4 <- df_comet %>% ggplot() + geom_link(aes(x = teampts_false, xend = team_pts_poss, size = stat(index), y = fct_reorder(OFF_PLAYER_NAME, fga_per_poss), yend = OFF_PLAYER_NAME)) + 
  theme_fivethirtyeight() + guides(size = "none")

df_comet %>%
  ggplot(aes(x = pts_prev, y = fct_reorder(OFF_PLAYER_NAME,fga_per_poss))) + geom_bar(stat = "identity")

p1+p2

p3

df_comet$team_pts_poss

df_comet$team
p1+p2


install.packages("patchwork")

library(patchwork)

library(ggthemes)

p1+p2

install.packages("ggforce")

library(ggforce)

matchup_best %>%
  #filter(OFF_PLAYER_NAME %in% c("Rudy Gobert", "Nikola Jokic", "Joel Embiid", "Karl-Anthony Towns")) %>%
  select(total_pts, total_poss, pts_per_poss, fta_pct, ast_pct, tov_pct, fga_per_poss,
         team_pts_poss, pct, best_defender, pts_prev, fga_prev, teampts_prev, pct_prev, ast_prev, tov_prev, fta_prev) %>%
  arrange(desc(pts_prev)) %>%
  filter(total_poss > 1000) %>%

best_def <- readxl::read_xlsx("best_defender_table.xlsx")


library(rvest)

read_html("https://craftednba.com/player-traits/defense") %>%
  html_elements(xpath = "//td[@class='table-wrapping is-responsive']") %>%
  html_attr("href")


view(matchup_join_bio)

matchup_join_bio %>% rename_with(matchup_join_bio, .fn = ~str_replace(string = matchup_join_bio, pattern = ".x", replacement = "_OFF"),.cols = ends_with(".x")) %>%
  colnames()

bio_data_2023 %>%
  filter(PLAYER_HEIGHT_INCHES > 81) %>%
  distinct(PLAYER_HEIGHT, PLAYER_HEIGHT_INCHES) %>%
  
  
  gsub()  

bio_data_2023 <- scrape_nba_stats("https://stats.nba.com/stats/leaguedashplayerbiostats",
                                  "https://stats.nba.com/stats/leaguedashplayerbiostats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=&LeagueID=00&Location=&Month=&OpponentTeamID=&Outcome=&PORound=&PerMode=Totals&Period=&PlayerExperience=&PlayerPosition=&Season=&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=&VsConference=&VsDivision=&Weight=",
                                  2021:2024) %>%
  
  bio_data_2023 <- left_join(bio_data_2023, best_def, by = c("PLAYER_NAME" = "Name")) %>%
  mutate(best_defender = !is.na(DLEBRON))





colnames(bio_data_2023)

colnames(matchup_data_2023)

matchup_data_2023$
  
  