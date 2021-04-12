library(dplyr)
library(rvest)
library(stringr)
library(tidyverse)
library(patchwork)
library(janitor)
library(lubridate)

url_pbp <- "https://www.basketball-reference.com/boxscores/pbp/201505020LAC.html"

webpage_pbp <- read_html(url_pbp)

col_names_pbp <- webpage_pbp %>% 
  html_nodes("#pbp > tr > th") %>% 
  html_attr("data-stat")    


games <- c(df_please$game_id)

df <- NULL
plz <- NULL

for (i in 1:length(games)) {
  
  url_pbp <- paste0("https://www.basketball-reference.com/boxscores/pbp/",games[i],".html",sep = "")

  webpage_pbp <- read_html(url_pbp)
  
data_teams  <- webpage_pbp %>% 
  html_nodes("#pbp") %>% html_nodes("tr > td") %>% 
  html_text() 

new_col <-  c("time", "visit_team", "change_visit", "score", "change_home", "home_team")
names(new_col) <- paste0("V",1:(length(new_col)))

test_test_test <- gsub(pattern = "^Start|^End","lllllllllllllll",data_teams)

plz <- as.data.frame(matrix(unlist(str_split(test_test_test,pattern = "Jump|ball:|gain|possession|End|\\dst|quarter|lllll"
                                             ,n = 5)),ncol = 6,byrow = TRUE),stringsAsFactors = FALSE)  %>%  
   rename_with(.cols = starts_with("V"), .fn = function(x){new_col[x]}) %>%
   separate(col = "score",into = c("visit_score", "home_score")) %>% 
   mutate(across(.cols = c(visit_team,home_team),.fns = ~ str_detect(.x,pattern = "[:alpha:]"),
                 .names = "mean_{.col}")) %>% 
   unite(event,c("visit_team","home_team"),remove = TRUE) %>% 
   mutate (team = ifelse(mean_visit_team == TRUE, "visit_team","home_team")) %>% 
   mutate(shot_type = str_extract(event,"[0-9]{1}-pt|free throw"), visit_adv = as.numeric(visit_score) - as.numeric(home_score),
          home_adv = -visit_adv,time = as.duration(ms(time))) %>%
   mutate(qurt = cumsum (replace_na(time == 720 & lead(time,1) != 720,0)) +
             cumsum (replace_na(time == 300 & is.na(home_adv) == TRUE & lead(time,1) != 300,0))) %>% 
   mutate(time = case_when(qurt == 1 ~ 720 - time + 720*(qurt-1),
                           qurt == 2 ~ 720 - time + 720*(qurt-1),
                           qurt == 3 ~ 720 - time + 720*(qurt-1),
                           qurt == 4 ~ 720 - time + 720*(qurt-1),
                           qurt > 4 ~ 300 - time + 720*4 + 300*(qurt-5)))

                  plz<-cbind(plz,rep(games[i],nrow(plz)))
                  df<- rbind(df, plz)
                  }


df_final <- df %>% select (time, event, visit_score, home_score, home_adv, visit_adv, team, qurt,shot_type) %>% 
   mutate(game_id = as.character(df$`rep(games[i], nrow(plz))`), across(contains("score"),as.numeric),time_minutes = round(time / 60),distance = 
             as.numeric(str_extract(event, pattern = "(?<=from)(.*)(?=ft)")), is_leading = if_else(visit_adv > 0 & team == "visit_team"|visit_adv < 0 & team == "home_team", TRUE, FALSE))


df_final_pbp_games_2021 <- left_join(df_final,df_games,by = "game_id") %>% mutate (adv = case_when(team == "visit_team"~visit_adv,
                                                                                                        team == "home_team"~home_adv)) %>% mutate(end_poss = case_when (str_detect(event,"makes [2-3]{1}-pt")~TRUE,
                                                                                        str_detect(event,"misses [2-3]{1}-pt") & lead(str_detect(event,"Defensive rebound"))~TRUE,
                                                                                        str_detect(event,"misses free throw 2 of 2") & lead(str_detect(event,"Defensive rebound"))~TRUE,
                                                                                        str_detect(event,"misses free throw 3 of 3") & lead(str_detect(event,"Defensive rebound"))~TRUE,
                                                                                        str_detect(event, "makes free throw 2 of 2")~TRUE,
                                                                                        str_detect(event, "makes free throw 3 of 3")~TRUE,
                                                                                        str_detect(event,"Turnover")~TRUE,
                                                                                        TRUE~FALSE)) %>% mutate (points = case_when(str_detect(event,"makes 2-pt")~2,
                                                                                                                                    str_detect(event,"makes 3-pt")~3,
                                                                                                                                    str_detect(event,"makes free throw")~1,
                                                                                                                                    TRUE~0)) %>% mutate (team_offense = case_when (team == "visit_team"~visitor_team_name,
                                                                                                                                                                            team == "home_team"~home_team_name),
                                                                                                                             team_defense = case_when(team == "visit_team"~home_team_name,
                                                                                                                                                                                  team == "home_team"~visitor_team_name)) %>% mutate (time_offense = as.duration(time - lag(time,1))) %>%
                                                                                                                                                                  mutate (has_made = str_extract(event, "misses|makes")) %>%  
                                                                                                                                      mutate (has_made = case_when (has_made == "makes" ~ TRUE, has_made == "misses" ~ FALSE)) %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                                                                                                                                                                                                                                                                                    TRUE~FALSE))


