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
