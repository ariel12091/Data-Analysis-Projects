library(dplyr)
library(rvest)
library(stringr)
library(tidyverse)
library(patchwork)
library(janitor)
library(lubridate)

scrape_game_id <- function (start_date, finish_date) {

url_games <- "https://www.basketball-reference.com/leagues/NBA_2020_games.html"

webpage_games <- read_html(url_games)

col_names_games <- webpage_games %>% 
  html_nodes("table > thead > tr > th") %>% 
  html_attr("data-stat")    

teams <- NULL
data_games <- NULL
total_teams<-NULL
total_games <- NULL
df_game <- NULL
df_final_game <- NULL

colmapping <- col_names_games[-1]
names(colmapping) <- paste0("V",1:9)

for (j in start_date:finish_date) {
  
  
  months <- c("october", "november", "december", "january", "february", "march", "april", "may", "june", "august", "september")
  
  for (i in 1:length(months)) {
    
    url_games <- paste0("https://www.basketball-reference.com/leagues/NBA_",j,"_games-",months[i],".html",sep ="")
    
    possible_read = possibly (.f = read_html,otherwise = "Error")
    
    webpage_games <- possible_read(url_games)
    
    if (webpage_games == "Error") {
      next
    }
    
    else {
      
      data_games  <- webpage_games %>% 
        html_nodes("table > tbody  > tr > th") %>% 
        html_attr("csk")
      
      teams <- webpage_games %>% 
        html_nodes("table > tbody  > tr > td") %>% 
        html_text()
      
      total_games <- c(total_games,data_games)
      total_teams <- c(total_teams,teams)
    }
    
    mat_teams <- as.data.frame(matrix(total_teams,ncol = 9,byrow = TRUE),stringsAsFactors = FALSE)
    
    df_game <- mat_teams %>% mutate (game_id = na.omit(total_games)) %>% rename_with(.cols = starts_with("V"), .fn = function(x){colmapping[x]})
    
  }
  df_game <- cbind(df_game)
      
    }
    
df_final_game <- rbind(df_final_game,df_game)

}


df_please <- scrape_game_id(2005,2021)
