scrape_draft <- function (year_start, year_finish) {
  
library(rvest)
library(tidyverse)

link_col <- "https://www.basketball-reference.com/draft/NBA_2020.html"

AA <- link_col %>% read_html() %>%
  html_nodes("table > tbody > tr > td") %>% 
  html_attr("data-stat")

col_names <- AA[1:21]  

start_draft <- NULL
draft_table <- NULL

for (year in year_start:year_finish) {
  
link <- paste0("https://www.basketball-reference.com/draft/NBA_",year,".html")
  
real_names <- link %>% read_html() %>%
  html_nodes("table > tbody > tr > td > a") %>% html_attr("href")
  
real_names_final <- real_names[which(str_detect(real_names, pattern = "players") == TRUE)]
  
BB <- link %>% read_html() %>%
  html_table()

colnames(BB[[1]]) <- c("pick_round", col_names)


start_draft <- BB[[1]] %>%
  filter(!player %in% c("Player", "Round 2") & str_detect(seasons, pattern = "Minnesota", negate = TRUE)) %>%
  cbind(., real_names_final, year)
  
draft_table <- rbind(draft_table, start_draft)

}

return(draft_table)

}


draft_history <- scrape_draft(1990,2020)


draft_history %>%
  separate(real_names_final,into = c("DUMMY1", "DUMMY2", "DUMMY3", "real_name"))
