library(rvest)
library(tidyverse)

list_links <- read_html("https://www.basketball-reference.com/teams/") %>%
  html_elements(xpath = "//table[@id='teams_active']//tr[@class='full_table']//a") %>%
  html_attr("href")

library(glue)

df_all_coaches %>%
  mutate(team = str_remove(team, pattern = "/"))

links_full <- glue('https://www.basketball-reference.com{list_links}')

coaches_scrape <- crul::Async$new(links_full)

coach_scrape <- coaches_scrape$get()


table_status <- NULL
for (i in 1:length(coach_scrape)) {
  vec_status <- c(coach_scrape[[i]]$status_code, coach_scrape[[i]]$url) 
  table_status <- rbind(vec_status, table_status)
}

df_all_coaches <- NULL

for (i in 1:length(coach_scrape)) {

col_names <- read_html(coach_scrape[[i]]$content) %>%
  html_elements(xpath = "//thead//tr//@data-stat") %>%
  html_text()

df_coaches_real_name <- as.data.frame(read_html(coach_scrape[[i]]$content) %>%
  html_elements(xpath = "//tbody//tr//td[@data-stat='coaches']//a") %>%
  html_attr("href"))

colnames(df_coaches_real_name) <- "real_name"

team_names_df <- read_html(coach_scrape[[i]]$content) %>%
  html_elements(xpath = "//td[@data-stat='team_name']//a") %>%
  html_attr("href")

df_coaches_name <- as.data.frame(str_split_fixed(read_html(coach_scrape[[i]]$content) %>%
  html_elements(xpath = "//tbody//tr//td[@data-stat='coaches']") %>%
  html_attr("csk"), pattern = "(?<=[0-9])\\s",n = Inf)) %>%
  mutate(season = str_extract(V1, "[0-9]{4,}")) %>%
  mutate(team_name = team_names_df) %>%
  pivot_longer(cols = -c(season, team_name)) %>%
  filter(str_length(value) != 0) %>%
  select(season, team_name, value)
  
combined_df_coach <- cbind(df_coaches_name, df_coaches_real_name)

colnames(combined_df_coach) <- c("season", "team_name", "name", "real_name")

final_combined_df_coach <- combined_df_coach %>%
  mutate(name = str_remove(name, "[0-9]{4,}"))
  #mutate(team = str_remove(team, "https://www.basketball-reference.com/teams/"))

df_all_coaches <- rbind(df_all_coaches, final_combined_df_coach)

}


df_coaches_clean <- df_all_coaches %>%
  mutate(team_name = str_remove(team_name, "/teams")) %>%
  mutate(team_name = str_remove(team_name, "\\d{4,}\\.html")) %>%
  mutate(team_name = str_remove_all(team_name, "/")) %>%
  rename(coach_name = name, real_name_coach = real_name) %>%
  mutate(season = as.numeric(season))


bind_rows(players_name, players_name_1) %>%
  filter(team_id != "TOT") %>%
  left_join(df_coaches_clean, by = c("year" = "season",
                                     "team_id" = "team_name")) %>%
  #filter(real_name == "augusdj01", real_name_coach == "/coaches/vogelfr99c.html") %>%
  left_join(name_change_teams, by = c("team_id" = "team_id.x")) %>%
  mutate(real_team = case_when(!is.na(team_id.y)~team_id.y, 
                               TRUE~team_id)) %>%
  left_join(name_change_teams, by = c("real_team" = "team_id.x")) %>%
  mutate(final_real_team = case_when(!is.na(team_id.y.y)~team_id.y.y, 
                               TRUE~real_team)) %>%
  relocate(final_real_team, .after = team_id) %>%
  select(-ends_with(".y")) %>%
  select(-ends_with(".x")) %>%
  group_by(player, real_name, coach_name, real_name_coach) %>%
  summarise(total_seasons = n_distinct(year), 
            total_teams = n_distinct(final_real_team)) %>%
  ungroup() %>%
  arrange(desc(total_teams)) %>%
  group_by(coach_name, real_name_coach) %>%
  mutate(total_players = n_distinct(player)) %>%
  filter(total_teams >=2) %>%
  mutate(total_players_over_2 = n_distinct(player)) %>%
  ungroup() %>%
  distinct(coach_name, real_name_coach, total_players, total_players_over_2) %>%
  mutate(pct_over_2 = total_players_over_2 / total_players) %>%
  arrange(desc(pct_over_2)) %>%
  view()
  
  filter(real_name_coach == "/coaches/riverdo01c.html") %>%
  
  view()

  
  filter(str_detect(player, fixed("taj", ignore_case = TRUE)))




colnames(df_coaches_name) <- "real_name"    

df_coaches_name %>%
  str_split_fixed()
  
  
library(rvest)
library(dplyr)
library(tidyverse)

#url - type of stat. accepts "totals", "advanced", "per poss", "per game"
#is playoff - regular season or playoffs - accepts "leagues", "playoffs

players_name <- scrape_b_ref("totals", 2004, 2024, "leagues")


players_name_1 <- scrape_b_ref("totals", 1983, 2003, "leagues")


team_name <- bind_rows(players_name, players_name_1) %>%
  group_by(team_id) %>%
  summarise(min_year = min(year),
            max_year = max(year)) %>%
  arrange(max_year) %>%
  ungroup() %>%
  view()

name_change_teams <- cross_join(team_name, team_name) %>%
  mutate(gap_year = min_year.y - max_year.x) %>%
  filter(gap_year == 1)

df_all_coaches %>%
  mutate(team = str_remove(team, "/")) %>%
  filter()
  distinct(team) %>%
  inner_join(name_change_teams, by = c("team" = "team_id.x"))
  
  left_join(name_change_teams, by = c("team" = "team_id.x")) %>%
  filter(!is.na(min_year.x)) %>%
  mutate(team = case_when(season >= min_year.y~team_id.y,
         TRUE~team)) %>%
  filter(team == "OKC")
  view()

name_change_teams
  

players_name %>%
  colnames()

df_all_coaches %>%
  group_by(team) %>%
  summarise(min_season = min(season),
            max_season = max(season)) %>%
  view()

scrape_b_ref <- function(url,year_start,year_finish,is_playoff) {
  
  
  df_mid <-NULL
  df <- NULL
  
  url_to_scrape <- paste0("https://www.basketball-reference.com/",is_playoff,"/NBA_2020_",url,".html", sep="")
  
  webpage_col <- read_html(url_to_scrape)
  col_names <- webpage_col %>% 
    html_nodes("table > thead > tr > th") %>% 
    html_attr("data-stat")
  col_names<-replace(col_names, which(col_names == "DUMMY"), paste(col_names[which(col_names == "DUMMY")],which(col_names == "DUMMY")))
  
  
  for (i in year_start:year_finish)
  {
    url_data <- paste0("https://www.basketball-reference.com/",is_playoff,"/NBA_",i,"_",url,".html",sep="")
    webpage_data <-read_html(url_data)
    
    data  <- webpage_data %>% 
      html_nodes("table > tbody > tr > td") %>% 
      html_text() 
    
    
    data_names  <- webpage_data %>%
      html_nodes("table > tbody > tr > td") %>% html_attr("data-append-csv")
    vec_names <- data_names[!is.na(data_names)]
    
    
    df<- as.data.frame(matrix(data,ncol = length(col_names)-1,byrow = TRUE),stringsAsFactors = FALSE)
    df<-cbind(df,rep(i,nrow(df)),vec_names)
    df_mid <- rbind(df_mid,df) 
  }
  
  df_final <- df_mid %>% 
    rename_at(vars(colnames(df_mid)),~c(col_names[-1],"year", "real_name")) %>% mutate_at(col_names[!col_names %in% c("ranker","player","pos","team_id","real_name")],as.numeric) %>%  mutate_at (c("player","real_name"),as.character)
  return(df_final)
}





  