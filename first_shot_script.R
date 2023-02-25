library(hoopR)
library(rvest)
library(tidyverse)

hoopR::nba_pbp()

future::plan("multisession")
pbp1 <- load_nba_pbp(seasons = c(2008:2023))


links_teams <- read_html("https://www.espn.com/nba/stats/player") %>%
  html_elements(xpath = "//select[@class='dropdown__select']//option") %>%
  html_attr("data-url")


links_teams_final <- paste("https://www.espn.com", 
                           links_teams[which(str_length(links_teams) > 1)],
                           sep = "")





install.packages("installr")

library(installr)

pbp %>%
  distinct(type_abbreviation, type_id, type_text) %>%
  filter(str_detect(type_text, regex("time",ignore_case = TRUE)))

library(crul)

teams_respons <- crul::Async$new(urls = links_teams_final)$get()
  

all_teams <- NULL

for (i in 1:length(teams_respons)) {

list_tables <- read_html(teams_respons[[i]]$parse()) %>%
  html_table()


ids_espn <- c(unique(read_html(teams_respons[[i]]$parse()) %>%
                       html_elements(xpath = "//a[@class='AnchorLink']") %>%
                       html_attr("href")),"Total")


table_team <- inner_join(cbind(list_tables[[1]],list_tables[[2]]),
cbind(list_tables[[3]],list_tables[[4]]),by = c("Name"))

table_team_id <- cbind(ids_espn, table_team,teams_respons[[i]]$url)

all_teams <- rbind(all_teams, table_team_id)


}

team_stats <- all_teams %>%
  janitor::clean_names() %>%
  distinct() %>%
  mutate(id = str_extract(ids_espn, "[0-9]{2,12}")) %>% 
  filter(name != "Total")

rank_shots_team <- team_stats %>%
  group_by(teams_respons_i_url) %>%
  mutate(rank_team_shot = row_number(desc(fga))) %>%
  select(ids_espn, id, fga, name,rank_team_shot) %>%
  view()

df_timeout <- pbp %>%
  filter(type_id=="16") %>%
  group_by(game_id, qtr) %>%
  mutate(num_timout = row_number()) %>%
  select(game_id, id, num_timout, qtr) %>%
  view()

fill()
  
df_timeout  
  
  group_by()

?fill  
  
pbp %>%
  group_by(participants_0_athlete_id, team_id) %>%
  mutate(total_games = n_distinct(game_id)) %>%
  ungroup() %>%
  left_join(df_timeout, by=c("game_id", "id","qtr")) %>%
  group_by(game_id, qtr) %>%
  fill(num_timout,.direction = "updown") %>%
  ungroup() %>%
  relocate(num_timout, .after = type_text) %>%
  filter(type_id != '615') %>%
  filter(shooting_play == TRUE) %>%
  group_by(game_id, team_id,qtr,num_timout) %>%
  mutate(row_num = row_number()) %>%
  filter(row_num == 1) %>%
  filter(type_id != "412") %>%
  ungroup() %>%
  group_by(team_id,participants_0_athlete_id,total_games) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  mutate(team_id = as.numeric(team_id)) %>%
  left_join(espn_id, by = c("participants_0_athlete_id" = "id")) %>%
  left_join(legend_teams, by = c("team_id" = "home_team_id")) %>%
  relocate(home_team_name, .after = team_id) %>%
  ungroup() %>%
  group_by(team_id) %>%
  mutate(total_plays = sum(n)) %>%
  mutate(pct_plays = n/total_plays) %>%
  mutate(per_game_play = n/total_games) %>%
  ungroup() %>%
  group_by(team_id) %>%
  mutate(rank_in_team_per_play = row_number(desc(pct_plays)),
         rank_in_team_per_game = row_number(desc(per_game_play))) %>%
  mutate(diff_1_2 = pct_plays - lead(pct_plays,order_by = rank_in_team_per_play)) %>%
  ungroup() %>%
  left_join(rank_shots_team, by = c("participants_0_athlete_id" = "id")) %>%
  mutate(home_team_regex = paste("\\b", str_to_lower(home_team_name),"\\b", sep = "")) %>%
  mutate(teams_respons_i_url = str_replace_all(teams_respons_i_url, "-"," ")) %>%
  mutate(is_name = str_detect(teams_respons_i_url, home_team_regex)) %>%
  filter(is_name == TRUE) %>%
  relocate(rank_team_shot, .after = rank_in_team_per_game) %>%
  mutate(diff_start_finish = rank_in_team_per_game - rank_team_shot) %>%
  arrange(diff_start_finish) %>%
  filter(team_id=="17") %>%
  view()
  view()




legend_teams <- pbp %>%
    distinct(home_team_id, home_team_name)

legend_teams %>%
  mutate(home_team_name = str_to_lower(home_team_name)) %>%
  fuzzyjoin::fuzzy_left_join(all_teams, by = c("home_team_name" = "teams_respons[[i]]$url"),match_fun = str_detect)

all_teams %>%
  fuzzyjoin::fuzzy_left_join(legend_teams %>%
                               mutate(home_team_name = str_to_lower(home_team_name)), 
                             by = c("teams_respons[[i]]$url" = "home_team_name"),match_fun = str_detect) %>%
  distinct(home_team_name, )

colnames(all_teams[21,30])

str_detect(all_teams[21,30], str_to_lower(legend_teams[1,2]))

library(rvest)

data_mid_table <-NULL
data_final_table <- NULL

for (j in 2012:2023) {
  
  start_link <- paste0("http://www.espn.com/nba/statistics/rpm/_/year/",j,"/page/", sep = "")
  
  for (i in 1:20) {
    
    link <- paste0(start_link,i,sep = "")
    webpage_data <- read_html(link)
    
    data <- webpage_data %>% 
      html_nodes("tr > td") %>% html_text()
    
    rpm_table <- as.data.frame(matrix(data, ncol = 9, byrow = TRUE))
    
    id_finder <-  webpage_data %>% 
      html_nodes("tr > td > a") %>% html_attr("href")  
    
    link_db <- id_finder[-c(1:4)]
    
    data_table <- cbind(rpm_table[-1,], link_db)
    
    data_mid_table <- rbind(data_mid_table,data_table)
    
  }
  
  data_final_table <- rbind(data_final_table,data_mid_table)
  
  
}


espn_id <- data_final_table %>%
  select(V2, link_db) %>% distinct() %>%
  separate(col = V2, into = c("name", "pos"),sep = ",") %>%
  mutate (id = str_extract(link_db, pattern ="[0-9]{1,12}"))


pbp %>%
  head() %>%
  view()
  

    distinct(type_id, type_text) %>%
  arrange(type_text) %>%
  view()


  group_by(type_id, type_text) %>%
  summarise(n = n())


  head() %>%
  view()