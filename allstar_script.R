date <- last_tuesday()

tidytyu <- tidytuesdayR::tt_load(date)

tech_data <- tidytyu$technology

summary(tech_data)

unique(tech_data$label)

full_data %>%
  filter(account_number == "08-01-813-0-0-9998") %>%
  distinct() %>%
  view()

library(nbastatR)

warnings()

all_star <- nbastatR::all_star_games()

all_star$urlASGVotingBREF

url_link <- "https://www.basketball-reference.com/allstar/NBA_2022.html"


all_together1 %>%
  select(-balance) %>%
  left_join(accounts_balance, by = c("account_number")) %>%
  box_write("all_together_with_balance.xlsx", dir_id = 194350311382)

accounts_balance %>%
  box_write()

accounts_balance %>%
  box_write()


accoounts_balance %>%
  box_write()

data_final <- NULL
for (i in 1970:2022) {

c(1980:2022)  
  
url_link <- paste("https://www.basketball-reference.com/allstar/NBA_",
                  c(1970:1979),".html", sep="")

response_allstar2 <- crul::Async$new(urls = url_link)$get()

library(rvest)

read_html(response_allstar[[1]]$url) %>%
  html_elements("tr > th > a") %>%
  html_attr('href')


data_playing <- map(cbind(reponse_allstar$url, 
                          read_html(response_allstar$parse()) %>%
                            html_elements("tr > th > a") %>%
                            html_attr('href'))

                    
allstar_clean <- function (x) {                    
                    
df_total <- NULL           

for (i in 1:length(x)) {
  year <- x[[i]]$url
  
    data_playing <- as.data.frame(read_html(x[[i]]$parse()) %>%
      html_elements(xpath = "//tr//th//a") %>%
      html_attr('href'))
      
  colnames(data_playing) <- "playing"
  
    data_playing <- data_playing %>%
      separate(playing, sep = "/", into = paste("x",seq(1:4),sep = ""))
      
  data_injured <- as.data.frame(read_html(x[[i]]$parse()) %>%
    html_element(xpath = '//ul[@class="page_index"]') %>%
    html_elements('li > div > a') %>%
    html_attr('href'))
    
  colnames(data_injured) <- "injury_replacement"
  
  data_injured <- data_injured %>%
    separate(injury_replacement, sep = "/", into = paste("x",seq(1:4),sep = ""))
  
  
 df_year <- mutate(full_join(data_playing, data_injured, by = c("x4")), year)
 
df_total<-rbind(df_total, df_year)

}

return (df_total)

}

read_html(response_allstar1[[5]]$parse()) %>%
  html_elements(xpath = "//tr//th//a") %>%
  html_attr('href')

df_total2 <- allstar_clean(response_allstar1)

df_total3 <- allstar_clean(response_allstar2)

df_total2 %>%
  view()

bind_rows(df_total, df_total2,df_total3) %>%
  distinct() %>%
  filter(!is.na(x2.x)) %>%
  select(x4,x2.y,year) %>%
  mutate(year_allstar = as.numeric(str_extract(year, "[0-9]{1,4}"))) %>%
  group_by(x4) %>%
  mutate(first_allstar = min(year_allstar)) %>%
  filter(first_allstar >= 1980) %>%
  mutate(injury_rep = case_when(!is.na(x2.y)~TRUE, TRUE~FALSE)) %>%
  mutate(injury_rep = case_when(year_allstar==2014 & str_detect(x4, "davis")~TRUE, 
                                        year_allstar==2015 & str_detect(x4, "cous")~TRUE,
                                        year_allstar==2015 & str_detect(x4, "korv")~TRUE,
                                        year_allstar==2015 & str_detect(x4, "now")~TRUE,
                                        year_allstar==2015 & str_detect(x4, "lill")~TRUE,
                                        year_allstar==2016 & str_detect(x4, "horf")~TRUE,
                                        year_allstar==2016 & str_detect(x4, "gasol")~TRUE,
                                        year_allstar==2017 & str_detect(x4, "antho")~TRUE,
                                        year_allstar==2018 & str_detect(x4, "george")~TRUE,
                                        year_allstar==2018 & str_detect(x4, "drag")~TRUE,
                                        year_allstar==2018 & str_detect(x4, "walk")~TRUE,
                                        year_allstar==2018 & str_detect(x4, "drum")~TRUE,
                                        year_allstar==2019 & str_detect(x4, "russed")~TRUE,
                                        year_allstar==2020 & str_detect(x4, "book")~TRUE,
                                        year_allstar==2021 & str_detect(x4, "conle")~TRUE,
                                        year_allstar==2021 & str_detect(x4, "book")~TRUE,
                                        year_allstar==2021 & str_detect(x4, "sabondo")~TRUE,
                                        year_allstar==2022 & str_detect(x4, "balll")~TRUE,
                                        year_allstar==2022 & str_detect(x4, "murra")~TRUE,
                                        year_allstar==2022 & str_detect(x4, "allenja")~TRUE,
                                TRUE~injury_rep)) %>%
    mutate(is_first = first_allstar == year_allstar) %>%
  group_by(is_first,injury_rep) %>%
  summarise(n = n()) %>%
           filter(injury_rep == TRUE) %>%
           view()
  
  
  filter(!is.na(x2.y)) %>%
  view()

df_total %>%
  view()

tryCatch(
  data_allstar <- read_html(url_link), 
  error = function(e) {NA}
)

data_playing <- data_allstar %>%
  html_elements("tr > th > a") %>%
  html_attr('href')

data_injured <- data_allstar %>%
  html_element(xpath = '//ul[@class="page_index"]') %>%
  html_elements('li > div > a') %>%
  html_attr('href')

data_playing <- cbind(data_playing, FALSE)
if (length(data_injured) > 0) {
data_injured <- cbind(data_injured, TRUE)
}

dd <- rbind(data_injured, data_playing)

data_year <- cbind(dd, i)

data_final <- rbind(data_year, data_final)

}


data_final
data_final %>%
  distinct()


df_al_roster <- as.data.frame(data_final) %>%
  rename(name = data_injured, year = i, injury_replacement = V2) %>%
  mutate(year = as.numeric(year), injury_replacement = as.logical(injury_replacement))

class(df_al_roster$year)

class(df_stats$year)

df_roster <- df_al_roster %>%
  group_by(year, name) %>%
  mutate(n = n()) %>%
  arrange(desc(n)) %>%
  ungroup() %>%
  mutate(injury_replacement = case_when(n==2~TRUE, TRUE~injury_replacement)) %>%
  mutate(injury_replacement = case_when(n==1 & injury_replacement==TRUE~FALSE, TRUE~injury_replacement)) %>%
  mutate(injury_replacement = case_when(year==2014 & str_detect(name, "davis")~TRUE, 
                                       year==2015 & str_detect(name, "cous")~TRUE,
                                       year==2015 & str_detect(name, "korv")~TRUE,
                                       year==2015 & str_detect(name, "now")~TRUE,
                                       year==2015 & str_detect(name, "lill")~TRUE,
                                       year==2016 & str_detect(name, "horf")~TRUE,
                                       year==2016 & str_detect(name, "gasol")~TRUE,
                                       year==2017 & str_detect(name, "antho")~TRUE,
                                       year==2018 & str_detect(name, "george")~TRUE,
                                       year==2018 & str_detect(name, "drag")~TRUE,
                                       year==2018 & str_detect(name, "walk")~TRUE,
                                       year==2018 & str_detect(name, "drum")~TRUE,
                                       year==2019 & str_detect(name, "russed")~TRUE,
                                       year==2020 & str_detect(name, "book")~TRUE,
                                       year==2021 & str_detect(name, "conle")~TRUE,
                                       year==2021 & str_detect(name, "book")~TRUE,
                                       year==2021 & str_detect(name, "sabondo")~TRUE,
                                       year==2022 & str_detect(name, "balll")~TRUE,
                                       year==2022 & str_detect(name, "murra")~TRUE,
                                       year==2022 & str_detect(name, "allenja")~TRUE,
                                       TRUE~injury_replacement)) %>%
  distinct() %>%
  select(-n) 


fuzzyjoin::fuzzy_left_join()

library(fuzzyjoin)

str_detect(a, "00")

a <- 200

class(a)


df_stats_year <- df_stats %>% mutate(year = as.character(year)) %>%
  group_by(real_name, player) %>% 
  mutate(start_year = min(year))  
  
glimpse(df_stats_year)

glimpse(df_al_roster)
  
install.packages("rtweet")

library(rtweet)

  bearer_token <- Sys.getenv("AAAAAAAAAAAAAAAAAAAAAIRLfwEAAAAAU1J%2BgKJlDEyCBLYznMnsHXGSd4c%3DCwiF2OuUG29KiPhx86UR5kQb3tQJLn79YaSd6vJZ5otAEY2uxL")
headers <- c(`Authorization` = sprintf('Bearer %s', bearer_token))




install.packages("fuzzyjoin")

  
  df_2022 <- df_roster %>%
    distinct() %>%
    group_by(name) %>%
    mutate(first_year = min(year)) %>%
    mutate(is_first = year==first_year) %>%
    arrange(year) %>%
    mutate(year = as.character(year)) %>%
    fuzzy_left_join(.,df_stats_year, by = c("year", "name" = "real_name"), match_fun = str_detect) %>%
    mutate(year = as.numeric(year.x), start_year = as.numeric(start_year)) %>%
    mutate(exp = year - start_year + 1) %>%
    filter(between(year, 1980, 2022)) %>% ungroup() %>%
    filter(year != 1999, is_first==FALSE) %>%
    mutate(decade = cut(year, breaks = c(1979, 1989, 1999, 2009, 2023), labels = c("80s", "90s", "00s", "10s"))) %>%
    group_by(decade) %>%
    summarise(num_first = sum(is_first), across(where(is.numeric), sum, .names = "sum_{.col}")) %>%
    ungroup() %>%
    mutate(mean_age = sum_age / num_first, mean_min = sum_mp  / sum_g, mean_pct = sum_fg / sum_fga, 
           mean_pts = sum_pts / sum_g, mean_exp = sum_exp / num_first,
           mean_reb = sum_trb / sum_g, mean_ast = sum_ast / sum_g) %>%
    select(num_first, decade, starts_with("mean")) %>%
  ungroup () %>%
  group_by(year, injury_replacement) %>%
  summarise(total_first = sum(is_first), total_players = n()) %>%
  ungroup () %>%
  group_by(injury_replacement) %>%
  summarise(sum_first = sum(total_first), sum_players = sum(total_players)) %>%
  mutate(freq = sum_first / sum_players)
  filter(between(year, 1992, 1996)) %>%
  arrange(year)

  dfx``
  
  df_stats_year
  
  install.packages("wdpar")
  
wdpar::wdpa_fetch(x = "Malta")

library(wdpar)

mlt_raw_pa_data <- wdpa_fetch("Malta", wait = TRUE,
                              download_dir = rappdirs::user_data_dir("wdpar"))
  
  
  df_al_roster %>%
    filter(year == )
  
colnames(df_al_roster)  
  

library(tidyverse)
 