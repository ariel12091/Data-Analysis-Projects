scrape_par_nba_final_season <- function (season, url, mode_type, season_type) {
  
  referer <- str_split(url, "\\?")[[1]][1]
  
  headers = list(
    "Connection" = 'keep-alive',
    "Accept" = 'application/json, text/plain, */*',
    "x-nba-stats-token" = 'true',
    "X-NewRelic-ID" = 'VQECWF5UChAHUlNTBwgBVw==',
    "User-Agent" = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
    "x-nba-stats-origin" = 'stats',
    "Sec-Fetch-Site" = 'same-origin',
    "Sec-Fetch-Mode" = 'cors',
    "Referer" = referer,
    "Accept-Encoding" = 'gzip, deflate, br',
    "Accept-Language" = 'en-US,en;q=0.9'
  )
  
  headers <- headers_function(url)
  param_list <- season_param(url, season_type, mode_type)
  req_list <- map(season, ~season_function(.x, param_list, headers))
  list_body <- scrape_function(req_list)
  final_df <- clean_df_function(list_body)
    
  return(final_df)
  
}


library(httr2)
library(tidyverse)

headers_function <- function(url) {

referer <- str_split(url, "\\?")[[1]][1]

headers = list(
  "Connection" = 'keep-alive',
  "Accept" = 'application/json, text/plain, */*',
  "x-nba-stats-token" = 'true',
  "X-NewRelic-ID" = 'VQECWF5UChAHUlNTBwgBVw==',
  "User-Agent" = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
  "x-nba-stats-origin" = 'stats',
  "Sec-Fetch-Site" = 'same-origin',
  "Sec-Fetch-Mode" = 'cors',
  "Referer" = referer,
  "Accept-Encoding" = 'gzip, deflate, br',
  "Accept-Language" = 'en-US,en;q=0.9'
)

return (headers)

}


season_param <- function(url, season_type, mode_type) {
  
  url_param <- str_split(url, "\\?")[[1]][2]
  param_list <- str_split(str_split(url_param, "&")[[1]],"=")
  param_names <- map_chr(param_list, ~str_split(.x,"=")[[1]][1])
  names(param_list) <- param_names
  param_list <- map(param_list, ~.x[[-1]])
  if (!is.null(mode_type)) {param_list$PerMode=mode_type}
  param_list$SeasonType= season_type
  
  return(param_list)
  
}


season_function <- function (season, param_list, headers) {
  year=paste(season, str_remove(season+1, "^\\d{1,2}"), sep = "-")
  local_params<-param_list
  if ("Season" %in% names(param_list)) {
    local_params$Season=year
  }
  
  else {
    local_params$SeasonYear=year
  }
  
  req <- httr2::request(headers$Referer) %>%
    req_url_query(!!!local_params) %>%
    req_headers(!!!headers)
  
  return(req)
  
}


game_function <- function (game_id, headers) {
  local_params <- NULL
  local_params$GameID=game_id
  
  req <- httr2::request(referer) %>%
    req_url_query(!!!local_params) %>%
    req_headers(!!!headers)
  
  return(req)
  
}


req_list <- map (game_id, games_function)


scrape_function  <- function(req_list)
{
  
req_list[[1]] 
  
  chunk_size <- 10
  
  total_body <- list()
  for (j in seq(1, length(req_list), chunk_size)) {
    end_index <- min(j + chunk_size - 1, length(req_list))
    store_body <- req_perform_parallel(reqs = req_list[j:end_index], on_error = "continue")
    total_body <- append(total_body, store_body)
    Sys.sleep(2)
  }
  
  list_body <- map(total_body, ~resp_body_json(.x, simplifyVector = TRUE)) 
  return(list_body)
  
}


clean_df_function <- function (list_body) {
  final_df <- tibble()
  for (i in 1:length(list_body)) {
    if (length(list_body[[i]]$resultSets$rowSet[[1]]) > 0) {
      col_names <- list_body[[i]]$resultSets$headers[[1]]
      
      year <- NULL
      
      if ("Season" %in% names(list_body[[i]]$parameters)) {
        year=list_body[[i]]$parameters$Season
      }
      
      if ("SeasonYear" %in% names(list_body[[i]]$parameters)) {
        year=list_body[[i]]$parameters$SeasonYear
      }
      
      df <- as.data.frame(list_body[[i]]$resultSets$rowSet[[1]])
      colnames(df) <- col_names
      df <- df %>%
        mutate(year=year)
      final_df <- bind_rows(df, final_df)
    }
    
    else {
      next
    }
    
  }
  
  final_df <- final_df %>%
    janitor::clean_names() %>%
    mutate(across(-any_of(c("player_id", "player_name", "nickname",
                            "team_id", "team_abbreviation", "year",
                            "team_name", "play_type", "type_grouping",
                            "player_last_name", "player_first_name",
                            "player_slug", "person_id","team_slug", "position",
                            "height", "college", "country", "roster_status", "stats_timeframe",
                            "game_date", "matchup", "wl", 
                            "off_player_name", "def_player_name", "def_player_id", "off_player_id",
                            "matchup_min")),~as.numeric(.x)))
  
  return(final_df)
}
