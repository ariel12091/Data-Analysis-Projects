
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
