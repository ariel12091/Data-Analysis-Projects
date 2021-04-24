
library(rvest)
library(dplyr)
library(tidyverse)

scrape_b_ref <- function(url,year_start,year_finish,is_playoff) {

  
  df_mid <-NULL
  df <- NULL
  
  url_to_scrape <- paste0("https://www.basketball-reference.com/",is_playoff,"/NBA_2020_",url,".html", sep="")
  
  webpage_col <- read_html(url_to_scrape)
  col_names <- webpage_col %>% 
    html_nodes("table > thead > tr > th") %>% 
    html_attr("data-stat")
    col_names <- replace(col_names,which(col_names < 0),"empty")
  
  
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

