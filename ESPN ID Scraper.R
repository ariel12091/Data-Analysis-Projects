data_mid_table <-NULL
data_final_table <- NULL

for (j in 2012:2021) {
  
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
