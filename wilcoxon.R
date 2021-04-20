library(ggthemes)
library(ggrepel)
library(rvest)
library(tidyr)
library(ggplot2)
library(tidyverse)

df_100 <- scrape_b_ref("per_poss",1985,2020,"leagues")

df_per_game <-scrape_b_ref("per_game",1985,2020,"leagues")

df_reg_100 <- df_100 %>% filter (mp > 1000) %>% mutate(group = case_when(year >= 1985 & year < 1995 ~ "80s",
                                    year >= 1995 & year < 2005 ~ "90s",
                                    year >= 2005 & year < 2015 ~ "00s",
                                    year >= 2015 & year < 2020 ~ "10s")) %>%
                                    mutate (real_pos = case_when(pos %in% c("PG", "SG", "PG-SG","SG-PG", "SG-SF") ~ "G",
                                                                  pos %in% c("SF","SF-SG","SF-PF", "PF-SF", "PF") ~ "F",
                                                                  pos %in% c("C-PF","PF-C", "C") ~ "C")) %>% mutate (group = as.factor(group)) %>% drop_na(group, real_pos) %>% select(-empty)

df_per_100 <- df_per_game %>% filter (mp_per_g > 5) %>% mutate(group = case_when(year >= 1985 & year < 1995 ~ "80s",
                                                                         year >= 1995 & year < 2005 ~ "90s",
                                                                         year >= 2005 & year < 2015 ~ "00s",
                                                                         year >= 2015 & year < 2020 ~ "10s")) %>%
  mutate (real_pos = case_when(pos %in% c("PG", "SG", "PG-SG","SG-PG", "SG-SF") ~ "G",
                               pos %in% c("SF","SF-SG","SF-PF", "PF-SF", "PF") ~ "F",
                               pos %in% c("C-PF","PF-C", "C") ~ "C")) %>% mutate (group = as.factor(group)) %>% drop_na(group, real_pos)
library(rcompanion)


df_test <- as.data.frame(matrix(),stringsAsFactors = FALSE)

df_final_per <- as.data.frame(matrix(ncol = 5),stringsAsFactors = FALSE)

df_test <- as.data.frame(matrix(),stringsAsFactors = FALSE)

df_final <- as.data.frame(matrix(ncol = 5),stringsAsFactors = FALSE)

split_grp <- split(df_reg_100,f = df_reg_100$group)

for (k in 1:ncol(df_reg_100)) {
  if (is.numeric(df_reg_100[,k])) {
    for (i in 1:length(split_grp)) {
      for (j in 1:4) {
        res <-  wilcox.test(split_grp[[i]][,k],split_grp[[j]][,k],p.adjust.method = "holm")
        df_test[i,j] <- res$p.value
      }
    }
    df_test <- df_test %>% mutate (V5 = colnames(df_reg_100[k]))
    df_final <- rbind(df_test,df_final)
  }
  else {
    next
  }
}

 df_wilcox <- df_final[1:112,] %>% rename("80s" = V1, "90s" = V2,"00s" = V3, "10s" = V4, "type" = V5) %>% 
   mutate(group_compare = rep(as.character(unique(df_reg_100$group)),28)) 

pivot_wilcox <- df_wilcox %>% pivot_longer(!c(type, group_compare), names_to = "year", values_to = "p_value") %>% mutate(year = factor(year,levels = c("80s", "90s", "00","10s")))

pivot_wilcox %>% filter(!group_compare == year) %>%  ggplot(aes(year,p_value, label = group_compare, col = p_value < 0.05)) + geom_point(size = 2,alpha = 0.1) + scale_color_fivethirtyeight() + theme_fivethirtyeight() + geom_text_repel(size = 3, max.overlaps = 30) + geom_point() + ylim(0,1) + facet_wrap(~type,scales = "free")

