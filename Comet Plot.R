reg_shot_dist <- df_final_pbp_games_2021 %>% filter (!(between(adv, -5,5) & between(as.numeric(time_minutes),43,53))) %>% 
   drop_na(shot_type) %>% group_by(team_offense, shot_type) %>% summarise(n = n()) %>% 
   mutate (n_1 = ifelse(shot_type == "free throw",n*0.44,n*1), freq = n_1 / sum(n_1))

clutch_shot_dist <- df_final_pbp_games_2021 %>% filter (between(adv, -5,5) & between(as.numeric(time_minutes),43,53)) %>% drop_na(shot_type) %>% 
   group_by(team_offense, shot_type) %>% summarise(n = n()) %>% 
   mutate (n_1 = ifelse(shot_type == "free throw",n*0.44,n*1), freq = n_1 / sum(n_1))

offensive_eff <- readxl::read_xlsx(path = "offensive_eff_nba_stats.xlsx",col_names = TRUE) %>% 
  mutate (TEAM = case_when(TEAM == "LA Clippers"~"Los Angeles Clippers", TRUE~TEAM)) %>% drop_na(TEAM)

df_comet <- left_join(clutch_shot_dist,reg_shot_dist,by = c("team_offense", "shot_type")) %>% select(team_offense,shot_type,freq.x, freq.y) %>% 
   rename(freq_clutch = freq.x, freq_reg = freq.y) %>% mutate(pos_neg = freq_reg-freq_clutch < 0, delta = freq_clutch-freq_reg) %>% 
   left_join(.,offensive_eff, by = c("team_offense" = "TEAM")) 
   
df_comet %>%
   ggplot() + geom_link(aes(x = freq_reg, y = fct_reorder(team_offense, OffRtg),xend = freq_clutch,yend = team_offense, size = stat(index),color = pos_neg)) +
   scale_color_manual(values = c("#E64B35FF","#00A087FF"),name = "",labels = c("Decrease","Increase"))  + scale_x_continuous(breaks = seq(0.1,0.6,0.05)) +
    facet_wrap(~shot_type) + theme_wsj() + guides(size = FALSE) + ggtitle("Change in Shot Distribution between\nClutch and Non-Clutch Situations",
                                                                          subtitle = "Team Order is by Offensive Efficiency in Clutch")




clutch_shot_dist_lead <- df_final_pbp_games_2021 %>% filter (between(adv, -5,5) & between(as.numeric(time_minutes),43,53)) %>% drop_na(shot_type) %>% 
   group_by(team_offense, is_leading, shot_type) %>% summarise(n = n()) %>% 
   mutate (n_1 = ifelse(shot_type == "free throw",n*0.44,n*1), freq = n_1 / sum(n_1))
                                                                          

df_comet_lead <- left_join(reg_shot_dist,clutch_shot_dist_lead, by = c("team_offense", "shot_type")) %>% 
   select(team_offense, shot_type,freq.x,freq.y,is_leading) %>% rename(freq_reg = freq.x,freq_clutch = freq.y) %>% 
   mutate(pos_neg = freq_reg-freq_clutch < 0, delta = freq_clutch-freq_reg)


lead_true <- df_comet_lead %>% filter (is_leading == TRUE) %>%
   ggplot() + geom_link(aes(x = freq_reg, y = team_offense,xend = freq_clutch,yend = team_offense, size = stat(index),color = pos_neg)) +
   scale_color_manual(values = c("#E64B35FF","#00A087FF"),name = "",labels = c("Decrease","Increase"))  + scale_x_continuous(breaks = seq(0.1,0.6,0.1)) +
   facet_wrap(~shot_type) + theme_wsj() + guides(size = FALSE) + ggtitle("Change in Shot Distribution in\nClutch and Non-Clutch Situations",
                                                                         subtitle = "When Leading")

lead_false <- df_comet_lead %>% filter (is_leading == FALSE) %>%
   ggplot() + geom_link(aes(x = freq_reg, y = team_offense,xend = freq_clutch,yend = team_offense, size = stat(index),color = pos_neg)) +
   scale_color_manual(values = c("#E64B35FF","#00A087FF"),name = "",labels = c("Decrease","Increase"))  + scale_x_continuous(breaks = seq(0.1,0.6,0.1)) +
   facet_wrap(~shot_type) + theme_wsj() + guides(size = FALSE) + ggtitle("Change in Shot Distribution in\nClutch and Non-Clutch Situations",
                                                                         subtitle = "When Trailing")
library(patchwork)

lead_true+lead_false
