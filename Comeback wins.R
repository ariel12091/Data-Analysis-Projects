
df_comeback <- df_final_pbp_games_2021 %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                         home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                         TRUE~FALSE),is_comeback = case_when (has_won == TRUE & adv < -15~TRUE,
                                                                                              TRUE~FALSE)) %>% group_by(game_id, is_comeback) %>% summarise (min_lead_time = min (time)) %>% filter (is_comeback == TRUE) %>% select(game_id, min_lead_time)
   


df_final_pbp_games_2021 %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                         home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                         TRUE~FALSE),is_comeback = case_when (has_won == TRUE & adv < -15~TRUE,
                                                                                              TRUE~FALSE)) %>% filter (is_comeback == TRUE) %>% distinct (game_id, .keep_all = TRUE)
                                                                                              
                                                                                                                                                                                      
df_time_comeback <- left_join(df_final_pbp_games_2021,df_comeback,by = ("game_id")) %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                                                                 home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                                                                 TRUE~FALSE)) %>%
   mutate (comeback_completed = case_when (has_won == TRUE & is.na(min_lead_time) == FALSE & adv > 0~TRUE,
                                           TRUE~FALSE)) %>% mutate (time_comeback = case_when (comeback_completed == TRUE~time - min_lead_time)) %>% 
                                    filter (time_comeback > 0) %>% group_by(game_id) %>% summarise(minimal_comeback = min(time_comeback)) %>% arrange(minimal_comeback)


final_df <- left_join (df_comeback,df_time_comeback, by = ("game_id")) %>% mutate (max_time_comeback = min_lead_time+minimal_comeback) %>% 
   inner_join(df_final_pbp_games_2021,by = ("game_id")) %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                                                         home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                                                         TRUE~FALSE)) %>% 
                  filter (time <= max_time_comeback, time >= min_lead_time) %>% drop_na(shot_type) %>% group_by(has_won,shot_type) %>% summarise(n = n()) %>% 
   mutate (n_1 = ifelse(shot_type == "free throw",n*0.44,n*1), freq = n_1 / sum(n_1))

library(ggplot2)

final_df %>% ggplot(aes(x = shot_type, y = freq, fill = has_won)) + geom_bar(stat = "identity",position = "dodge") + ylab("Frequancy") + xlab("Shot Type") +
   ggtitle(label = "Shot Distribution in Comeback Win\n by Winning Team and Losing Team") + scale_fill_discrete(name = "Team", labels = c("Losing", "Winning")) +
   theme_fivethirtyeight() + scale_color_fivethirtyeight()
   
   
   
final_df_pct <- left_join (df_comeback,df_time_comeback, by = ("game_id")) %>% mutate (max_time_comeback = min_lead_time+minimal_comeback) %>% 
   inner_join(df_final_pbp_games_2021,by = ("game_id")) %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                                                         home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                                                         TRUE~FALSE)) %>% 
                  filter (time <= max_time_comeback, time >= min_lead_time) %>% drop_na(shot_type) %>% group_by(has_won,shot_type) %>% summarise(n = n(), total_made = sum(has_made)) %>% mutate(pct = total_made / n)

final_df_pct %>% ggplot(aes(x = shot_type, y = pct, fill = has_won)) + geom_bar(stat = "identity",position = "dodge") + ylab("Frequancy") + xlab("Shot Type") +
   ggtitle(label = "Shot Making in Comeback Win\n by Winning Team and Losing Team") + scale_fill_discrete(name = "Team", labels = c("Losing", "Winning")) +
   theme_fivethirtyeight() + scale_color_fivethirtyeight()
