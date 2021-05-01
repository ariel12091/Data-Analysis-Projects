#data

df_comeback <- df_final_pbp_games_2021 %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                         home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                         TRUE~FALSE),is_comeback = case_when (has_won == TRUE & adv < -15~TRUE,
                                                                                              TRUE~FALSE)) %>% group_by(game_id, is_comeback) %>% 
summarise (min_lead_time = min (time)) %>% filter (is_comeback == TRUE) %>% select(game_id, min_lead_time)

df_time_comeback <- left_join(df_final_pbp_games_2021,df_comeback,by = ("game_id")) %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                                                                 home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                                                                 TRUE~FALSE)) %>%
   mutate (comeback_completed = case_when (has_won == TRUE & is.na(min_lead_time) == FALSE & adv > 0~TRUE,
                                           TRUE~FALSE)) %>% mutate (time_comeback = case_when (comeback_completed == TRUE~time - min_lead_time)) %>% 
                                    filter (time_comeback > 0) %>% group_by(game_id) %>% summarise(minimal_comeback = min(time_comeback)) %>% arrange(minimal_comeback)

   
final_df_defense_offense <- left_join (df_comeback,df_time_comeback, by = ("game_id")) %>% mutate (max_time_comeback = min_lead_time+minimal_comeback) %>% 
      inner_join(df_final_pbp_games_2021,by = ("game_id")) %>% mutate (has_won = case_when (visitor_pts > home_pts & team == "visit_team"~TRUE,
                                                                                            home_pts > visitor_pts & team == "home_team" ~ TRUE,
                                                                                            TRUE~FALSE)) %>% filter (time <= max_time_comeback, time >= min_lead_time) %>% 
      group_by(game_id, team_offense, team_defense,has_won) %>% 
      summarise(total_points = sum(points), total_poss = sum(end_poss), pts_per_poss = (total_points/total_poss)*100)
      
off_ef <- left_join (filter(final_df_defense_offense,has_won == TRUE),filter(final_df_defense_offense,has_won == FALSE),by = c("game_id")) %>% 
   rename("pts_per_poss_off" = "pts_per_poss.x", "pts_per_poss_def" = "pts_per_poss.y" ) %>% filter (has_won.x == TRUE) %>% 
   rename( "winner" = "team_offense.x", "loser" = "team_defense.x", "total_points_off" = "total_points.x", "total_poss_off" = "total_poss.x", 
          "total_poss_def" = "total_poss.y", "total_points_def" = "total_points.y") %>% 
   select(game_id, winner,loser, pts_per_poss_off, pts_per_poss_def, total_points_off, total_poss_off, total_points_def, total_poss_def) 

off_ef <- off_ef %>%
   mutate(total_team_poss_off = colSums(off_ef[7]),total_team_poss_def = colSums(off_ef[9])) %>% 
   mutate (freq_def = total_poss_def/total_team_poss_def, freq_off = total_poss_off/total_team_poss_off, diff_off = pts_per_poss_off - 112.9, diff_def = pts_per_poss_def - 112.9) %>%
   mutate (weighted_off = freq_off * diff_off, weighted_def = freq_def*diff_def)

mean_off_diff <- colMeans(off_ef[15])
mean_def_diff <- colMeans(off_ef[14])

#visualization

off_ef %>% ggplot() + geom_segment(aes(x = game_id, xend = game_id, y=pts_per_poss_off,yend = pts_per_poss_def)) + 
   geom_point(aes(x = game_id, y = pts_per_poss_off,col = "green", size = total_poss_off)) +
   geom_point(aes(x = game_id, y = pts_per_poss_def, col = "orange", size = total_poss_def)) +
   scale_size(range = c(0.1,3.5)) +
   geom_hline(yintercept = 112.6 , linetype = "twodash", col = "orange", size = 1.5)  + theme_fivethirtyeight() + scale_color_fivethirtyeight() +
   theme(axis.ticks.x = element_blank(), axis.text.x = element_blank()) + ggtitle("Offensive and Defensive Efficiency\n in Comeback Victories") +
   labs(color = "") + scale_color_discrete(labels = c("Offensive Efficiency", "Defensive Efficiency")) + labs(size = "Total Possesions") + 
   labs(linetype = "ff") + guides(color = guide_legend(order = 1)) 
