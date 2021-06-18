# Data-Analysis-Projects
 Data analysis projects, mostly about the NBA. Using mostly R, some SQL. Visualization using ggplot2. Doing it for fun, self-learning and changing the discourse to a data-informed one.
 
 ## Scraping
 
### Basketball-reference game id scraper
Scraping and parsing data from Basketball-reference.com (using Rvest, tidyverse). [code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/game_id_scraping_function.R)

### Basketball-reference Play by Play scraper
Scraping and parsing play by play data (text) to tabular data (using Rvest, tidyverse, lubridate, StringR) [code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/pbp_parser.R)

### Basketball-reference Player stats scarper
Scraping and parsing data from Basketball-reference.com (using Rvest, tidyverse) [code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/player_stats_scraper.R)

### Stats.nba.com Scraper
Scraping data from stats.nba.com (using httr, stringr) [code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/stats_nba_com_scraper.R)

## Projects

### Analyzing effect of time and score on shot distribtion
I have tested the hypothesis of the effect of time and score on shot distribution, thinking that teams tend to take riskier (/higer variance) shots under certein terms (when the game is getting out of hand). However, visual analysis of shot distribution per minute has not shown any results, nor did heatmap visualisation of time (x axis) against score (y axis). 
I have also used regression with time and score in order to estimate their impact (with interaction), but the coeffiecents were small.

Instead, I have used the visulations to illustrate a different problem -  the very large share of free throws in the end of games, especially in the end of close games, which can really harm the viewing experience.

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Time%20and%20Score%20effect%20on%20shot%20distribution)

Visualization:

Bar Chart             |  Heatmap
:-------------------------:|:-------------------------:
![](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/shot%20distribution%20over%20minutes.png)  |  ![](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/heatmap.png)

### Analyzing if a comeback (closing a large defecit) impacts shot distribtion
The regression mentioned above did indicate that a comeback win (a win after a large defecit), does have an impact on its team shot distribution. In order to further invesitigate it, I have compared the shot distribution of the losing team and the winning team in each the comeback wins of 2020-2021, during the comeback. As can be seen below, no difference in shot distribution, and a large difference in shot making. This indicates that in order to come back from a large defecit, a team does not need to change its strategy. It only needs to make more shots.

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Comeback%20wins.R)

Visualization:

Shot Distribution             |  Shot Making
:-------------------------:|:-------------------------:
![](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Shot%20Distribution%20Comeback.png)  |  ![](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Shot%20making%20Comeback.png)

### Analyzing whether comeback is impacted by defense or offense
After publishing my findings above, I have received a follow-up question, asking whether the win is impacted more by strength of the defense or the offense. I have created a lolipop chart which details each game (ordered by offensive efficiency of winning team), and indicating if the offensive efficiency of the winning team is above the league average (dashed line), or if its defenseive efficiency. 
[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Comeback%20with%20Defense%20or%20Offense.R)

Visualization:

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/plot_defense_offense.png)

### Testing differences between decades in NBA with Wilcoxon signed-rank test 
Using the Wilcoxon signed-rank test in order to find out which player stats distribution has changed between decades. I have used the wilcoxon test because it is a-parmetric, and most of the stats distribution are unknown. 

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/wilcoxon.R)

Visalization:

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/wilcoxon.png)

### Checking difference between the past 7 years in margin of victory
Box-plot indicating that the average margin of victory this year has drastically increased.

Visualization:

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/box%20plot%20margin.png)

### Comet Plot 
Checking which team changes its strategy (shot distribution), in  final minutes of close games, and wether the decision is impacted by its position in the game (trailing/leading)

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Comet%20Plot.R)

Visualization:

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/comet_plot_final.png)

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/comet_plot_trail.png)

### What's the effect of guarding shots in game?
[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Effect%20of%20Contested%20Threes%20on%203PT%20Percentage)

Visualiation:

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/Effect%20plot%20opp%203pt.png)

## Just for fun

### Counting Buzzer beaters (end of game shots) per NBA team

### Counting which initials (First name and last name) produce the most NBA player, and the best NBA players (total and per capita) (SQL)

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/best%20initials.sql)

### Counting which duo of players have played in the most teams together  (SQL)

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/duo%20different%20teams.sql)

### Is the Boston Celtics is the worst team on a Sunday Afternoon?

[code](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/sunday_wining_pct.R)

visualization:

![alt text](https://github.com/ariel12091/Data-Analysis-Projects/blob/main/sunday%20tbale.png)

### Counting wire-to-wire wins 
