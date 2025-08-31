# app.R -------------------------------------------------------------
library(shiny)
library(dplyr)
library(dbplyr)
library(gt)
library(scales)

# Assumptions: these are dbplyr lazy tables, e.g.:
# con <- DBI::dbConnect(duckdb::duckdb())
# lineups_lookup             <- tbl(con, "lineups_lookup")
# df_pts_poss_lineups_longer <- tbl(con, "df_pts_poss_lineups_longer")
# schedule                   <- tbl(con, "schedule")
# full_rosters               <- tbl(con, "full_rosters")

ui <- fluidPage(
  titlePanel("Player ON/OFF Impact (DuckDB / dbplyr)"),
  sidebarLayout(
    sidebarPanel(
      uiOutput("date_filter_ui"),
      uiOutput("team_filter_ui"),
      tags$hr(),
      sliderInput("min_all_poss", "Min possessions per side (eligibility):",
                  min = 0, max = 2000, value = 100, step = 10),
      sliderInput("min_on_poss", "Min ON possessions (eligibility):",
                  min = 0, max = 3000, value = 300, step = 10),
      sliderInput("min_net_rtg", "Min Net RTG (display filter):",
                  min = -50, max = 50, value = -50, step = 1),
      tags$hr(),
      downloadButton("download_csv", "Download CSV")
    ),
    mainPanel(
      gt_output("onoff_gt")
    )
  )
)

server <- function(input, output, session) {
  
  # --------- UI defaults (tiny collects) ----------
  observe({
    req(exists("schedule", inherits = TRUE), exists("full_rosters", inherits = TRUE))
    
    date_rng <- schedule %>%
      distinct(game_date) %>%
      summarise(min_date = min(game_date, na.rm = TRUE),
                max_date = max(game_date, na.rm = TRUE)) %>%
      collect()
    
    s_min <- suppressWarnings(as.Date(date_rng$min_date))
    s_max <- suppressWarnings(as.Date(date_rng$max_date))
    if (is.na(s_min)) s_min <- as.Date("2000-01-01")
    if (is.na(s_max)) s_max <- as.Date("2100-01-01")
    
    output$date_filter_ui <- renderUI({
      dateRangeInput("date_range", "Game Date Range",
                     start = s_min, end = s_max,
                     min = s_min,  max = s_max,
                     format = "yyyy-mm-dd", weekstart = 0)
    })
    
    teams <- full_rosters %>%
      distinct(team_id, team_name) %>%
      arrange(team_name) %>%
      collect()
    
    output$team_filter_ui <- renderUI({
      selectizeInput("teams", "Teams",
                     choices = teams$team_name,
                     multiple = TRUE,
                     options = list(placeholder = "All teams"))
    })
  })
  
  # ---------- Debounced inputs ----------
  debounced_range <- reactive(input$date_range) %>% debounce(300)
  debounced_teams <- reactive(input$teams) %>% debounce(300)
  
  # ---------- schedule filtered by date (lazy) ----------
  sched_filtered <- reactive({
    req(exists("schedule", inherits = TRUE))
    rng <- debounced_range()
    if (is.null(rng) || length(rng) != 2) {
      schedule %>% distinct(game_id, game_date)
    } else {
      start_d <- as.Date(rng[1]); end_d <- as.Date(rng[2])
      schedule %>%
        distinct(game_id, game_date) %>%
        filter(game_date >= !!start_d, game_date <= !!end_d)   # ≥ and ≤
    }
  })
  
  # ---------- resolve selected team_ids (tiny collect) ----------
  selected_team_ids <- reactive({
    req(exists("full_rosters", inherits = TRUE))
    teams_in <- debounced_teams()
    if (is.null(teams_in) || !length(teams_in)) return(NULL)
    full_rosters %>%
      distinct(team_id, team_name) %>%
      filter(team_name %in% !!teams_in) %>%
      collect() %>%
      pull(team_id)
  })
  
  # ---------- Core pipeline (all lazy/dbplyr) ----------
  lazy_result <- reactive({
    req(exists("lineups_lookup", inherits = TRUE),
        exists("df_pts_poss_lineups_longer", inherits = TRUE),
        exists("full_rosters", inherits = TRUE))
    
    # early team filter on lineups_lookup (most selective first)
    tids <- selected_team_ids()
    base0 <- lineups_lookup %>%
      distinct(player_id, team_id, lineup_hash, is_on_verdict) %>%
      mutate(is_on_verdict = coalesce(is_on_verdict, FALSE)) %>%
      { if (!is.null(tids) && length(tids)) filter(., team_id %in% !!tids) else . }
    
    sched_tbl <- sched_filtered()
    
    # joins (remain lazy)
    base <- base0 %>%
      inner_join(df_pts_poss_lineups_longer, by = "lineup_hash") %>%
      inner_join(sched_tbl, by = "game_id")
    
    # Aggregate to PPP per (player, team, on/off, side)
    agg <- base %>%
      group_by(player_id.x, team_id.x, is_on_verdict, type_lineup) %>%
      summarise(
        total_pts  = sum(team_score, na.rm = TRUE),
        total_poss = sum(final_end_poss, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(ppp_calc = round((total_pts / if_else(total_poss == 0, NA_real_, total_poss)) * 100, 1)) %>%
      rename(player_id = player_id.x, team_id = team_id.x)
    
    # Join roster names (lazy)
    with_names <- agg %>%
      inner_join(
        full_rosters %>% distinct(player_id, firstName, lastName, team_id),
        by = c("player_id", "team_id")
      )
    
    # Eligibility thresholds from UI (≥)
    min_all <- input$min_all_poss
    min_on  <- input$min_on_poss
    
    elig <- with_names %>%
      group_by(player_id, team_id) %>%
      summarise(
        min_poss_all = min(total_poss, na.rm = TRUE),
        max_poss_on  = max(if_else(is_on_verdict, total_poss, 0.0), na.rm = TRUE),
        .groups = "drop"
      )
    
    filtered <- with_names %>%
      inner_join(
        elig %>%
          filter(min_poss_all >= !!min_all,
                 max_poss_on  >= !!min_on),
        by = c("player_id","team_id")
      )
    
    # Window 1: OFF→ON order; your flipped net calc stays
    filtered2 <- filtered %>%
      mutate(
        is_on_key = if_else(is_on_verdict, 1L, 0L),
        type_key  = case_when(
          type_lineup == "offense" ~ 1L,
          type_lineup == "defense" ~ 2L,
          TRUE ~ 3L
        )
      ) %>%
      group_by(player_id, team_id, type_lineup) %>%
      window_order(is_on_key) %>%
      mutate(net_rtg = ppp_calc - lag(ppp_calc)) %>%
      ungroup()
    
    # Window 2: by side within ON/OFF; same flip logic
    with_total <- filtered2 %>%
      group_by(player_id, team_id, is_on_verdict) %>%
      window_order(type_key) %>%
      mutate(total_net_rtg = round(lag(net_rtg) - net_rtg, 2)) %>%
      ungroup()
    
    with_team <- with_total %>%
      inner_join(full_rosters %>% distinct(team_id, team_name), by = "team_id") %>%
      mutate(onoff = if_else(is_on_verdict, "on", "off"))
    
    # Final collapse (DuckDB type-stable)
    final_lazy <- with_team %>%
      group_by(player_id, team_id, team_name, firstName, lastName) %>%
      summarise(
        offense_on_ppp  = max(if_else(type_lineup == "offense" & onoff == "on",  as.numeric(ppp_calc),  NA_real_), na.rm = TRUE),
        offense_off_ppp = max(if_else(type_lineup == "offense" & onoff == "off", as.numeric(ppp_calc),  NA_real_), na.rm = TRUE),
        defense_on_ppp  = max(if_else(type_lineup == "defense" & onoff == "on",  as.numeric(ppp_calc),  NA_real_), na.rm = TRUE),
        defense_off_ppp = max(if_else(type_lineup == "defense" & onoff == "off", as.numeric(ppp_calc),  NA_real_), na.rm = TRUE),
        
        offense_on_net  = max(if_else(type_lineup == "offense" & onoff == "on",  as.numeric(net_rtg),   NA_real_), na.rm = TRUE),
        defense_on_net  = max(if_else(type_lineup == "defense" & onoff == "on",  as.numeric(net_rtg),   NA_real_), na.rm = TRUE),
        
        total_net_rtg   = max(as.numeric(total_net_rtg), na.rm = TRUE),
        
        on_poss  = max(if_else(onoff == "on",  as.numeric(total_poss), NA_real_), na.rm = TRUE),
        off_poss = max(if_else(onoff == "off", as.numeric(total_poss), NA_real_), na.rm = TRUE),
        .groups = "drop"
      ) %>%
      rename(
        Team            = team_name,
        `First Name`    = firstName,
        `Last Name`     = lastName,
        `Total Net RTG` = total_net_rtg,
        `ON Poss`       = on_poss,
        `OFF Poss`      = off_poss,
        `Off ON PPP`    = offense_on_ppp,
        `Off OFF PPP`   = offense_off_ppp,
        `Off ON Net`    = offense_on_net,
        `Def ON PPP`    = defense_on_ppp,
        `Def OFF PPP`   = defense_off_ppp,
        `Def ON Net`    = defense_on_net
      ) %>%
      # Final display threshold: Net RTG ≥ slider
      filter(`Total Net RTG` >= !!input$min_net_rtg) %>%
      select(
        Team, `First Name`, `Last Name`, `Total Net RTG`,
        `ON Poss`, `OFF Poss`,
        `Off ON PPP`, `Off OFF PPP`, `Off ON Net`,
        `Def ON PPP`, `Def OFF PPP`, `Def ON Net`,
        player_id, team_id
      )
    
    final_lazy
  }) %>% bindEvent(debounced_range(), debounced_teams(),
                   input$min_all_poss, input$min_on_poss, input$min_net_rtg)
  
  # ---------- Styled GT (percentile-colored; sorting ON; filters OFF; sticky cols) ----------
  output$onoff_gt <- render_gt({
    df <- lazy_result() %>%
      collect() %>%
      arrange(desc(`Total Net RTG`), Team, `Last Name`, `First Name`)  # default sort
    
    pal <- col_numeric(
      palette = c("#e57373", "#f4c7a1", "#cbead3", "#59c1a9"),
      domain  = c(0, 1)
    )
    color_by_pct_desc <- function(x) { p <- percent_rank(x);     p[!is.finite(p)] <- NA; pal(p) }  # higher=better
    color_by_pct_asc  <- function(x) { p <- 1 - percent_rank(x); p[!is.finite(p)] <- NA; pal(p) }  # lower=better
    
    gt(
      df %>% select(
        Team, `First Name`, `Last Name`,
        `Total Net RTG`, `ON Poss`, `OFF Poss`,
        `Off ON PPP`, `Off OFF PPP`, `Off ON Net`,
        `Def ON PPP`, `Def OFF PPP`, `Def ON Net`
      )
    ) %>%
      tab_header(
        title = "Player ON/OFF Impact (Values, Percentile-colored)",
        subtitle = paste0("As of ", format(Sys.Date(), "%B %d, %Y"))
      ) %>%
      cols_label(
        Team = "Team", `First Name` = "First", `Last Name` = "Last",
        `Total Net RTG` = "Net RTG",
        `ON Poss` = "ON Poss", `OFF Poss` = "OFF Poss",
        `Off ON PPP` = "Off ON", `Off OFF PPP` = "Off OFF", `Off ON Net` = "Off Net",
        `Def ON PPP` = "Def ON", `Def OFF PPP` = "Def OFF", `Def ON Net` = "Def Net"
      ) %>%
      tab_spanner("Summary", columns = c(`Total Net RTG`, `ON Poss`, `OFF Poss`)) %>%
      tab_spanner("Offense", columns = c(`Off ON PPP`, `Off OFF PPP`, `Off ON Net`)) %>%
      tab_spanner("Defense", columns = c(`Def ON PPP`, `Def OFF PPP`, `Def ON Net`)) %>%
      fmt_number(columns = `Total Net RTG`, decimals = 2) %>%
      fmt_number(columns = c(`Off ON PPP`, `Off OFF PPP`, `Off ON Net`,
                             `Def ON PPP`, `Def OFF PPP`, `Def ON Net`),
                 decimals = 1) %>%
      fmt_number(columns = c(`ON Poss`, `OFF Poss`), decimals = 0, use_seps = TRUE) %>%
      tab_style(
        style = cell_borders(sides = "left", weight = px(2), color = "black"),
        locations = list(
          cells_body(columns = `Off ON PPP`),
          cells_body(columns = `Def ON PPP`),
          cells_column_labels(columns = `Off ON PPP`),
          cells_column_labels(columns = `Def ON PPP`)
        )
        ) %>%
      # Percentile coloring
      data_color(columns = c(`Total Net RTG`, `Off ON PPP`, `Off OFF PPP`, `Off ON Net`),
                 colors  = color_by_pct_desc) %>%
      data_color(columns = c(`Def ON PPP`, `Def OFF PPP`, `Def ON Net`),
                 colors  = color_by_pct_asc) %>%
      cols_width(
        Team ~ px(140), `First Name` ~ px(100), `Last Name` ~ px(110),
        everything() ~ px(80)
      ) %>%
      tab_options(
        table_body.border.bottom.width = px(1),
        table_body.border.bottom.color = "gray90",
        data_row.padding = px(2),
        heading.background.color = "#faf3ea",
        table.background.color = "#fffaf3",
        column_labels.font.weight = "bold"
      ) %>%
      # Keep sorting only (no filters/search)
      opt_interactive(
        use_sorting = TRUE,
        use_filters = FALSE,
        use_search  = FALSE,
        use_resizers = TRUE,
        page_size_default = 30,
        use_page_size_select = FALSE,
        use_compact_mode = TRUE,
        height = "auto"
      ) 
  }) %>% bindEvent(debounced_range(), debounced_teams(),
                   input$min_all_poss, input$min_on_poss, input$min_net_rtg)
  
  # ---------- Download ----------
  output$download_csv <- downloadHandler(
    filename = function() paste0("onoff_summary_", Sys.Date(), ".csv"),
    content = function(file) {
      df <- lazy_result() %>%
        collect() %>%
        arrange(desc(`Total Net RTG`), Team, `Last Name`, `First Name`)
      readr::write_csv(df, file, na = "")
    }
  )
}

shinyApp(ui, server)
