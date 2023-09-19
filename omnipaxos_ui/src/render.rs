use crate::{
    app::{App, Role},
    util::defaults::*,
};
use ratatui::{
    prelude::*,
    widgets::{block::Title, *},
};
use tui_logger::{TuiLoggerLevelOutput, TuiLoggerWidget, TuiWidgetState};

/// Render ui components
pub(crate) fn render<B>(f: &mut Frame<B>, app: &App)
where
    B: Backend,
{
    if Role::Follower == app.current_role {
        render_follower(f, app);
    } else if Role::Leader == app.current_role {
        render_leader(f, app);
    }
}

/// Render ui components for follower
pub(crate) fn render_follower<B>(f: &mut Frame<B>, app: &App)
where
    B: Backend,
{
    let size = f.size();
    let window_width: usize = size.width.into();

    // Vertical layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                // Title
                Constraint::Length(3),
                // Bar Chart
                Constraint::Length(10),
                // Info
                Constraint::Length(8),
                // Table and Logger
                Constraint::Min(10),
            ]
            .as_ref(),
        )
        .split(size);

    // Title
    let title = draw_title();
    f.render_widget(title, chunks[0]);

    // Bar Chart
    let chart = draw_chart(app, window_width);
    f.render_widget(chart, chunks[1]);

    // Info
    draw_follower_info(f, app, chunks[2]);

    // Table and Logger
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(chunks[3]);

    // Logger
    let logger_w = draw_logging();
    f.render_widget(logger_w, body_chunks[0]);

    // Table
    let table = draw_follower_table(app, Borders::ALL);
    f.render_widget(table, body_chunks[1]);
}

/// Render ui components for leader
pub(crate) fn render_leader<B>(f: &mut Frame<B>, app: &App)
where
    B: Backend,
{
    let size = f.size();
    let window_width: usize = size.width.into();

    // Vertical layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                // Title
                Constraint::Length(3),
                // Bar Chart
                Constraint::Length(10),
                // Info
                Constraint::Length(8),
                // Table and Logger
                Constraint::Min(10),
            ]
            .as_ref(),
        )
        .split(size);

    // Title
    let title = draw_title();
    f.render_widget(title, chunks[0]);

    // Bar Chart
    let chart = draw_chart(app, window_width);
    f.render_widget(chart, chunks[1]);

    // Table and Logger
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(chunks[3]);

    // Logger
    let logger_w = draw_logging();
    f.render_widget(logger_w, body_chunks[0]);

    // Info
    draw_leader_info(f, app, chunks[2]);

    // Table and Progress bar
    let table_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(20), Constraint::Length(20)].as_ref())
        .split(body_chunks[1]);
    // Table
    let table = draw_leader_table(app, Borders::TOP | Borders::LEFT | Borders::BOTTOM);
    f.render_widget(table, table_chunks[0]);
    // Progress bar
    draw_progress(f, app, table_chunks[1]);
}

fn draw_title<'a>() -> Paragraph<'a> {
    Paragraph::new(UI_TITLE)
        .style(Style::default().fg(Color::LightCyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        )
}

fn draw_chart(app: &App, window_width: usize) -> BarChart {
    let data: &Vec<(&str, u64)> = &app
        .throughput_data
        .iter()
        .take(window_width / (UI_BARCHART_WIDTH + UI_BARCHART_GAP) as usize)
        .map(|(s, num)| {
            if *num > 0 {
                (s.as_str(), *num)
            } else {
                ("", 0)
            }
        })
        .collect::<Vec<(&str, u64)>>();
    let title = Title::from(Line::from(vec![
        Span::styled(
            format!("{}: {} req/s", UI_THROUGHPUT_TITLE, app.dps as u64),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            " (# reqs/tick)",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    ]));
    BarChart::default()
        .block(
            Block::default()
                // .title(format!("{}{:.2} req/s", UI_THROUGHPUT_TITLE, app.dps).cyan().bold())
                .title(title)
                .borders(Borders::ALL),
        )
        .data(data)
        .bar_width(UI_BARCHART_WIDTH)
        .bar_gap(UI_BARCHART_GAP)
        .value_style(Style::default().fg(Color::LightGreen).bg(Color::LightGreen))
        .label_style(Style::default().fg(Color::Yellow))
        .bar_style(Style::default().fg(Color::LightGreen))
}

fn draw_follower_info<B>(f: &mut Frame<B>, app: &App, area: Rect)
where
    B: Backend,
{
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(area);

    // cluster info
    let mut cluster_info = match app.current_leader {
        Some(ref id) => format!("\nCurrent Leader: {:?}", id),
        None => "\nNo leader yet".to_string(),
    };
    cluster_info.push_str(&format!("\nPeers: {:?}", app.peers));
    cluster_info.push_str(&format!(
        "\nConfiguration ID: {:?}",
        app.current_node.configuration_id
    ));
    let cluster_info_text = Paragraph::new(cluster_info)
        .style(Style::default().fg(Color::LightCyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(UI_CLUSTER_INFO_TITLE)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        );
    f.render_widget(cluster_info_text, chunks[0]);

    // node info
    let mut node_info = "".to_string();
    node_info.push_str(&format!("\nNode Id: {:?}", app.current_node.pid));
    node_info.push_str(&format!("\nRole: {:?}", app.current_role));
    node_info.push_str(&format!("\nDecided idx: {:?}", app.decided_idx));
    let node_info_text = Paragraph::new(node_info)
        .style(Style::default().fg(Color::LightCyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(UI_NODE_INFO_TITLE)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        );
    f.render_widget(node_info_text, chunks[1]);
}

fn draw_leader_info<B>(f: &mut Frame<B>, app: &App, area: Rect)
where
    B: Backend,
{
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(area);

    // cluster info
    let mut cluster_info = match app.current_leader {
        Some(ref id) => format!("\nCurrent Leader: {:?}", id),
        None => "\nNo leader yet".to_string(),
    };
    cluster_info.push_str(&format!("\nPeers: {:?}", app.peers));
    cluster_info.push_str(&format!(
        "\nConfiguration ID: {:?}",
        app.current_node.configuration_id
    ));
    let cluster_info_text = Paragraph::new(cluster_info)
        .style(Style::default().fg(Color::LightCyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(UI_CLUSTER_INFO_TITLE)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        );
    f.render_widget(cluster_info_text, chunks[0]);

    // node info
    let mut node_info = "".to_string();
    node_info.push_str(&format!("\nNode Id: {:?}", app.current_node.pid));
    node_info.push_str(&format!("\nRole: {:?}", app.current_role));
    node_info.push_str(&format!(
        "\nAccepted idx: {:?}",
        app.followers_accepted_idx[app.current_node.pid as usize]
    ));
    node_info.push_str(&format!("\nDecided idx: {:?}", app.decided_idx));
    let node_info_text = Paragraph::new(node_info)
        .style(Style::default().fg(Color::LightCyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(UI_NODE_INFO_TITLE)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        );
    f.render_widget(node_info_text, chunks[1]);
}

fn draw_logging<'a>() -> TuiLoggerWidget<'a> {
    // Smart logging window
    // let tui_sm = TuiLoggerSmartWidget::default()

    // Simple logging window
    let filter_state = TuiWidgetState::new()
        .set_default_display_level(log::LevelFilter::Off)
        .set_level_for_target("omnipaxos::sequence_paxos", log::LevelFilter::Info)
        .set_level_for_target("omnipaxos::ballot_leader_election", log::LevelFilter::Trace);
    let logger_w: TuiLoggerWidget = TuiLoggerWidget::default()
        .block(
            Block::default()
                .title(UI_LOGGING_TITLE)
                .border_style(Style::default().fg(Color::White))
                .borders(Borders::ALL),
        )
        .style_error(Style::default().fg(Color::Red))
        .style_debug(Style::default().fg(Color::Green))
        .style_warn(Style::default().fg(Color::Yellow))
        .style_trace(Style::default().fg(Color::Magenta))
        .style_info(Style::default().fg(Color::Cyan))
        .output_separator('|')
        .output_timestamp(Some("%F %H:%M:%S".to_string()))
        .output_level(Some(TuiLoggerLevelOutput::Long))
        .output_target(false)
        .output_file(false)
        .output_line(true)
        .style(Style::default())
        .state(&filter_state);
    logger_w
}

fn draw_follower_table<'a>(app: &App, borders: Borders) -> Table<'a> {
    let header_cells = ["PID", "Ballot number", "Connectivity"]
        .iter()
        .map(|h| Cell::from(*h));
    let number_of_columns = header_cells.len();
    let header = Row::new(header_cells)
        .height(UI_TABLE_CONTENT_HEIGHT)
        .bottom_margin(UI_TABLE_ROW_MARGIN)
        .style(
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        );
    let rows = app.active_peers.iter().map(|peer| {
        let mut cells = Vec::with_capacity(number_of_columns);
        cells.push(Cell::from(peer.pid.to_string()));
        cells.push(Cell::from(peer.ballot_number.to_string()));
        cells.push(Cell::from(peer.connectivity.to_string()));
        Row::new(cells)
            .height(UI_TABLE_CONTENT_HEIGHT)
            .bottom_margin(UI_TABLE_ROW_MARGIN)
    });
    Table::new(rows)
        .header(header)
        .block(Block::default().borders(borders).title(UI_TABLE_TITLE))
        .widths({
            let widths = &[
                Constraint::Percentage(20),
                Constraint::Percentage(40),
                Constraint::Percentage(40),
            ];
            assert_eq!(widths.len(), number_of_columns);
            widths
        })
        .style(Style::default().fg(Color::White))
}

fn draw_leader_table<'a>(app: &App, borders: Borders) -> Table<'a> {
    let header_cells = ["PID", "Ballot number", "Accepted index", "Connectivity"]
        .iter()
        .map(|h| Cell::from(*h));
    let number_of_columns = header_cells.len();
    let header = Row::new(header_cells)
        .height(UI_TABLE_CONTENT_HEIGHT)
        .bottom_margin(UI_TABLE_ROW_MARGIN)
        .style(
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        );
    let rows = app.active_peers.iter().map(|peer| {
        let mut cells = Vec::with_capacity(number_of_columns);
        cells.push(Cell::from(peer.pid.to_string()));
        cells.push(Cell::from(peer.ballot_number.to_string()));
        cells.push(Cell::from(
            app.followers_accepted_idx[peer.pid as usize].to_string(),
        ));
        cells.push(Cell::from(peer.connectivity.to_string()));
        Row::new(cells)
            .height(UI_TABLE_CONTENT_HEIGHT)
            .bottom_margin(UI_TABLE_ROW_MARGIN)
    });
    Table::new(rows)
        .header(header)
        .block(Block::default().borders(borders).title(UI_TABLE_TITLE))
        .widths({
            let widths = &[
                Constraint::Percentage(10),
                Constraint::Percentage(25),
                Constraint::Percentage(32),
                Constraint::Percentage(25),
            ];
            assert_eq!(widths.len(), number_of_columns);
            widths
        })
        .style(Style::default().fg(Color::White))
}

fn draw_progress<B>(f: &mut Frame<B>, app: &App, area: Rect)
where
    B: Backend,
{
    let num_of_active_peers = app.active_peers.len();
    let constraints = if num_of_active_peers > 0 {
        vec![
            Constraint::Length(UI_TABLE_CONTENT_HEIGHT + UI_TABLE_ROW_MARGIN);
            num_of_active_peers + 2
        ]
    } else {
        vec![]
    };
    let chunks = Layout::default()
        .constraints(constraints.as_ref())
        .split(area);

    // draw border and title
    let block = Block::default()
        .borders(Borders::RIGHT | Borders::BOTTOM | Borders::TOP)
        .title("Progress");
    f.render_widget(block.clone(), area);

    app.active_peers
        .iter()
        .enumerate()
        .for_each(|(idx, node_id)| {
            draw_progress_bar(
                f,
                chunks[idx + 1],
                app.followers_progress[node_id.pid as usize],
            )
        });
}

// Draw a progress bar for one node in the cell
fn draw_progress_bar<B>(f: &mut Frame<B>, cell: Rect, ratio: f64)
where
    B: Backend,
{
    let chunks = Layout::default()
        .constraints(
            [
                Constraint::Length(UI_TABLE_CONTENT_HEIGHT),
                Constraint::Length(UI_TABLE_CONTENT_HEIGHT),
            ]
            .as_ref(),
        )
        .split(cell);

    let gauge_color = if ratio == 1.0 {
        Color::Green
    } else {
        Color::LightYellow
    };
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::RIGHT))
        .gauge_style(
            Style::default()
                .fg(gauge_color)
                .add_modifier(Modifier::ITALIC | Modifier::BOLD),
        )
        .ratio(ratio);
    f.render_widget(gauge, chunks[1]);
}
