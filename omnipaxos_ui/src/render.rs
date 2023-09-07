use ratatui::{
    widgets::{
        canvas::{Canvas, Line, Rectangle},
        *,
    },
    prelude::*,
};
use tui_logger::{TuiLoggerLevelOutput, TuiLoggerSmartWidget, TuiLoggerWidget, TuiWidgetState};
use omnipaxos::util::NodeId;
use std::{collections::HashMap, f64::consts::PI};
use crate::{app::App, util::defaults::*};

// render ui components
pub(crate) fn render<B>(f: &mut Frame<B>, app: &App)
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
                // Temp and Canvas
                Constraint::Min(10),
                // Temp
                Constraint::Length(8),
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
        .constraints([Constraint::Length(80), Constraint::Min(20)].as_ref())
        .split(chunks[2]);

    // Logger
    /// Smart logging window
    // let tui_sm = TuiLoggerSmartWidget::default()
    //     .style_error(Style::default().fg(Color::Red))
    //     .style_debug(Style::default().fg(Color::Green))
    //     .style_warn(Style::default().fg(Color::Yellow))
    //     .style_trace(Style::default().fg(Color::Magenta))
    //     .style_info(Style::default().fg(Color::Cyan))
    //     .output_separator(':')
    //     .output_timestamp(Some("%H:%M:%S".to_string()))
    //     .output_level(Some(TuiLoggerLevelOutput::Abbreviated))
    //     .output_target(true)
    //     .output_file(true)
    //     .output_line(true)
    //     .state(&TuiWidgetState::new().set_default_display_level(log::LevelFilter::Debug));
    // rect.render_widget(tui_sm, body_chunks[0]);

    /// Simple logging window
    let filter_state = TuiWidgetState::new()
        .set_default_display_level(log::LevelFilter::Off)
        .set_level_for_target("omnipaxos::sequence_paxos", log::LevelFilter::Debug)
        .set_level_for_target("omnipaxos::ballot_leader_election", log::LevelFilter::Debug);
    let tui_w: TuiLoggerWidget = TuiLoggerWidget::default()
        .block(
            Block::default()
                .title("Independent Tui Logger View")
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
    f.render_widget(tui_w, body_chunks[0]);

    let table_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(20), Constraint::Length(20)].as_ref())
        .split(body_chunks[1]);
    // Table
    let table = draw_table(app);
    f.render_widget(table, table_chunks[0]);
    // Progress bar
    draw_progress(f, app, table_chunks[1]);

    // Temp
    let temp = draw_temp(app);
    f.render_widget(temp, chunks[3]);
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
    BarChart::default()
        .block(
            Block::default()
                .title(format!("{}{:.2} req/s", UI_THROUGHPUT_TITLE, app.dps))
                .borders(Borders::ALL),
        )
        .data(data)
        .bar_width(UI_BARCHART_WIDTH)
        .bar_gap(UI_BARCHART_GAP)
        .value_style(Style::default().fg(Color::Black).bg(Color::LightGreen))
        .bar_style(Style::default().fg(Color::LightGreen))
}

fn draw_temp<'a>(app: &App) -> Paragraph<'a> {
    let mut temp = match app.current_leader {
        Some(ref id) => format!("Current Leader: {:?}", id),
        None => "No leader yet".to_string(),
    };

    temp.push_str(&format!("\nState: {:?}", app.current_node));
    temp.push_str(&format!("\nPeers: {:?}", app.peers));
    temp.push_str(&format!("\nDecided idx: {:?}", app.decided_idx));
    temp.push_str(&format!("\nThroughput: {:.2} dps", app.dps));
    temp.push_str(&format!("\nThroughput data: {:?}", app.throughput_data));
    Paragraph::new(temp)
        .style(Style::default().fg(Color::LightCyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        )
}

struct CanvasComponents {
    nodes: HashMap<u64, Rectangle>,
    connections: HashMap<(u64, u64), Line>,
    labels: HashMap<u64, Label<'static>>,
}

struct Label<'a> {
    x: f64,
    y: f64,
    span: Span<'a>,
}

fn make_canvas(app: &App) -> CanvasComponents {
    let num_of_nodes = app.peers.len() + 1;
    // Ids of all nodes, including the current one.
    // temp: for now the first node is the current node
    let node_ids = vec![app.current_node.pid]
        .iter()
        .chain(app.peers.iter())
        .map(|p| *p)
        .collect::<Vec<NodeId>>();
    let node_width = UI_CANVAS_NODE_WIDTH;
    let radius = UI_CANVAS_RADIUS;
    let center_x = -node_width / 2.0; // X-coordinate of the circle's center
    let center_y = -node_width / 2.0; // Y-coordinate of the circle's center

    let angle_step = 2.0 * PI / (num_of_nodes as f64); // Angle increment between each rectangle
    let mut nodes_with_rects = HashMap::new();

    // Nodes
    for i in 0..num_of_nodes {
        let angle = i as f64 * angle_step;
        let x = center_x + radius * angle.cos();
        let y = center_y + radius * angle.sin();
        let node_id = node_ids[i];
        // Color of the rectangle
        let color = Color::White;
        let rect = Rectangle {
            x,
            y,
            width: node_width,
            height: node_width,
            color,
        };
        nodes_with_rects.insert(node_id, rect);
    }

    // Connections
    let mut lines = HashMap::new();
    let i = 0;
    // for i in 0..num_of_nodes {
    for j in i..num_of_nodes {
        let node1_id = node_ids[i];
        let node2_id = node_ids[j];
        let current_rect = nodes_with_rects.get(&node1_id).unwrap();
        let next_rect = nodes_with_rects.get(&node2_id).unwrap();

        if app.active_peers.iter().any(|node| node.pid == node2_id) {
            let line = Line {
                x1: current_rect.x + current_rect.width / 2.0,
                y1: current_rect.y + current_rect.height / 2.0,
                x2: next_rect.x + next_rect.width / 2.0,
                y2: next_rect.y + next_rect.height / 2.0,
                color: Color::LightCyan,
            };
            lines.insert((i as u64, j as u64), line);
        }
    }
    // }

    // Labels
    let mut labels = HashMap::new();
    for (node_id, rect) in &nodes_with_rects {
        let label = Label {
            x: rect.x + rect.width / 4.0,
            y: rect.y + rect.width / 3.0,
            span: Span::styled(
                String::from("Node".to_string() + &*node_id.to_string()),
                Style::default().fg(Color::White),
            ),
        };
        labels.insert(*node_id, label);
    }

    CanvasComponents {
        nodes: nodes_with_rects,
        connections: lines,
        labels,
    }
}

fn draw_table<'a>(app: &App) -> Table<'a> {
    let header_cells = ["PID", "Ballot number", "Configuration ID", "Connectivity"]
        .iter()
        .map(|h| Cell::from(*h));
    let number_of_columns = header_cells.len();
    let header = Row::new(header_cells)
        .height(UI_TABLE_TITLE_HEIGHT)
        .bottom_margin(UI_TABLE_ROW_MARGIN)
        .style(Style::default().fg(Color::LightCyan).add_modifier(Modifier::BOLD));
    let rows = app.active_peers.iter().map(|peer| {
        let mut  cells = Vec::with_capacity(number_of_columns);
        cells.push(Cell::from(peer.pid.to_string()));
        cells.push(Cell::from(peer.ballot_number.to_string()));
        cells.push(Cell::from(peer.configuration_id.to_string()));
        cells.push(Cell::from(peer.connectivity.to_string()));
        Row::new(cells).height(UI_TABLE_CONTENT_HEIGHT).bottom_margin(UI_TABLE_ROW_MARGIN)
    });
    Table::new(rows)
        .header(header)
        .block(Block::default().borders(Borders::TOP | Borders::LEFT | Borders::BOTTOM).title(UI_TABLE_TITLE))
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
    let chunks = Layout::default()
        .constraints(
            [
                Constraint::Length(UI_TABLE_TITLE_HEIGHT + UI_TABLE_ROW_MARGIN),
                Constraint::Length(UI_TABLE_CONTENT_HEIGHT + UI_TABLE_ROW_MARGIN),
                Constraint::Length(UI_TABLE_CONTENT_HEIGHT + UI_TABLE_ROW_MARGIN),
                Constraint::Length(UI_TABLE_CONTENT_HEIGHT + UI_TABLE_ROW_MARGIN),
            ]
                .as_ref(),
        )
        .split(area);

    // draw border and title
    let block = Block::default().borders(Borders::RIGHT | Borders::BOTTOM | Borders::TOP).title("Progress");
    f.render_widget(block.clone(), area);

    draw_progress_bar(f, chunks[1], 1.0);
    draw_progress_bar(f, chunks[2], 0.5);
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