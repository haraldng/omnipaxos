use ratatui::{
    backend::Backend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::*,
    Frame,
};
use ratatui::symbols::Marker;
use ratatui::text::Span;
use ratatui::widgets::canvas::{Canvas, Line, Rectangle};
use std::collections::HashMap;
use std::f64::consts::PI;
use crate::util::NodeId;
use crate::utils::ui::{app::App, util::*};

// render ui components
pub(crate) fn render<B>(rect: &mut Frame<B>, app: &App)
where
    B: Backend,
{
    let size = rect.size();
    let window_width: usize = size.width.into();

    // Vertical layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                // Title
                Constraint::Length(3),
                // Temp and Canvas
                Constraint::Min(10),
                // Temp
                Constraint::Length(3),
            ]
            .as_ref(),
        )
        .split(size);

    // Title
    let title = draw_title();
    rect.render_widget(title, chunks[0]);

    // Temp and Canvas
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(80), Constraint::Min(20)].as_ref())
        .split(chunks[1]);

    // Canvas
    let canvas_node = Canvas::default()
        .block(Block::default().title("Canvas").borders(Borders::ALL))
        .marker(Marker::Braille)
        .x_bounds([-90.0, 90.0])
        .y_bounds([-60.0, 60.0])
        .paint(|ctx| {
            let canvas_components = make_canvas(&app);
            for node in canvas_components.nodes.values() {
                ctx.draw(node);
            }

            for label in canvas_components.labels.values() {
                ctx.print(label.x, label.y, label.span.clone());
            }
        });
    let canvas_line_lable = Canvas::default()
        .block(Block::default().title("Canvas").borders(Borders::ALL))
        .marker(Marker::Braille)
        .x_bounds([-90.0, 90.0])
        .y_bounds([-60.0, 60.0])
        .paint(|ctx| {
            let canvas_components = make_canvas(&app);

            for line in canvas_components.connections.values() {
                ctx.draw(line);
            }
            for label in canvas_components.labels.values() {
                ctx.print(label.x, label.y, label.span.clone());
            }
        });
    rect.render_widget(canvas_line_lable, body_chunks[1]);
    rect.render_widget(canvas_node, body_chunks[1]);


    // Temp
    let temp = draw_temp(&app);
    rect.render_widget(temp, body_chunks[0]);
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

fn draw_temp<'a>(app: &App) -> Paragraph<'a> {
    let mut temp = match app.current_leader {
        Some(ref id) => format!("Current Leader: {:?}", id),
        None => "No leader yet".to_string(),
    };

    temp.push_str(&format!("\nState: {:?}", app.current_node));
    temp.push_str(&format!("\nPeers: {:?}", app.peers));
    temp.push_str(&format!("\nDecided idx: {:?}", app.decided_idx));
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
    let node_ids = vec![app.current_node.pid].iter().chain(app.peers.iter()).map(|p| *p).collect::<Vec<NodeId>>();
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

