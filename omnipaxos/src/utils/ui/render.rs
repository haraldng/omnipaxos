use crate::utils::ui::{app::App, util::UI_TITLE};
use ratatui::{
    backend::Backend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::*,
    Frame,
};

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
                // Temp
                Constraint::Length(3),
            ]
            .as_ref(),
        )
        .split(size);

    // Title
    let title = draw_title();
    rect.render_widget(title, chunks[0]);

    // Temp
    let temp = draw_temp(&app);
    rect.render_widget(temp, chunks[1]);
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
        Some(ref ballot) => format!("Current Leader: {:?}", ballot),
        None => "No leader yet".to_string(),
    };

    temp.push_str(&format!("\nPID: {}", app.pid));
    temp.push_str(&format!("\nPeers: {:?}", app.peers));
    temp.push_str(&format!("\nConfiguration ID: {}", app.configuration_id));
    temp.push_str(&format!("\nDecided Index: {}", app.decided_idx));
    temp.push_str(&format!("\nBallot: {:?}", app.ballot));
    temp.push_str(&format!("\nConnectivity: {}", app.connectivity));
    temp.push_str(&format!("\nBallots: {:?}", app.ballots));
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