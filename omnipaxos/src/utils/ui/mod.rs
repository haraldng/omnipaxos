use crate::ballot_leader_election::Ballot;
use crate::utils::ui::app::{App, UIAppConfig};
use crossterm::event::DisableMouseCapture;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, LeaveAlternateScreen};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io::stdout;
use crate::OmniPaxosConfig;

mod app;
mod render;
mod util;

pub struct UI {
    // temp: pub
    pub(crate) app: App,
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    started: bool,
}

impl UI {
    pub(crate) fn with(config: UIAppConfig) -> Self {
        // Configure Crossterm backend for tui
        let stdout = stdout();
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).unwrap();
        Self {
            app: App::with(config),
            terminal,
            started: false,
        }
    }

    pub(crate) fn start(&mut self) {
        enable_raw_mode().unwrap();
        self.terminal.clear().unwrap();
        self.terminal.hide_cursor().unwrap();
        self.redraw();
        self.started = true;
    }

    pub(crate) fn stop(&mut self) {
        disable_raw_mode().unwrap();
        crossterm::execute!(
            self.terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )
        .unwrap();
        self.terminal.clear().unwrap();
        self.terminal.show_cursor().unwrap();
        self.started = false;
    }

    pub(crate) fn is_started(&self) -> bool {
        self.started
    }

    // Redraw the ui, should be called manually after updating the ui state
    pub(crate) fn redraw(&mut self) {
        self.terminal
            .draw(|rect| {
                render::render(rect, &self.app);
            })
            .unwrap();
    }

    // Only update the ui state without redrawing
    pub(crate) fn set_current_leader(&mut self, leader: Option<Ballot>) {
        self.app.current_leader = leader;
    }
}
