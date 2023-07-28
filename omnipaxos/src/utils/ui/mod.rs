use crate::utils::ui::app::App;
use crossterm::terminal::enable_raw_mode;
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io::stdout;

mod app;
mod render;
mod util;

pub struct UI {
    app: App,
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
}

impl UI {
    pub(crate) fn new() -> Self {
        // Configure Crossterm backend for tui
        let stdout = stdout();
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).unwrap();
        Self {
            app: App::new(),
            terminal,
        }
    }

    pub(crate) fn start(&mut self) {
        enable_raw_mode().unwrap();
        self.terminal.clear().unwrap();
        self.terminal.hide_cursor().unwrap();
        self.update();
    }

    pub(crate) fn update(&mut self) {
        self.terminal
            .draw(|rect| {
                render::render(rect, &self.app);
            })
            .unwrap();
    }
}
