use crate::app::{App, UIAppConfig};
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::event::{Event, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io::stdout;
use std::time::Duration;

pub mod app;
mod render;
mod util;

pub struct UI {
    pub app: App,
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    started: bool,
}

impl UI {
    pub fn with(config: UIAppConfig) -> Self {
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

    // Start the UI, do nothing if already started
    pub fn start(&mut self) {
        if !self.started {
            let mut stdout = stdout();
            crossterm::execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
            enable_raw_mode().unwrap();
            self.terminal.hide_cursor().unwrap();
            self.update();
            self.started = true;
        }
    }

    // Stop the UI, do nothing if already stopped
    pub fn stop(&mut self) {
        if self.started {
            disable_raw_mode().unwrap();
            crossterm::execute!(
                self.terminal.backend_mut(),
                LeaveAlternateScreen,
                DisableMouseCapture
            ).unwrap();
            self.terminal.show_cursor().unwrap();
            self.started = false;
        }
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    // Handle user input, redraw the ui, should be called manually after updating the ui app
    pub fn update(&mut self) {
        // Handle user input
        if crossterm::event::poll(Duration::from_millis(0)).unwrap() {
            if let Event::Key(key) = crossterm::event::read().unwrap() {
                if let KeyCode::Char('q') = key.code {
                    self.stop();
                }
            }
        }
        // Redraw the UI
        self.terminal
            .draw(|rect| {
                render::render(rect, &self.app);
            })
            .unwrap();
    }
}
