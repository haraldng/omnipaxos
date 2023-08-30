use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use tui_logger::*;
use log::LevelFilter;
use slog::{self, o, Drain, info, debug};
use omnipaxos::util::OmniPaxosStates;
use std::{io::stdout, time::Duration};
use crate::app::{App, UIAppConfig};

pub mod app;
mod render;
mod util;

pub struct OmniPaxosUI {
    pub app: App,
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    started: bool,
}

impl OmniPaxosUI {
    pub fn with(config: UIAppConfig) -> Self {
        /// Configure Crossterm backend for tui
        let stdout = stdout();
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).unwrap();
        Self {
            app: App::with(config),
            terminal,
            started: false,
        }
    }

    /// Get the logger for logging into UI, need to be used with slog configration.
    pub fn logger() -> slog::Logger {
        let drain = slog_drain().fuse();
        slog::Logger::root(drain, o!())
    }

    /// Start the UI, do nothing if already started
    pub fn start(&mut self) {
        if !self.started {
            init_logger(LevelFilter::Trace).unwrap();
            set_default_level(LevelFilter::Debug);
            let mut stdout = stdout();
            crossterm::execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
            enable_raw_mode().unwrap();
            self.terminal.hide_cursor().unwrap();
            self.update();
            self.started = true;
            debug!(Self::logger(), "UI started with slog");
        }
    }

    /// Stop the UI, do nothing if already stopped
    pub fn stop(&mut self) {
        if self.started {
            disable_raw_mode().unwrap();
            crossterm::execute!(
                self.terminal.backend_mut(),
                LeaveAlternateScreen,
                DisableMouseCapture
            )
                .unwrap();
            self.terminal.show_cursor().unwrap();
            self.started = false;
        }
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    /// Handle user input, redraw the ui, should be called manually after updating the ui app
    pub fn update(&mut self) {
        // Handle user input
        if crossterm::event::poll(Duration::from_millis(0)).unwrap() {
            if let Event::Key(key) = crossterm::event::read().unwrap() {
                if let KeyCode::Esc = key.code {
                    self.stop();
                };
                if let KeyCode::Char('q') = key.code {
                    self.stop();
                };

            }
        }
        // Redraw the UI
        if self.started {
            self.terminal
                .draw(|rect| {
                    render::render(rect, &self.app);
                })
                .unwrap();
        }
    }

    /// Update the UI with the latest states from the OmniPaxos instance
    pub fn tick(&mut self, op_states: OmniPaxosStates) {
        let ballot = op_states.current_ballot;
        self.app.current_node.ballot_number = ballot.n;
        self.app.current_node.configuration_id = ballot.config_id;
        self.app.current_leader = op_states.current_leader;
        self.app.set_decided_idx(op_states.decided_idx);
        self.app.active_peers.clear();
        op_states
            .ballots
            .iter()
            .filter(|(b, _)| b.pid != self.app.current_node.pid)
            .for_each(|(b, c)| {
                self.app.active_peers.push((*b).into());
                self.app.active_peers.last_mut().unwrap().connectivity = *c;
            });
        self.app.active_peers.sort_by(|a, b| a.pid.cmp(&b.pid));
        self.app.current_node.connectivity = self.app.active_peers.len() as u8 + 1;
        self.update();
    }
}
