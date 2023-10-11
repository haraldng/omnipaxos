use crate::app::{App, Node, Role, UIAppConfig};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use log::LevelFilter;
use omnipaxos::utils::ui::OmniPaxosStates;
use ratatui::{backend::CrosstermBackend, Terminal};
use slog::{self, debug, o, Drain};
use std::{io::stdout, time::Duration};
use tui_logger::*;

pub mod app;
mod render;
mod util;

pub struct OmniPaxosUI {
    app: App,
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    started: bool,
}

impl OmniPaxosUI {
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
            self.update_ui();
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
    fn update_ui(&mut self) {
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

    fn update_progress(&mut self, op_states: &OmniPaxosStates) {
        if let Some(leader_id) = self.app.current_leader {
            if leader_id == self.app.current_node.pid {
                // Current node is the leader
                self.app.current_role = Role::Leader;
                // Update the progress of all the followers
                let leader_acc_idx = op_states.cluster_state.accepted_indexes[leader_id as usize];
                for (idx, &accepted_idx) in
                    op_states.cluster_state.accepted_indexes.iter().enumerate()
                {
                    self.app.followers_progress[idx] = if leader_acc_idx == 0 {
                        0.0 // To avoid division by zero
                    } else {
                        accepted_idx as f64 / leader_acc_idx as f64
                    };
                    self.app.followers_accepted_idx[idx] = accepted_idx;
                }
            } else {
                // Current node is a follower
                self.app.current_role = Role::Follower;
            }
        }
    }

    fn update_active_peers(&mut self, op_states: &OmniPaxosStates) {
        self.app.active_peers.clear();
        op_states
            .cluster_state
            .heartbeats
            .iter()
            .filter(|reply| reply.ballot.pid != self.app.current_node.pid)
            .for_each(|reply| {
                let mut node = Node::from(reply.ballot);
                node.leader = reply.leader.pid;
                self.app.active_peers.push(node);
            });
        // Sort the active peers by pid
        self.app.active_peers.sort_by(|a, b| a.pid.cmp(&b.pid));
    }

    fn update_leader(&mut self, op_states: &OmniPaxosStates) {
        self.app.current_leader = op_states.current_leader;
        self.app.leader_color = match self.app.current_leader {
            None => Default::default(),
            Some(leader_id) => {
                if leader_id == self.app.current_node.pid {
                    self.app.current_node.color
                } else {
                    let leader = self.app.nodes.iter().find(|x| x.pid == leader_id).unwrap();
                    leader.color
                }
            }
        };
    }

    /// Update the UI if started, with the latest states from the OmniPaxos instance.
    pub fn tick(&mut self, op_states: OmniPaxosStates) {
        if self.started {
            let ballot = op_states.current_ballot;
            self.app.current_node.ballot_number = ballot.n;
            self.app.current_node.configuration_id = ballot.config_id;
            self.app.set_decided_idx(op_states.decided_idx);
            self.update_progress(&op_states);
            self.update_active_peers(&op_states);
            self.update_leader(&op_states);
            self.update_ui();
        }
    }
}
