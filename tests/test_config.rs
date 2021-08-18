use hocon::{Error, Hocon, HoconLoader};
use std::time::Duration;

pub struct TestConfig {
    pub wait_timeout: Duration,
    pub num_threads: usize,
    pub num_nodes: usize,
    pub ble_hb_delay: u64,
    pub increment_delay: u64,
    pub num_proposals: u64,
    pub num_elections: u64,
    pub gc_idx: u64,
}

impl TestConfig {
    pub fn load(name: &str) -> Result<TestConfig, Error> {
        let raw_cfg = HoconLoader::new()
            .load_file("tests/config/test.conf")?
            .hocon()?;

        let cfg: &Hocon = &raw_cfg[name];

        Ok(TestConfig {
            wait_timeout: cfg["wait_timeout"].as_duration().unwrap_or_default(),
            num_threads: cfg["num_threads"].as_i64().unwrap_or_default() as usize,
            num_nodes: cfg["num_nodes"].as_i64().unwrap_or_default() as usize,
            ble_hb_delay: cfg["ble_hb_delay"].as_i64().unwrap_or_default() as u64,
            increment_delay: cfg["increment_delay"].as_i64().unwrap_or_default() as u64,
            num_proposals: cfg["num_proposals"].as_i64().unwrap_or_default() as u64,
            num_elections: cfg["num_elections"].as_i64().unwrap_or_default() as u64,
            gc_idx: cfg["gc_idx"].as_i64().unwrap_or_default() as u64,
        })
    }
}
