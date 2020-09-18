use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub heartbeat_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Config {}
