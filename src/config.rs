use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub struct Config {
    pub self_id: String,
    pub heartbeat_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_rpc_timeout: Duration,
    pub election_random_sleep_time_range: (u64, u64),
    pub election_random_val_range: (u64, u64),
    pub election_vote_rpc_timeout: Duration,

    pub election_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            self_id: Uuid::new_v4().to_string(),
            heartbeat_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(200),
            heartbeat_rpc_timeout: Duration::from_millis(50),
            election_random_sleep_time_range: (10, 501),
            election_random_val_range: (std::u64::MIN, std::u64::MAX),
            election_vote_rpc_timeout: Duration::from_millis(200),
            election_timeout: Duration::from_millis(100),
        }
    }
}

impl Config {}

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        InvaludItem(s: &'static str) {
            from()
            display("config error: {}", s)
        }
    }
}
