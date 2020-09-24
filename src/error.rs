use crate::config::ConfigError;
use crate::rpc::RPCError;

quick_error! {
    #[derive(Debug)]
    pub enum SupervisorError {
        ConfigError {
            from(ConfigError)
        }
        RPCError {
            from(RPCError)
        }
    }
}

pub type Result<T> = std::result::Result<T, SupervisorError>;
