use crate::config::ConfigError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        ConfigError {
            from()
        }
        RPCError {
            from()
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
