pub trait RPC {
    fn heartbeat(&self, req: &HeartbeatRequest) -> RPCResult<HeartbeatResponse>;
    fn vote(&self, req: &VoteRequest) -> RPCResult<VoteResponse>;
}

#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub from_id: String,
    pub to_id: String,
    pub round: u64,
}

#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub from_id: String,
    pub to_id: String,
}

#[derive(Debug, Clone)]
pub struct VoteRequest {
    pub from_id: String,
    pub to_id: String,
    pub round: u64,
}

#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub from_id: String,
    pub to_id: String,
    pub is_approved: bool,
}

quick_error! {
    #[derive(Debug)]
    pub enum RPCError {
        Io {
            from(std::io::Error)
        }
        InvalidOperation {}
        Timeout {}
        Inaccessible {}
    }
}

pub type RPCResult<T> = std::result::Result<T, RPCError>;
