use crate::config::Config;
use crate::error::Result;
use crate::rpc::{
    HeartbeatRequest, HeartbeatResponse, RPCError, RPCResult, VoteRequest, VoteResponse, RPC,
};
use crossbeam::Sender;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
pub struct Supervisor<R> {
    rpc: Arc<R>,
    config: Config,
    peers: Vec<String>,
    leader: String,
    round: u64,
    vote_for_id: String,
    vote_for_round: u64,
    election_last_time: Instant,
    election_random_sleep_time: Duration,
    leader_heartbeat_last_time: Instant,
    follower_heartbeat_last_time: Instant,
}

impl<R> Supervisor<R>
where
    R: RPC + Sync + Send + 'static,
{
    fn get_quorum(&self) -> usize {
        ((self.peers.len() + 1) / 2) + 1
    }

    fn peers_for_each<F>(&self, f: F)
    where
        F: Fn(&String),
    {
        for peer in self.peers.iter() {
            f(&peer)
        }
    }

    fn become_leader(&mut self) {
        self.leader = self.config.self_id.clone();
    }

    fn check_vote(&self, req: &VoteRequest) -> bool {
        if req.round <= self.vote_for_round {
            false
        } else {
            true
        }
    }

    fn election_self(&mut self, req: &VoteRequest, s: &Sender<VoteResponse>) {
        if !self.check_vote(req) {
            return;
        }
        // vote myself
        self.vote_for(self.config.self_id.clone(), req.round);
        let _ = s.send(VoteResponse {
            from_id: self.config.self_id.clone(),
            to_id: self.config.self_id.clone(),
            is_approved: true,
        });
    }

    fn vote_for(&mut self, id: String, round: u64) {
        self.vote_for_id = id;
        self.vote_for_round = round;
    }

    fn update_election_random_sleep_time(&mut self) {
        let r = rand::thread_rng().gen_range(
            self.config.election_random_sleep_time_range.0,
            self.config.election_random_sleep_time_range.1,
        );

        self.election_random_sleep_time = Duration::from_millis(r);
    }

    fn run_election(&mut self) -> Result<()> {
        let now = Instant::now();
        if now - self.election_last_time < self.election_random_sleep_time {
            return Ok(());
        }

        self.election_last_time = now;
        self.update_election_random_sleep_time();

        self.round += 1;

        info!("{}: run election {}", &self.config.self_id, self.round);

        let (s, r) = crossbeam::unbounded();

        let req = VoteRequest {
            from_id: self.config.self_id.clone(),
            to_id: self.config.self_id.clone(),
            round: self.round,
        };

        self.election_self(&req, &s);

        self.peers_for_each(|id| {
            let rpc = self.rpc.clone();
            let s = s.clone();
            let mut req = req.clone();
            req.to_id = id.clone();

            let self_id = self.config.self_id.clone();

            rayon::spawn(move || match rpc.vote(&req) {
                Ok(resp) => {
                    if resp.is_approved {
                        info!("{}: {:?} 接受了我的投票 ", &self_id, &resp.from_id);
                        let _ = s.send(resp);
                    } else {
                        info!("{}: {:?} 拒绝了我的投票", &self_id, &resp.from_id);
                    }
                }
                Err(err) => {
                    warn!("{}: rpc vote error: {:?}", &self_id, err);
                }
            });
        });

        let quorum = self.get_quorum();
        let mut n = 0;

        loop {
            select! {
                recv(r) -> _ => {
                    n += 1;
                    if n >= quorum {
                        info!("{}: 我赢了 {}", &self.config.self_id, n);
                        self.become_leader();
                        return Ok(());
                    }
                },
                default(self.config.election_timeout) => {
                    warn!("{}: election timeout {}", &self.config.self_id, n);
                    return Ok(());
                },
            }
        }
    }

    fn run_leader(&mut self) -> Result<()> {
        let now = Instant::now();

        if now - self.leader_heartbeat_last_time < self.config.heartbeat_interval {
            return Ok(());
        }

        self.leader_heartbeat_last_time = now;

        info!("{}: run leader", &self.config.self_id);

        self.peers_for_each(|id| {
            let rpc = self.rpc.clone();
            let req = HeartbeatRequest {
                from_id: self.config.self_id.clone(),
                to_id: id.clone(),
                round: self.round,
            };
            let self_id = self.config.self_id.clone();

            rayon::spawn(move || match rpc.heartbeat(&req) {
                Ok(resp) => {
                    info!("{}: leader heartbeat response {:?}", &self_id, &resp);
                }
                Err(err) => {
                    info!("{}: leader heartbeat error: {:?}", &self_id, err);
                }
            });
        });

        Ok(())
    }

    fn run_follower(&mut self) -> Result<()> {
        info!("{}: run follower", &self.config.self_id);
        if Instant::now() - self.follower_heartbeat_last_time > self.config.heartbeat_timeout {
            warn!("{}: heartbeat timeout", &self.config.self_id);
            self.leader.clear();
        }
        Ok(())
    }
}

impl<R> Supervisor<R>
where
    R: RPC + Sync + Send + 'static,
{
    pub fn new(config: Config, peers: Vec<String>, rpc: Arc<R>) -> Result<Self> {
        config.check()?;
        Ok(Supervisor {
            config,
            rpc,
            peers,
            round: Default::default(),
            leader: Default::default(),
            vote_for_id: Default::default(),
            vote_for_round: Default::default(),
            election_last_time: Instant::now(),
            election_random_sleep_time: Default::default(),
            leader_heartbeat_last_time: Instant::now(),
            follower_heartbeat_last_time: Instant::now(),
        })
    }

    pub fn get_leader(&self) -> String {
        self.leader.clone()
    }

    pub fn heartbeat(&mut self, req: &HeartbeatRequest) -> RPCResult<HeartbeatResponse> {
        info!("{}: heartbeat {:?}", &self.config.self_id, req);

        if req.from_id == self.config.self_id {
            return Err(RPCError::InvalidOperation);
        }

        if req.round < self.vote_for_round {
            return Err(RPCError::InvalidOperation);
        }

        if self.leader != req.from_id {
            info!(
                "{}: heartbeat 发现新领导人 {:?} {:?}",
                &self.config.self_id, &req.from_id, req.round
            );
        }

        self.leader = req.from_id.clone();
        self.round = req.round;

        self.follower_heartbeat_last_time = Instant::now();

        Ok(HeartbeatResponse {
            from_id: self.config.self_id.clone(),
            to_id: req.from_id.clone(),
        })
    }

    pub fn vote(&mut self, req: &VoteRequest) -> RPCResult<VoteResponse> {
        info!("{}: 收到投票 {:?}", &self.config.self_id, req);

        let mut resp = VoteResponse {
            from_id: self.config.self_id.clone(),
            to_id: req.from_id.clone(),
            is_approved: false,
        };

        if !self.check_vote(req) {
            info!(
                "{}: 拒绝投票 {:?} {:?}",
                &self.config.self_id, self.vote_for_round, req
            );
            return Ok(resp);
        }

        resp.is_approved = true;

        self.vote_for(req.from_id.clone(), req.round);

        Ok(resp)
    }

    pub fn advance(&mut self) -> Result<()> {
        if self.leader == "" {
            // no leader to election
            self.run_election()?;
        } else if self.leader == self.config.self_id {
            // leader
            self.run_leader()?;
        } else {
            // follower
            self.run_follower()?;
        }
        Ok(())
    }
}
