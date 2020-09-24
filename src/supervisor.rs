use crate::config::Config;
use crate::error::Result;
use crate::rpc::{
    HeartbeatRequest, HeartbeatResponse, RPCError, RPCResult, VoteRequest, VoteResponse, RPC,
};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
pub struct Supervisor<R> {
    rpc: Arc<R>,
    config: Config,
    peers: Vec<String>,
    term: usize,
    leader: String,
    vote_for_id: String,
    vote_for_term: usize,
    vote_for_random: u64,
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
        (self.peers.len() / 2) + 1
    }

    fn peers_for_each<F>(&self, f: F)
    where
        F: Fn(&String),
    {
        for peer in self.peers.iter() {
            f(&peer)
        }
    }

    fn set_leader(&mut self, l: String, t: usize) {
        self.leader = l;
        self.term = t;
    }

    fn become_leader(&mut self) {
        self.set_leader(self.config.self_id.clone(), self.term + 1);
    }

    fn election_self(&mut self, random: u64) {
        self.vote_for(self.config.self_id.clone(), self.term + 1, random);
    }

    fn vote_for(&mut self, id: String, term: usize, random: u64) {
        self.vote_for_id = id;
        self.vote_for_term = term;
        self.vote_for_random = random;
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

        info!("{}: run election", &self.config.self_id);

        let random = rand::thread_rng().gen_range(
            self.config.election_random_val_range.0,
            self.config.election_random_val_range.1,
        );

        self.election_self(random);
        let (s, r) = crossbeam::unbounded();

        self.peers_for_each(|id| {
            let rpc = self.rpc.clone();
            let s = s.clone();

            let req = VoteRequest {
                from_id: self.config.self_id.clone(),
                to_id: id.clone(),
                term: self.term + 1,
                random,
            };

            let self_id = self.config.self_id.clone();

            rayon::spawn(move || match rpc.vote(&req) {
                Ok(resp) => {
                    if resp.is_approved {
                        info!("{}: {:?} approved of my proposal", &self_id, &resp.from_id);
                        s.send(resp).unwrap_or_else(|err| {
                            warn!("{} {:?}", &self_id, err);
                        });
                    } else {
                        info!(
                            "{}: {:?} proposal was not approved",
                            &self_id, &resp.from_id
                        );
                    }
                }
                Err(err) => {
                    warn!("{}: rpc vote error: {:?}", &self_id, err);
                }
            });
        });

        let quorum = self.get_quorum();
        let mut n = 1;

        loop {
            select! {
                recv(r) -> _ => {
                    n += 1;
                    if n >= quorum {
                        self.become_leader();
                        return Ok(());
                    }
                },
                default(self.config.election_timeout) => {
                    warn!("{}: election timeout", &self.config.self_id);
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

        let term = self.term;

        self.peers_for_each(|id| {
            let rpc = self.rpc.clone();
            let req = HeartbeatRequest {
                from_id: self.config.self_id.clone(),
                to_id: id.clone(),
                term,
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
        let now = Instant::now();
        if now - self.follower_heartbeat_last_time < self.config.heartbeat_timeout {
            self.follower_heartbeat_last_time = now;
        } else {
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
            term: Default::default(),
            leader: Default::default(),
            vote_for_id: Default::default(),
            vote_for_term: Default::default(),
            vote_for_random: Default::default(),
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

        if req.from_id != self.leader {
            if req.term >= self.term {
                info!(
                    "{}: heartbeat got new leader {:?} {:?}",
                    &self.config.self_id, &req.from_id, req.term
                );
                // new leader
                self.leader = req.from_id.clone();
                self.term = req.term;
            } else {
                return Err(RPCError::InvalidOperation);
            }
        }

        self.follower_heartbeat_last_time = Instant::now();

        Ok(HeartbeatResponse {
            from_id: self.config.self_id.clone(),
            to_id: req.from_id.clone(),
            term: self.term,
        })
    }

    pub fn vote(&mut self, req: &VoteRequest) -> RPCResult<VoteResponse> {
        info!("{}: vote {:?}", &self.config.self_id, req);

        let mut resp = VoteResponse {
            from_id: self.config.self_id.clone(),
            to_id: req.from_id.clone(),
            is_approved: false,
        };

        if req.term <= self.term {
            return Ok(resp);
        }

        let vote_for_term = self.vote_for_term;
        let vote_for_random = self.vote_for_random;

        if req.term < vote_for_term {
            return Ok(resp);
        } else if req.term == vote_for_term && req.random <= vote_for_random {
            return Ok(resp);
        }

        resp.is_approved = true;

        self.vote_for(req.from_id.clone(), req.term, req.random);

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
