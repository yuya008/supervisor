use crate::config::Config;
use crate::error::Result;
use crate::rpc::{HeartbeatRequest, HeartbeatResponse, RPCError, RPCResult, VoteRequest, RPC};
use crossbeam::sync::WaitGroup;
use crossbeam::Receiver;
use rand::Rng;
use std::sync::{Arc, RwLock};
use std::thread::{sleep, Builder};
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
struct Inner<R> {
    rpc: Arc<R>,
    config: Config,
    peers: RwLock<Vec<String>>,
    shutdown: RwLock<bool>,
    term: RwLock<usize>,
    leader: RwLock<String>,
    heartbeat_last_time: RwLock<Instant>,
}

impl<R> Inner<R>
where
    R: RPC + Sync + Send + 'static,
{
    fn get_quorum(&self) -> usize {
        (self.peers.read().unwrap().len() / 2) + 1
    }

    fn set_shutdown(&self, b: bool) {
        *self.shutdown.write().unwrap() = b;
    }

    fn get_shutdown(&self) -> bool {
        *self.shutdown.read().unwrap()
    }

    fn set_term(&self, term: usize) {
        *self.term.write().unwrap() = term;
    }

    fn get_term(&self) -> usize {
        *self.term.read().unwrap()
    }

    fn get_leader(&self) -> String {
        (*self.leader.read().unwrap()).clone()
    }

    fn peers_for_each<F>(&self, f: F)
    where
        F: Fn(&String),
    {
        let peers = self.peers.read().unwrap();

        for peer in peers.iter() {
            f(&peer)
        }
    }

    fn set_leader(&self, l: &String, t: usize) {
        let mut leader = self.leader.write().unwrap();
        let mut term = self.term.write().unwrap();

        *leader = l.clone();
        *term = t;
    }

    fn become_leader(&self) {
        self.set_leader(&self.config.self_id, self.get_term() + 1);
    }

    fn run_election(&self) -> Result<()> {
        info!("run election");
        let term = self.get_term();
        loop {
            if !self.get_shutdown() || term < self.get_term() {
                break;
            }

            let random = rand::thread_rng().gen_range(
                self.config.election_random_val_range.0,
                self.config.election_random_val_range.1,
            );

            let (s, r) = crossbeam::unbounded();

            self.peers_for_each(|id| {
                let rpc = self.rpc.clone();
                let rpc_timeout = self.config.election_vote_rpc_timeout;
                let s = s.clone();

                let req = VoteRequest {
                    from_id: self.config.self_id.clone(),
                    to_id: id.clone(),
                    term: term + 1,
                    random,
                };

                rayon::spawn(move || match rpc.vote(&req, rpc_timeout) {
                    Ok(resp) => {
                        if resp.is_approved {
                            info!("{:?} approved of my proposal", &resp.from_id);
                            s.send(resp.from_id.clone());
                        } else {
                            info!("{:?} proposal was not approved", &resp.from_id);
                        }
                    }
                    Err(err) => {
                        warn!("rpc vote error: {:?}", err);
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
                            self.become_leader();
                            return Ok(());
                        }
                    },
                    default(self.config.election_timeout) => {
                        warn!("election timeout");
                        let r = rand::thread_rng().gen_range(
                            self.config.election_random_sleep_time_range.0,
                            self.config.election_random_sleep_time_range.1,
                        );
                        info!("random sleep {}", r);
                        sleep(Duration::from_millis(r));
                        info!("start the next round");
                        break;
                    },
                }
            }
        }
        Ok(())
    }

    fn run_leader(&self) -> Result<()> {
        info!("run leader");
        while !self.get_shutdown() {
            break;
        }
        Ok(())
    }

    fn run_follower(&self) -> Result<()> {
        info!("run follower");
        while !self.get_shutdown() {
            break;
        }
        Ok(())
    }

    fn update_heartbeat_last_time(&self) {
        *self.heartbeat_last_time.write().unwrap() = Instant::now();
    }
}

#[derive(Debug)]
pub struct Supervisor<R> {
    inner: Arc<Inner<R>>,
    wg: WaitGroup,
}

impl<R> Supervisor<R>
where
    R: RPC + Sync + Send + 'static,
{
    pub fn new(config: Config, peers: Vec<String>, rpc: R) -> Self {
        Supervisor {
            inner: Arc::new(Inner {
                config,
                rpc: Arc::new(rpc),
                peers: RwLock::new(peers),
                shutdown: Default::default(),
                term: Default::default(),
                leader: Default::default(),
                heartbeat_last_time: RwLock::new(Instant::now()),
            }),
            wg: WaitGroup::new(),
        }
    }

    pub fn get_leader(&self) -> String {
        self.inner.get_leader()
    }

    pub fn heartbeat(&self, req: &HeartbeatRequest) -> RPCResult<HeartbeatResponse> {
        info!("heartbeat");
        if req.from_id == self.inner.config.self_id {
            return Err(RPCError::InvalidOperation);
        }

        if req.from_id != self.inner.get_leader() {
            if req.term > self.inner.get_term() {
                info!("heartbeat got new leader {:?} {:?}", &req.from_id, req.term);
                // new leader
                self.inner.set_leader(&req.from_id, req.term);
            } else {
                return Err(RPCError::InvalidOperation);
            }
        }

        self.inner.update_heartbeat_last_time();

        Ok(HeartbeatResponse {
            from_id: self.inner.config.self_id.clone(),
            to_id: req.from_id.clone(),
            term: self.inner.get_term(),
        })
    }

    pub fn vote(&self, req: &HeartbeatRequest) -> RPCResult<HeartbeatResponse> {
        info!("vote");

        unimplemented!()
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.inner.set_shutdown(true);
        self.wg.clone().wait();
        Ok(())
    }

    pub fn run(&mut self) -> Result<()> {
        let wg = self.wg.clone();
        let inner = self.inner.clone();
        Builder::new()
            .name("supervisor".into())
            .spawn(move || {
                while !inner.get_shutdown() {
                    inner.run_election().unwrap();
                    if inner.get_leader() == inner.config.self_id {
                        inner.run_leader().unwrap();
                    } else {
                        inner.run_follower().unwrap();
                    }
                }
                info!("supervisor shutdown");
                drop(wg);
            })
            .unwrap();
        Ok(())
    }
}
