use std::future::Future;
use std::ops::{Add, Deref};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use futures::channel::mpsc::UnboundedSender;
use futures::executor::{block_on, LocalPool, ThreadPool, ThreadPoolBuilder};
use futures::future::join_all;
use futures::{StreamExt, TryFutureExt};
use futures::channel::oneshot;
use futures::channel::oneshot::Sender;
use futures::task::SpawnExt;
use futures_timer::Delay;
use rand::Rng;
use labrpc::Client;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT: Duration = Duration::from_millis(300);

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// State of a raft peer.
#[derive(Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub me: usize,
    pub voted_for: Option<usize>,
    pub heartbeat_time: SystemTime,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

// A single Raft peer.
pub struct Raft {
    shared: Mutex<Shared>,
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Mutex<Box<dyn Persister>>,
}

struct Shared {

    // this peer's index into peers[]
    me: usize,
    // state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    term: u64,
    is_leader: bool,
    voted_for: Option<usize>,
    heartbeat_time: SystemTime,
    // apply_ch: UnboundedSender<ApplyMsg>,
}

impl Shared {
    fn get_state(&self) -> State {
        State {
            term: self.term,
            is_leader: self.is_leader,
            me: self.me,
            voted_for:  self.voted_for,
            heartbeat_time: self.heartbeat_time,
        }
    }

    fn inc_term(&mut self) -> State {
        self.term += 1;
        self.get_state()
    }

    fn mark_as_leader(&mut self, leader: bool, term: u64) {
        self.is_leader = leader;
        self.term = term;
        if !leader {
            self.heartbeat_time = SystemTime::now();
        }
    }

    fn update_heartbeat(&mut self, term: u64) {
        self.heartbeat_time = SystemTime::now();
        self.term = term;
    }

    fn vote_for(&mut self, term: u64, candidate_id: usize) {
        self.term = term;
        self.voted_for = Option::Some(candidate_id);
        self.heartbeat_time = SystemTime::now();
    }
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            shared: Mutex::new(Shared {
                me,
                term: 0,
                is_leader: false,
                voted_for: Option::None,
                heartbeat_time: SystemTime::now(),
            }),
            peers,
            persister: Mutex::new(persister)
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))

        rf
    }

    fn get_state(&self) -> State {
        let shared = self.shared.lock().unwrap();
        shared.get_state()
    }

    fn inc_term(&self) -> State {
        let mut shared = self.shared.lock().unwrap();
        shared.inc_term()
    }

    fn mark_as_leader(&self, leader: bool, term: u64) {
        let mut shared = self.shared.lock().unwrap();
        shared.mark_as_leader(leader, term);
    }

    fn vote_for(&self, term: u64, candidate_id: usize) {
        let mut shared = self.shared.lock().unwrap();
        shared.vote_for(term, candidate_id);
    }

    fn update_heartbeat(&self, term: u64) {
        let mut shared = self.shared.lock().unwrap();
        shared.update_heartbeat(term);
    }

    fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let state = self.get_state();

        if state.term > args.term {
            return RequestVoteReply {
                term: state.term,
                vote_granted: false,
            };
        }

        let candidate_id = args.candidate_id as usize;

        let approve = state
            .voted_for
            .map(|c| c == candidate_id)
            .unwrap_or(true);

        if approve {
            self.vote_for(args.term, candidate_id)
        }

        RequestVoteReply {
            term: args.term,
            vote_granted: approve,
        }
    }

    fn handle_append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let state = self.get_state();

        if state.term > args.term {
            return AppendEntriesReply {
                term: state.term,
                success: false

            }
        }

        self.update_heartbeat(args.term);

        AppendEntriesReply {
            term: args.term,
            success: true,
        }
    }

    async fn check_leader_heartbeat(&self) {
        let now = SystemTime::now();

        let state = self.get_state();

        if state.is_leader {
            return;
        }

        let duration_since_heartbeat = now
            .duration_since(state.heartbeat_time)
            .unwrap();

        if !state.is_leader && duration_since_heartbeat > ELECTION_TIMEOUT {
            self.initiate_vote().await;
        }
    }

    async fn send_heartbeat(&self) {
        let state = self.get_state();

        if !state.is_leader {
            return;
        }

        let futures: Vec<_> = self.peers.iter()
            .enumerate()
            .filter(|p| p.0 != state.me)
            .map(|p| {
                self.send_append_entries(p.1, AppendEntriesArgs {
                    term: state.term,
                    leader_id: state.me as u32
                })
            })
            .collect();

        let responses = join_all(futures).await;

        let rejected_resp = responses
            .iter()
            .filter_map(|resp| resp.as_ref().ok())
            .find(|resp| !resp.success);

        if let Some(resp) = rejected_resp {
            self.mark_as_leader(false, resp.term);
        }
    }

    async fn initiate_vote(&self) {
        let state = self.inc_term();

        let futures: Vec<_> = self.peers.iter()
            .enumerate()
            .filter(|p| p.0 != state.me)
            .map(|p| {
                self.send_request_vote(p.1, RequestVoteArgs {
                    term: state.term,
                    candidate_id: state.me as u32,
                })
            })
            .collect();

        let responses = join_all(futures).await;

        let approved_count = responses.iter()
            .filter(|resp| match resp {
                Ok(reply) => reply.vote_granted,
                Err(_) => false
            })
            .count();

        let quorum = responses.len() / 2 + 1;

        if approved_count >= quorum {
            self.mark_as_leader(true, state.term);
        }
    }


    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    async fn send_request_vote(
        &self,
        peer: &RaftClient,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        peer.request_vote(&args).await.map_err(Error::Rpc)
    }

    async fn send_append_entries(
        &self,
        peer: &RaftClient,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        peer.append_entries(&args).await.map_err(Error::Rpc)
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

// impl Raft {
//     /// Only for suppressing deadcode warnings.
//     // #[doc(hidden)]
//     // pub fn __suppress_deadcode(&mut self) {
//     //     let _ = self.start(&0);
//     //     let _ = self.cond_install_snapshot(0, 0, &[]);
//     //     let _ = self.snapshot(0, &[]);
//     //     // let _ = self.send_request_vote(0, Default::default());
//     //     self.persist();
//     //     // let _ = &self.state;
//     //     // let _ = &self.me;
//     //     // let _ = &self.persister;
//     //     // let _ = &self.peers;
//     // }
// }

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // raft: Arc<Mutex<Raft>>,
    raft: Arc<Raft>,
    pool: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let pool = ThreadPoolBuilder::new()
            .pool_size(1)
            .create()
            .unwrap();

        let raft = Arc::new(raft);
        let raft_clone = raft.clone();

        pool.spawn_ok(async move {
            loop {
                Delay::new(HEARTBEAT_INTERVAL).await;
                raft_clone.send_heartbeat().await;
            }
        });

        let raft_clone = raft.clone();

        pool.spawn_ok(async move {
            loop {
                let duration = randomize_duration(ELECTION_TIMEOUT);
                Delay::new(duration).await;
                raft_clone.check_leader_heartbeat().await;
            }
        });

        Node {
            raft,
            pool,
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.raft.shared.lock().unwrap();
        raft.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.shared.lock().unwrap();
        raft.is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.raft.get_state()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let reply = self.raft.handle_request_vote(args);
        labrpc::Result::Ok(reply)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let reply = self.raft.handle_append_entries(args);
        labrpc::Result::Ok(reply)
    }
}

fn randomize_duration(duration: Duration) -> Duration {
    let offset: u64 = rand::thread_rng().gen_range(0, 100);
    duration.add(Duration::from_millis(offset))
}