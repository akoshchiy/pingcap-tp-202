use core::panicking::panic;
use std::cmp::max;
use std::detect::__is_feature_detected::sha;
use std::future::Future;
use std::ops::{Add, Deref};
use std::os::macos::raw::stat;
use std::process::id;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use futures::channel::mpsc::UnboundedSender;
use futures::executor::{block_on, LocalPool, ThreadPool, ThreadPoolBuilder};
use futures::future::{join_all, RemoteHandle};
use futures::{FutureExt, select, StreamExt, TryFutureExt, TryStreamExt};
use futures::channel::oneshot;
use futures::channel::oneshot::{Receiver, Sender};
use futures::stream::FuturesUnordered;
use futures::task::SpawnExt;
use futures_timer::Delay;
use log::Log;
use prost::encoding::float::encoded_len;
use rand::Rng;
use labrpc::Client;
use linearizability::models::Op;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::PeerStatus::Leader;


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
    pub status: PeerStatus,
    pub me: usize,
    pub voted_for: Option<Vote>,
    pub received_heartbeat_time: Option<SystemTime>,
    pub sent_heartbeat_time: Option<SystemTime>,
    pub commit_idx: usize,
    pub last_applied: usize,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.status == PeerStatus::Leader
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Vote {
    peer: usize,
    term: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    Follower,
    Candidate,
    Leader
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT: Duration = Duration::from_millis(300);

// A single Raft peer.
pub struct Raft {
    shared: Mutex<Shared>,
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Mutex<Box<dyn Persister>>,
}

#[derive(Debug)]
struct LogEntry {
    term: u64,
    data: Vec<u8>,
}

struct Shared {
    // this peer's index into peers[]
    me: usize,
    // state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    status: PeerStatus,
    term: u64,
    voted_for: Option<Vote>,
    received_heartbeat_time: Option<SystemTime>,
    sent_heartbeat_time: Option<SystemTime>,
    commit_idx: usize,
    last_applied: usize,
    logs: Vec<LogEntry>,
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Shared {
    fn get_state(&self) -> State {
        State {
            term: self.term,
            status: self.status,
            me: self.me,
            voted_for: self.voted_for,
            received_heartbeat_time: self.received_heartbeat_time,
            sent_heartbeat_time: self.sent_heartbeat_time,
            commit_idx: self.commit_idx,
            last_applied: self.last_applied,
        }
    }

    fn inc_term(&mut self) {
        self.term += 1;
    }

    fn append_entry(&mut self, data: Vec<u8>) {
        let entry = LogEntry {
            term: self.term as u64,
            data,
        };
        self.logs.push(entry);
    }

    fn update_entry(&mut self, idx: usize, entry: LogEntry) {
        if idx > self.logs.len() {
            panic!("");
        }
        self.logs[idx - 1] = entry;
    }

    fn get_log_entry(&self, idx: usize) -> Option<&LogEntry> {
        self.logs.get(idx - 1)
    }

    fn get_logs_size(&self) -> usize {
        self.logs.len()
    }

    fn get_last_entry_info(&self) -> (u64, u64) {
        if self.logs.is_empty() {
            return (0, 0);
        }
        let idx = self.logs.len();
        let entry = &self.logs[self.logs.len() - 1];
        (idx as u64, entry.term)
    }

    fn update_status(&mut self, status: PeerStatus) {
        self.status = status;
    }

    fn update_received_heartbeat(&mut self) {
        self.received_heartbeat_time = Option::Some(SystemTime::now());
    }

    fn update_sent_heartbeat(&mut self) {
        self.sent_heartbeat_time = Option::Some(SystemTime::now());
    }

    fn update_term(&mut self, term: u64) {
        self.term = term;
    }

    fn vote_for(&mut self, term: u64, candidate_id: usize) {
        self.voted_for = Option::Some(Vote {
            peer: candidate_id,
            term
        });
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
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            shared: Mutex::new(Shared {
                me,
                term: 0,
                status: PeerStatus::Follower,
                voted_for: Option::None,
                received_heartbeat_time: Option::None,
                sent_heartbeat_time: Option::None,
                commit_idx: 0,
                last_applied: 0,
                logs: Vec::new(),
                apply_ch,
            }),
            peers,
            persister: Mutex::new(persister),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))

        rf
    }

    fn update_shared<F, R>(&self, f: F) -> R
        where F: Fn(&mut Shared) -> R
    {
        let mut shared = self.shared.lock().unwrap();
        f(&mut shared)
    }

    fn get_state(&self) -> State {
        self.extract(|s| s.get_state())
    }

    fn is_leader(&self) -> bool {
        self.extract(|s| s.status == PeerStatus::Leader)
    }

    fn get_me(&self) -> usize {
        self.extract(|s| s.me)
    }

    fn extract<F, R>(&self, extractor: F) -> R
        where F: Fn(&Shared) -> R
    {
        let shared = self.shared.lock().unwrap();
        extractor(&shared)
    }

    fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let state = self.get_state();

        if state.term > args.term {
            info!("peer#{} - request_vote, my term: {}, candidate_term: {}, candidate_id: {}, reject",
                state.me, state.term, args.term, args.candidate_id);

            return RequestVoteReply {
                term: state.term,
                vote_granted: false,
            };
        }

        let already_voted = state.voted_for
            .map(|v| v.term >= args.term)
            .unwrap_or(false);

        let already_candidate =
            state.status == PeerStatus::Candidate && state.term >= args.term;

        if !already_voted && !already_candidate {
            self.update_shared(|s| {
                s.update_term(args.term);
                s.vote_for(args.term, args.candidate_id as usize);
                s.update_status(PeerStatus::Follower);
                s.update_received_heartbeat();
            });
        }

        info!("peer#{} - request_vote, my term: {}, candidate_term: {}, candidate_id: {}, {}",
                state.me, state.term, args.term, args.candidate_id,
            if !already_voted {"approve"} else {"reject"}
        );

        RequestVoteReply {
            term: args.term,
            vote_granted: !already_voted,
        }
    }

    fn handle_append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        self.handle_heartbeat(args)

        // if args.entries.is_empty() {
        //     self.handle_heartbeat(args)
        // } else {
        //     self.handle_new_entries(args)
        // }
    }

    fn handle_new_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        return self.update_shared(|shared| {
            if shared.term > args.term {
                return AppendEntriesReply {
                    peer: shared.me as u32,
                    term: shared.term,
                    success: false
                }
            }

            shared.update_received_heartbeat();
            shared.update_term(args.term);
            shared.update_status(PeerStatus::Follower);

            let has_conflict = shared.logs.get(args.prev_log_idx as usize)
                .map(|e| e.term != args.prev_log_term)
                .unwrap_or(true);

            if has_conflict {
                return AppendEntriesReply {
                    peer: shared.me as u32,
                    term: shared.term,
                    success: false
                }
            }

            for idx in args.prev_log_idx..args.entries.len() {
                if idx > shared.get_logs_size() {
                    shared.append_entry(LogEntry {
                        data: a
                    })
                }



            }















            unimplemented!()
        });
    }

    fn handle_heartbeat(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let state = self.get_state();

        if state.term > args.term {
            return AppendEntriesReply {
                peer: state.me as u32,
                term: state.term,
                success: false
            }
        }

        self.update_shared(|s| {
            s.update_received_heartbeat();
            s.update_term(args.term);
            s.update_status(PeerStatus::Follower);
        });

        AppendEntriesReply {
            peer: state.me as u32,
            term: args.term,
            success: true,
        }
    }

    async fn check_leader_heartbeat(&self) {
        let now = SystemTime::now();

        let state = self.get_state();

        if state.is_leader() {
            return;
        }

        let duration_since = state
            .received_heartbeat_time
            .map(|t| now.duration_since(t).unwrap());

        let expired = duration_since
            .map(|d| d > ELECTION_TIMEOUT)
            .unwrap_or(true);

        if expired {
            let dur_ms = duration_since
                .map(|d| d.as_millis())
                .unwrap_or(0);
            info!("peer#{} - last heartbeat: {}, initiating new vote",
                state.me, dur_ms);

            self.initiate_vote().await;
        }
    }

    async fn send_heartbeat(&self) {
        let state = self.get_state();

        if !state.is_leader() {
            return;
        }

        let skip_heartbeat = state.sent_heartbeat_time
            .map(|t|  SystemTime::now().duration_since(t).unwrap())
            .map(|d| d < HEARTBEAT_INTERVAL)
            .unwrap_or(false);

        if skip_heartbeat {
            return;
        }

        info!("peer#{} - try send heartbeat, term: {}", state.me, state.term);

        self.update_shared(|s| s.update_sent_heartbeat());

        let args = AppendEntriesArgs {
            term: state.term,
            leader_id: state.me as u32,
            entries: Vec::with_capacity(0),
            leader_commit: 0,
            prev_log_idx: 0,
            prev_log_term: 0,
        };

        let futures: FuturesUnordered<_> = self.peers.iter()
            .enumerate()
            .filter(|p| p.0 != state.me)
            .map(|p| {
                self.send_append_entries(p.1, &args)
            })
            .collect();

        let ctx = HeartbeatCtx {
            me: state.me,
            failed_count: AtomicUsize::new(0),
            quorum: self.peers.len() / 2 + 1,
        };

        futures.for_each_concurrent(None, |resp| async {
            match resp {
                Ok(reply) => self.handle_heartbeat_reply(reply, &ctx),
                Err(err) => self.handle_heartbeat_error(err, &ctx),
            }
        }).await;
    }

    fn handle_heartbeat_reply(&self, reply: AppendEntriesReply, ctx: &HeartbeatCtx) {
        if reply.success {
            return;
        }

        info!("peer#{} - heartbeat reject, loosing leadership, peer: {}, term: {}",
                            ctx.me, reply.peer, reply.term);

        self.update_shared(|s| {
            s.update_status(PeerStatus::Follower);
            s.update_term(reply.term);
        });
    }

    fn handle_heartbeat_error(&self, err: Error, ctx: &HeartbeatCtx) {
        warn!("peer#{} - send_heartbeat error resp: {}", ctx.me, err.to_string());
        ctx.inc_failed_count();

        if ctx.get_failed_count() >= ctx.quorum {
            info!("peer#{} - send_heartbeat error quorum, loosing leadership", ctx.me);
            self.update_shared(|s| {
                s.update_status(PeerStatus::Follower);
            });
        }
    }

    async fn initiate_vote(&self) {
        let state = self.get_state();

        if state.is_leader() {
            return;
        }

        let state = self.update_shared(|s| {
            s.inc_term();
            s.update_status(PeerStatus::Candidate);
            s.get_state()
        });

        let args = RequestVoteArgs {
            term: state.term,
            candidate_id: state.me as u32,
        };

        let futures: FuturesUnordered<_> = self.peers.iter()
            .enumerate()
            .filter(|p| p.0 != state.me)
            .map(|p| {
                self.send_request_vote( p.1, RequestVoteArgs {
                    term: state.term,
                    candidate_id: state.me as u32,
                })
            })
            .collect();

        let ctx = VoteCtx {
            me: state.me,
            term: state.term,
            approved_count: AtomicUsize::new(1),
            quorum: self.peers.len() / 2 + 1,
            approved: AtomicBool::new(false)
        };

        futures.for_each_concurrent(None, |resp| async {
            match resp {
                Ok(reply) => self.handle_vote_reply(reply, &ctx),
                Err(err) =>
                    warn!("peer#{} - request_vote[term={}] error resp: {}", ctx.me, ctx.term, err.to_string()),
            }
        }).await;
    }

    fn handle_vote_reply(&self, reply: RequestVoteReply, ctx: &VoteCtx) {
        if !reply.vote_granted {
            info!("peer#{} - received vote reject, my.term: {}, resp.term: {}",
                                ctx.me, ctx.term, reply.term);
            return;
        }

        ctx.inc_approved_count();
        let count = ctx.get_approved_count();

        info!("peer#{} - received vote approve, current: {}", ctx.me, count);

        if count >= ctx.quorum && !ctx.is_approved() {
            info!("peer#{} - received quorum approves, i am leader, term={}",
                                        ctx.me, ctx.term);

            self.update_shared(|s| {
                s.update_status(PeerStatus::Leader);
            });

            ctx.set_approved();
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
        args: &RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        peer.request_vote(args).await.map_err(Error::Rpc)
    }

    async fn send_append_entries(
        &self,
        peer: &RaftClient,
        args: &AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        peer.append_entries(args).await.map_err(Error::Rpc)
    }

    fn append_command<M>(&self, command: &M) -> Result<AppendLogResult>
        where
            M: labcodec::Message,
    {
        if !self.is_leader() {
            return Err(Error::NotLeader);
        }

        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;

        let (term, idx, prev_term, prev_idx) = self.update_shared(|s| {
            let prev_info = s.get_last_entry_info();
            s.append_entry(buf.clone());
            let current_info = s.get_last_entry_info();
            (current_info.1, current_info.0, prev_info.1, prev_info.0)
        });

        Ok(AppendLogResult {
            term,
            prev_term,
            prev_idx,
            idx,
            data: buf,
        })
    }

    async fn replicate_entries(
        &self,
        peer: usize,
        ctx: Arc<ReplicateEntriesCtx>,
        append_result: AppendLogResult
    ) {
        let state = self.get_state();

        let args = AppendEntriesArgs {
            term: append_result.term,
            leader_id: state.me as u32,
            entries: vec![EntryItem {
                term: append_result.term,
                data: append_result.data,
            }],
            leader_commit: state.commit_idx as u64,
            prev_log_idx: append_result.prev_idx,
            prev_log_term: append_result.prev_term,
        };

        let result = self.send_append_entries(&self.peers[peer], &args).await;

        match result {
            Ok(reply) => {


            },
            Err(err) => {},
        }
    }

    fn handle_replicate_entries_resp(&self) {

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
    raft: Arc<Raft>,
    pool: TaskPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let pool = TaskPool::new(1);
        let raft = Arc::new(raft);

        Node::spawn_tasks(raft.clone(), pool.clone());

        Node {
            raft,
            pool,
        }
    }

    fn spawn_tasks(raft: Arc<Raft>, pool: TaskPool) {
        let raft_clone = raft.clone();
        let pool_clone = pool.clone();

        let delay_fn = move || {
            raft_clone.get_state()
                .sent_heartbeat_time
                .map(|t| SystemTime::now().duration_since(t).unwrap())
                .map(|d| {
                    if d > HEARTBEAT_INTERVAL {
                        Duration::from_millis(0)
                    } else {
                        HEARTBEAT_INTERVAL - d
                    }
                })
                .unwrap_or(HEARTBEAT_INTERVAL)
        };

        let raft_clone = raft.clone();

        pool.spawn_loop(delay_fn, move || {
            let raft_clone = raft_clone.clone();
            let pool_clone = pool_clone.clone();
            async move {
                pool_clone.spawn(async move {
                    raft_clone.send_heartbeat().await;
                })
            }
        });

        let raft_clone = raft.clone();
        let pool_clone = pool.clone();

        pool.spawn_loop(|| randomize_duration(ELECTION_TIMEOUT), move || {
            let raft_clone = raft_clone.clone();
            let pool_clone = pool_clone.clone();
            async move {
                pool_clone.spawn(async move {
                    raft_clone.check_leader_heartbeat().await;
                })
            }
        });
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
        let append_result = self.raft.append_command(command)?;
        let idx = append_result.idx;
        let term = append_result.term;
        let me = self.raft.get_me();

        let ctx = Arc::new(ReplicateEntriesCtx {
            commit_succeed: AtomicBool::new(false),
            replicate_count: AtomicUsize::new(0),
        });

        for peer in 0..self.raft.peers.len() {
            if peer == me {
                continue;
            }
            let raft = self.raft.clone();
            let append_result = append_result.clone();
            let ctx = ctx.clone();

            self.pool.spawn(async move {
                raft.replicate_entries(peer, ctx, append_result).await;
            });
        }

        Ok((idx, term))
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.raft.shared.lock().unwrap();
        raft.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.shared.lock().unwrap();
        raft.status == PeerStatus::Leader
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
        self.pool.shutdown();
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

#[derive(Clone)]
struct TaskPool {
    tasks: Arc<Mutex<Vec<TaskHandle>>>,
    pool: ThreadPool,
    stop: Arc<AtomicBool>,
}

struct TaskHandle {
    wait_ch: oneshot::Receiver<()>,
}

impl TaskPool {
    fn new(num_threads: usize) -> TaskPool {
        let pool = ThreadPoolBuilder::new()
            .pool_size(num_threads)
            .create()
            .unwrap();

        TaskPool {
            tasks: Arc::new(Mutex::new(Vec::new())),
            pool,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn spawn<F>(&self, future: F)
        where
            F: Future<Output=()> + Send + 'static,
    {
        self.pool.spawn_ok(future)
    }

    fn spawn_loop<D, F, T>(&self, delay_fn: D, task: T)
        where
            D: (Fn() -> Duration) + Send + 'static,
            F: Future + Send + 'static,
            T: (Fn() -> F) + Send + 'static
    {
        let mut tasks = self.tasks.lock().unwrap();
        let (wait_sender, wait_recv) = oneshot::channel();
        let stop = self.stop.clone();
        self.pool.spawn_ok(
            TaskPool::task_loop(delay_fn, stop, wait_sender, task)
        );
        let handle = TaskHandle {
            wait_ch: wait_recv,
        };
        tasks.push(handle);
    }

    fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);

        let mut tasks = self.tasks.lock().unwrap();

        while !tasks.is_empty() {
            let task = tasks.pop().unwrap();
            block_on(task.wait_ch);
        }
    }

    async fn task_loop<D, F, T>(
        delay_fn: D,
        stop: Arc<AtomicBool>,
        _wait_ch: oneshot::Sender<()>,
        task: T
    )
        where
            D: (Fn() -> Duration) + Send + 'static,
            F: Future + Send + 'static,
            T: (Fn() -> F) + Send + 'static
    {
        while stop.load(Ordering::SeqCst) != true {
            let duration = delay_fn();
            Delay::new(duration).await;
            let task_fut = task();
            task_fut.await;
        }
    }
}

fn randomize_duration(duration: Duration) -> Duration {
    let offset: u64 = rand::thread_rng().gen_range(0, 200);
    duration.add(Duration::from_millis(offset))
}

struct HeartbeatCtx {
    me: usize,
    failed_count: AtomicUsize,
    quorum: usize,
}

impl HeartbeatCtx {

    fn get_failed_count(&self) -> usize {
        self.failed_count.load(Ordering::SeqCst)
    }

    fn inc_failed_count(&self) {
        self.failed_count.fetch_add(1, Ordering::SeqCst);
    }
}

struct VoteCtx {
    me: usize,
    term: u64,
    approved_count: AtomicUsize,
    quorum: usize,
    approved: AtomicBool,
}

impl VoteCtx {
    fn is_approved(&self) -> bool {
        self.approved.load(Ordering::SeqCst)
    }

    fn set_approved(&self) {
        self.approved.store(true, Ordering::SeqCst);
    }

    fn get_approved_count(&self) -> usize {
        self.approved_count.load(Ordering::SeqCst)
    }

    fn inc_approved_count(&self) {
        self.approved_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct AppendLogResult {
    term: u64,
    idx: u64,
    prev_idx: u64,
    prev_term: u64,
    data: Vec<u8>,
}

struct ReplicateEntriesCtx {
    commit_succeed: AtomicBool,
    replicate_count: AtomicUsize,
}