use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;

use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::executor::{ThreadPool, ThreadPoolBuilder};
use futures::StreamExt;
use futures_timer::Delay;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
mod raft_core;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::raft_core::{PeerStatus, RequestVoteData, get_election_timeout};
use crate::proto::raftpb::*;
use crate::raft::raft_core::{
    get_heartbeat_delay, get_heartbeat_timeout, AppendEntriesData, AppendEntriesPeerReply,
    RaftCore, RaftCoreEvent,
};

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
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
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
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // this peer's index into peers[]
    me: usize,
    // state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    raft_core: Arc<RaftCore>,
    stop: Arc<AtomicBool>,
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

        // let peer = &peers[me];
        let (sender, mut receiver) = unbounded::<RaftCoreEvent>();
        let stop = Arc::new(AtomicBool::new(false));
        let pool = Arc::new(
            ThreadPoolBuilder::new().create().unwrap()
        );

        let raft_core = Arc::new(RaftCore::new(me, peers.len(), persister, sender));
        let stop_clone = stop.clone();
        let pool_clone = pool.clone();

        let event_handler = Arc::new(RaftEventHandler::new(
            peers.clone(),
            raft_core.clone(),
            apply_ch,
            stop_clone,
            pool_clone
        ));

        // let raft_core_clone = raft_core.clone();
        // let stop_clone = stop.clone();

        // peer.spawn(async move {
        //     while !stop_clone.load(SeqCst) {
        //         Delay::new(get_heartbeat_timeout()).await;
        //         raft_core_clone.check_leader_heartbeat();
        //     }
        // });

        // let raft_core_clone = raft_core.clone();
        // let stop_clone = stop.clone();

        // peer.spawn(async move {
        //     while !stop_clone.load(SeqCst) {
        //         Delay::new(get_heartbeat_delay()).await;
        //         raft_core_clone.tick_append_entries();
        //     }
        // });

        // let event_handler_clone = event_handler.clone();
        let stop_clone = stop.clone();

        pool.spawn_ok(async move {
            while !stop_clone.load(SeqCst) {
                let msg = receiver.next().await.unwrap();
                event_handler.handle(msg);
            }
        });

        raft_core.restore(&raft_state);
        raft_core.become_follower();

        Raft {
            peers,
            me,
            raft_core,
            stop,
        }

        // initialize from state persisted before a crash
        // rf.restore(&raft_state);
        // rf
    }

    fn kill(&self) {
        self.stop.store(true, SeqCst);
    }

    fn term(&self) -> u64 {
        self.raft_core.get_state().1
    }

    fn is_leader(&self) -> bool {
        self.raft_core.get_state().0
    }

    fn get_state(&self) -> State {
        let state = self.raft_core.get_state();
        State {
            term: state.1,
            is_leader: state.0,
        }
    }

    fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteReply {
        self.raft_core.handle_request_vote_request(args)
    }

    fn handle_append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        self.raft_core.handle_append_entries_request(args)
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
        self.raft_core.restore(data);
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
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        crate::your_code_here((server, args, tx, rx))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // let index = 0;
        // let term = 0;
        // let is_leader = true;
        // let mut buf = vec![];
        // labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        //
        // if is_leader {
        //     Ok((index, term))
        // } else {
        //     Err(Error::NotLeader)
        // }
        self.raft_core.append_log(command)
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

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        let _ = self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.me;
        let _ = &self.peers;
    }
}

struct RaftEventHandler {
    peers: Vec<RaftClient>,
    raft_core: Arc<RaftCore>,
    apply_ch: UnboundedSender<ApplyMsg>,
    stop: Arc<AtomicBool>,
    pool: Arc<ThreadPool>,
}

impl RaftEventHandler {
    fn new(
        peers: Vec<RaftClient>,
        raft_core: Arc<RaftCore>,
        apply_ch: UnboundedSender<ApplyMsg>,
        stop: Arc<AtomicBool>,
        pool: Arc<ThreadPool>,
    ) -> RaftEventHandler {
        RaftEventHandler {
            peers,
            raft_core,
            apply_ch,
            stop,
            pool,
        }
    }

    fn handle(&self, event: RaftCoreEvent) {
        match event {
            RaftCoreEvent::AppendEntries { data } => self.on_append_entries(data),
            RaftCoreEvent::CommitMessage { index, data } => self.on_commit_message(index, data),
            RaftCoreEvent::BecomeCandidate { data } => self.on_become_candidate(data),
            RaftCoreEvent::BecomeFollower => self.on_become_follower(),
            RaftCoreEvent::BecomeLeader => self.on_become_leader(),
        }
    }

    fn on_become_leader(&self) {
        let stop = self.stop.clone();
        let raft = self.raft_core.clone();

        self.pool.spawn_ok(async move {
            loop {
                let stop = stop.load(SeqCst);
                let is_leader = raft.get_peer_status() == PeerStatus::Leader;
                if stop || !is_leader {
                    return;
                }
                Delay::new(get_heartbeat_delay()).await;
                raft.tick_append_entries();
            }
        });
    }

    fn on_become_candidate(&self, data: RequestVoteData) {
        let stop = self.stop.clone();
        let raft = self.raft_core.clone();

        self.pool.spawn_ok(async move {
            loop {
                let stop = stop.load(SeqCst);
                let is_candidate = raft.get_peer_status() == PeerStatus::Candidate;
                if stop || !is_candidate {
                    return;
                }
                Delay::new(get_election_timeout()).await;
                raft.check_election_timeout();
            }
        });

        self.send_request_vote(data);
    }

    fn send_request_vote(&self, data: RequestVoteData) {
        for i in 0..self.peers.len() {
            if i == data.me {
                continue;
            }

            let peer = &self.peers[i];
            let raft_clone = self.raft_core.clone();
            let peer_clone = peer.clone();

            self.pool.spawn_ok(async move {
                let args = RequestVoteArgs {
                    term: data.term,
                    candidate_id: data.me as u32,
                    last_log_term: data.last_log_term,
                    last_log_index: data.last_log_idx,
                };
                let result = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
                raft_clone.handle_request_vote_result(result);
            });
        }
    }

    fn on_become_follower(&self) {
        let stop = self.stop.clone();
        let raft = self.raft_core.clone();

        self.pool.spawn_ok(async move {
            loop {
                let stop = stop.load(SeqCst);
                let is_follower = raft.get_peer_status() == PeerStatus::Follower;
                if stop || !is_follower {
                    return;
                }
                Delay::new(get_heartbeat_timeout()).await;
                raft.check_leader_heartbeat();
            }
        });
    }

    fn on_commit_message(&self, index: u64, data: Vec<u8>) {
        self.apply_ch
            .unbounded_send(ApplyMsg::Command { data, index })
            .unwrap();
    }

    fn on_append_entries(&self, data: AppendEntriesData) {
        let peer = &self.peers[data.peer];
        let raft_clone = self.raft_core.clone();
        let peer_clone = peer.clone();

        self.pool.spawn_ok(async move {
            let entries: Vec<_> = data
                .entries
                .into_iter()
                .map(|entry| EntryItem {
                    term: entry.term,
                    index: entry.term,
                    data: entry.data,
                })
                .collect();

            let entries_len = entries.len();

            let args = AppendEntriesArgs {
                term: data.term,
                leader_id: data.me as u32,
                entries,
                leader_commit: data.leader_commit,
                prev_log_idx: data.prev_log_index,
                prev_log_term: data.prev_log_term,
            };

            let result = peer_clone
                .append_entries(&args)
                .await
                .map_err(Error::Rpc)
                .map(|r| AppendEntriesPeerReply {
                    reply: r,
                    entries_len,
                });

            raft_clone.handle_append_entries_result(data.peer, result);
        });
    }
}

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
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        Node {
            raft: Arc::new(raft),
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
        // crate::your_code_here(command)
        self.raft.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // crate::your_code_here(())
        self.raft.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // crate::your_code_here(())
        self.raft.is_leader()
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
        self.raft.kill();
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
        Ok(self.raft.handle_request_vote(args))
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        Ok(self.raft.handle_append_entries(args))
    }
}
