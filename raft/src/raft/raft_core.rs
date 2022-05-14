use rand::Rng;
use std::ops::Add;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use crate::proto::raftpb::*;
use crate::raft::errors::Error::NotLeader;
use crate::raft::errors::*;

use super::persister::Persister;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);

pub enum RaftCoreEvent {
    RequestVote { data: RequestVoteData },
    AppendEntries { data: AppendEntriesData },
    CommitMessage { index: u64, data: Vec<u8> },
}

#[derive(Clone, Copy)]
pub struct RequestVoteData {
    pub me: usize,
    pub term: u64,
    pub last_log_idx: u64,
    pub last_log_term: u64,
}

pub struct AppendEntriesData {
    pub peer: usize,
    pub me: usize,
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PeerStatus {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, PartialEq, Message)]
pub struct PersistState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(message, repeated, tag = "2")]
    pub log: Vec<LogEntry>,
    #[prost(message, tag = "3")]
    pub voted_for: Option<Vote>,
}

#[derive(Copy, Clone, PartialEq, Message)]
pub struct Vote {
    #[prost(uint64, tag = "1")]
    pub peer: u64,
    #[prost(uint64, tag = "2")]
    pub term: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(bytes, tag = "1")]
    pub data: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(uint64, tag = "3")]
    pub index: u64,
}

pub struct RaftCore {
    me: usize,
    inner: Mutex<InnerRaft>,
}

struct InnerRaft {
    me: usize,
    peers_count: usize,
    quorum: usize,
    term: u64,
    status: PeerStatus,
    voted_for: Option<Vote>,
    received_heartbeat_time: Option<SystemTime>,
    vote_progress: ProgressContainer,
    log: Vec<LogEntry>,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    commit_index: u64,
    last_applied: u64,
    persister: Box<dyn Persister>,
}

impl InnerRaft {
    fn new(me: usize, peers_count: usize, persister: Box<dyn Persister>) -> InnerRaft {
        let quorum = peers_count / 2 + 1;
        InnerRaft {
            me,
            term: 0,
            peers_count,
            quorum,
            status: PeerStatus::Follower,
            voted_for: None,
            received_heartbeat_time: None,
            vote_progress: ProgressContainer::new(),
            log: Vec::new(),
            next_index: vec![0; peers_count],
            match_index: vec![0; peers_count],
            commit_index: 0,
            last_applied: 0,
            persister,
        }
    }

    fn is_leader(&self) -> bool {
        self.status == PeerStatus::Leader
    }

    fn tick_append_entries(&mut self) -> Vec<RaftCoreEvent> {
        if self.status != PeerStatus::Leader {
            return Vec::with_capacity(0);
        }

        let mut events = Vec::with_capacity(self.peers_count);

        for i in 0..self.peers_count {
            if i == self.me {
                continue;
            }
            let next_index = self.next_index[i];
            let prev_log_index = next_index - 1;
            let prev_log_term = self
                .get_log_entry(prev_log_index)
                .map(|e| e.term)
                .unwrap_or(0);

            let entries = self.log[(next_index - 1) as usize..].to_vec();

            events.push(RaftCoreEvent::AppendEntries {
                data: AppendEntriesData {
                    peer: i,
                    me: self.me,
                    term: self.term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            });
        }

        info!(
            "peer#{} - tick append_entries, term: {}",
            self.me, self.term
        );

        events
    }

    fn handle_append_entries_reply(
        &mut self,
        peer: usize,
        peer_reply: AppendEntriesPeerReply,
    ) -> Vec<RaftCoreEvent> {
        let reply = peer_reply.reply;

        if !self.is_leader() {
            info!("peer#{} - append_entries ignore, not a leader, peer:{}, reply.term: {}, my.term: {}",
                self.me, peer, reply.term, self.term);

            return Vec::with_capacity(0);
        }

        if reply.term > self.term {
            info!("peer#{} - append_entries reject, loosing leadership, peer:{}, reply.term: {}, my.term: {}",
                self.me, peer, reply.term, self.term
            );
            self.become_follower();
            self.term = reply.term;
            self.persist();
            return Vec::with_capacity(0);
        }

        if !reply.success {
            self.next_index[peer] = self.next_index[peer] - 1;
            return Vec::with_capacity(0);
        }

        let next_idx = self.next_index[peer];
        self.next_index[peer] = next_idx + peer_reply.entries_len as u64;
        self.match_index[peer] = self.next_index[peer] - 1;

        let mut events = Vec::new();

        for i in self.commit_index + 1..=self.get_log_last_index() {
            let mut match_count = 1;

            for peer_idx in 0..self.peers_count {
                if self.match_index[peer_idx] >= i {
                    match_count += 1;
                }
            }
            if match_count >= self.quorum {
                for e in self.apply_commit(i) {
                    events.push(e);
                }
            }
        }

        events
    }

    fn check_leader_heartbeat(&mut self) -> Option<RaftCoreEvent> {
        let now = SystemTime::now();

        if self.status == PeerStatus::Leader {
            return None;
        }

        let duration_since = self
            .received_heartbeat_time
            .map(|t| now.duration_since(t).unwrap());

        let expired = duration_since
            .map(|d| d > HEARTBEAT_TIMEOUT)
            .unwrap_or(true);

        if !expired {
            return None;
        }

        let dur_ms = duration_since.map(|d| d.as_millis()).unwrap_or(0);

        info!(
            "peer#{} - last heartbeat: {}, initiating new vote",
            self.me, dur_ms
        );

        self.term += 1;
        self.status = PeerStatus::Candidate;
        self.vote_progress.start(Progress::new(self.term));

        // self-vote
        self.voted_for = Some(Vote {
            peer: self.me as u64,
            term: self.term,
        });
        self.vote_progress.get_mut().update(true);

        let last_entry = self.get_last_log_entry();
        let last_log_idx = last_entry.map(|e| e.index).unwrap_or(0);
        let last_log_term = last_entry.map(|e| e.term).unwrap_or(0);

        self.persist();

        Some(RaftCoreEvent::RequestVote {
            data: RequestVoteData {
                me: self.me,
                term: self.term,
                last_log_idx: last_log_idx,
                last_log_term: last_log_term,
            },
        })
    }

    fn handle_request_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.term {
            self.become_follower();
            self.term = args.term;
        }

        let last_entry = self.get_log_entry(self.log.len() as u64);
        let last_log_idx = last_entry.map(|e| e.index).unwrap_or(0);
        let last_log_term = last_entry.map(|e| e.term).unwrap_or(0);

        let matches_term = self.term == args.term;
        let already_voted = self
            .voted_for
            .map(|v| v.term >= args.term && v.peer != args.candidate_id as u64)
            .unwrap_or(false);
        let not_expired_log = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_idx);

        let mut vote_granted = false;

        if matches_term && !already_voted && not_expired_log {
            self.voted_for = Some(Vote {
                peer: args.candidate_id as u64,
                term: args.term,
            });
            self.reset_heartbeat_timeout();
            vote_granted = true;
        }

        info!(
            "peer#{} - request_vote, my term: {}, candidate_term: {}, candidate_id: {}, \
             last_log_idx: {}, last_log_term: {}, arg.last_log_idx: {}, arg.last_log_term: {}, \
             matches_term: {}, already_voted: {}, not_expired_log: {} ==  {}",
            self.me,
            self.term,
            args.term,
            args.candidate_id,
            last_log_idx,
            last_log_term,
            args.last_log_index,
            args.last_log_term,
            matches_term,
            already_voted,
            not_expired_log,
            if vote_granted { "approve" } else { "reject" }
        );

        self.persist();

        RequestVoteReply {
            term: args.term,
            vote_granted,
        }
    }

    fn handle_append_entries_request(
        &mut self,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Vec<RaftCoreEvent>) {
        if args.term < self.term {
            return (
                AppendEntriesReply {
                    term: self.term,
                    success: false,
                },
                Vec::with_capacity(0),
            );
        }

        self.term = args.term;
        self.reset_heartbeat_timeout();
        self.become_follower();

        let mut succeed = false;
        let mut events = Vec::new();

        if self.matches_log_term(&args) {
            succeed = true;

            let mut log_insert_idx = args.prev_log_idx + 1;
            let mut new_entries_idx = 1;
            loop {
                if log_insert_idx > self.log.len() as u64 || new_entries_idx > args.entries.len() {
                    break;
                }
                let mismatch_term = self.get_log_entry(log_insert_idx).unwrap().term
                    != args.entries[new_entries_idx - 1].term;

                if mismatch_term {
                    break;
                }
                log_insert_idx += 1;
                new_entries_idx += 1;
            }

            if new_entries_idx <= args.entries.len() {
                self.replicate_log_entries(&args.entries, log_insert_idx as usize, new_entries_idx);
            }

            let current_commit_idx = self.commit_index;

            if args.leader_commit > current_commit_idx {
                let new_commit_index = std::cmp::min(args.leader_commit, self.log.len() as u64);
                for e in self.apply_commit(new_commit_index) {
                    events.push(e);
                }
            }
        }
        self.persist();
        (
            AppendEntriesReply {
                term: self.term,
                success: succeed,
            },
            events,
        )
    }

    fn replicate_log_entries(
        &mut self,
        entries: &[EntryItem],
        insert_idx: usize,
        new_entries_idx: usize,
    ) {
        let mut log_entry_idx = insert_idx;

        for i in new_entries_idx - 1..entries.len() {
            let entry = &entries[i];
            let log_entry = LogEntry {
                data: entry.data.clone(),
                term: entry.term,
                index: log_entry_idx as u64,
            };

            info!("peer#{} - replicate log entry: {:?}", self.me, &log_entry);

            if log_entry_idx > self.log.len() {
                self.log.push(log_entry);
            } else {
                self.log[log_entry_idx - 1] = log_entry;
            }

            log_entry_idx += 1;
        }
    }

    fn matches_log_term(&self, args: &AppendEntriesArgs) -> bool {
        if args.prev_log_idx == 0 {
            return true;
        }

        args.prev_log_idx <= self.log.len() as u64
            && args.prev_log_term == self.get_log_entry(args.prev_log_idx).unwrap().term
    }

    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) {
        self.vote_progress.check_expired(reply.term);

        if !self.vote_progress.contains() {
            return;
        }

        let progress = self.vote_progress.get_mut();
        if !reply.vote_granted {
            info!(
                "peer#{} - received vote reject, my.term: {}, resp.term: {}",
                self.me, self.term, reply.term
            );
            progress.update(false);
            return;
        }

        progress.update(true);
        let count = progress.succeed_count();
        info!(
            "peer#{} - received vote approve, current: {}",
            self.me, count
        );

        if count >= self.quorum {
            info!(
                "peer#{} - received quorum approves, i am leader, term={}",
                self.me, self.term
            );
            self.become_leader();
            self.vote_progress.clear();
        }
    }

    fn become_leader(&mut self) {
        self.status = PeerStatus::Leader;

        let last_index = self.get_log_last_index();

        for i in 0..self.peers_count {
            self.match_index[i] = 0;
            self.next_index[i] = last_index + 1;
        }
    }

    fn become_follower(&mut self) {
        self.status = PeerStatus::Follower;
    }

    fn get_log_last_index(&self) -> u64 {
        if self.log.is_empty() {
            return 0;
        }
        self.log[self.log.len() - 1].index
    }

    fn get_last_log_entry(&self) -> Option<&LogEntry> {
        if self.log.is_empty() {
            return Option::None;
        }
        self.get_log_entry(self.log.len() as u64)
    }

    fn get_log_entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        let vec_idx = (index - 1) as usize;
        if vec_idx >= self.log.len() {
            return None;
        }
        Some(&self.log[vec_idx])
    }

    fn handle_request_vote_error(&mut self, err: Error) {
        self.vote_progress.check_expired(self.term);

        if !self.vote_progress.contains() {
            return;
        }
        let progress = self.vote_progress.get_mut();
        progress.update(false);
        warn!(
            "peer#{} - request_vote[term={}] error resp: {}",
            self.me,
            self.term,
            err.to_string()
        );
    }

    fn append_log<M>(&mut self, data: Vec<u8>, msg: &M) -> (u64, u64)
    where
        M: labcodec::Message,
    {
        let index = self.get_log_last_index() + 1;
        let entry = LogEntry {
            term: self.term,
            data,
            index,
        };
        info!(
            "peer#{} - append_log, term: {}, entry: {:?}",
            self.me, self.term, &entry
        );
        self.log.push(entry);
        self.persist();
        (index, self.term)
    }

    fn reset_heartbeat_timeout(&mut self) {
        self.received_heartbeat_time = Some(SystemTime::now());
    }

    fn persist(&self) {
        let state = PersistState {
            term: self.term,
            log: self.log.clone(),
            voted_for: self.voted_for.clone(),
        };

        let mut buf: Vec<u8> = vec![];
        labcodec::encode(&state, &mut buf).unwrap();

        self.persister.save_raft_state(buf);
    }

    fn restore(&mut self, data: &[u8]) {
        let state: PersistState = labcodec::decode(data).unwrap();
        self.term = state.term;
        self.log = state.log;
        self.voted_for = state.voted_for;
    }

    fn apply_commit(&mut self, commit_index: u64) -> Vec<RaftCoreEvent> {
        if self.commit_index > commit_index {
            return Vec::with_capacity(0);
        }

        self.commit_index = commit_index;

        info!("peer#{} - apply_commit to index {}", self.me, commit_index);

        let mut events = Vec::new();

        while self.commit_index > self.last_applied {
            let idx = self.last_applied + 1;

            let entry = self.get_log_entry(idx).unwrap();

            info!("peer#{} - apply_state entry: {:?}", self.me, entry);

            events.push(RaftCoreEvent::CommitMessage {
                index: idx,
                data: entry.data.clone(),
            });

            self.last_applied = idx;
        }

        events
    }
}

impl RaftCore {
    pub fn new(me: usize, peers_count: usize, persister: Box<dyn Persister>) -> RaftCore {
        RaftCore {
            me,
            inner: Mutex::new(InnerRaft::new(me, peers_count, persister)),
        }
    }

    // request vote
    pub fn handle_request_vote_request(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut inner = self.inner.lock().unwrap();
        inner.handle_request_vote_request(args)
    }

    pub fn handle_request_vote_result(&self, result: Result<RequestVoteReply>) {
        let mut inner = self.inner.lock().unwrap();
        match result {
            Ok(reply) => inner.handle_request_vote_reply(reply),
            Err(err) => inner.handle_request_vote_error(err),
        }
    }

    pub fn handle_append_entries_request(
        &self,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Vec<RaftCoreEvent>) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle_append_entries_request(args)
    }

    pub fn handle_append_entries_result(
        &self,
        peer: usize,
        result: Result<AppendEntriesPeerReply>,
    ) -> Vec<RaftCoreEvent> {
        let mut inner = self.inner.lock().unwrap();
        match result {
            Ok(reply) => inner.handle_append_entries_reply(peer, reply),
            Err(err) => {
                warn!(
                    "peer#{} - append_entries error, peer: {}, resp: {}",
                    self.me,
                    peer,
                    err.to_string()
                );
                Vec::with_capacity(0)
            }
        }
    }

    pub fn get_state(&self) -> (bool, u64) {
        let inner = self.inner.lock().unwrap();
        let is_leader = inner.status == PeerStatus::Leader;
        (is_leader, inner.term)
    }

    pub fn tick_append_entries(&self) -> Vec<RaftCoreEvent> {
        let mut inner = self.inner.lock().unwrap();
        inner.tick_append_entries()
    }

    pub fn check_leader_heartbeat(&self) -> Option<RaftCoreEvent> {
        let mut inner = self.inner.lock().unwrap();
        inner.check_leader_heartbeat()
    }

    pub fn append_log<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut inner = self.inner.lock().unwrap();

        if inner.status != PeerStatus::Leader {
            return Err(NotLeader);
        }

        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;

        Ok(inner.append_log(buf, command))
    }

    pub fn restore(&self, data: &[u8]) {
        let mut inner = self.inner.lock().unwrap();
        inner.restore(data);
    }
}

pub struct AppendEntriesPeerReply {
    pub reply: AppendEntriesReply,
    pub entries_len: usize,
}

struct ProgressContainer {
    progress: Option<Progress>,
}

impl ProgressContainer {
    fn new() -> ProgressContainer {
        ProgressContainer {
            progress: Option::None,
        }
    }

    fn check_expired(&mut self, term: u64) {
        if self.progress.is_none() {
            return;
        }

        let expired = self.progress.as_ref().unwrap().is_expired(term);

        if expired {
            self.progress = Option::None;
        }
    }

    fn contains(&self) -> bool {
        self.progress.is_some()
    }

    fn start(&mut self, progress: Progress) {
        self.progress = Option::Some(progress);
    }

    fn clear(&mut self) {
        self.progress = Option::None;
    }

    fn get_mut(&mut self) -> &mut Progress {
        self.progress.as_mut().unwrap()
    }
}

struct Progress {
    term: u64,
    failed_count: usize,
    succeed_count: usize,
}

impl Progress {
    fn new(term: u64) -> Progress {
        Progress {
            term,
            failed_count: 0,
            succeed_count: 0,
        }
    }

    fn succeed_count(&self) -> usize {
        self.succeed_count
    }

    fn is_expired(&self, term: u64) -> bool {
        self.term < term
    }

    fn update(&mut self, success: bool) {
        if !success {
            self.failed_count += 1;
        } else {
            self.succeed_count += 1;
        }
    }
}

pub fn get_heartbeat_timeout() -> Duration {
    randomize_duration(HEARTBEAT_TIMEOUT)
}

pub fn get_heartbeat_delay() -> Duration {
    HEARTBEAT_INTERVAL
}

fn randomize_duration(duration: Duration) -> Duration {
    let offset: u64 = rand::thread_rng().gen_range(0, 200);
    duration.add(Duration::from_millis(offset))
}
