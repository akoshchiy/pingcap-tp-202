use futures::channel::mpsc::UnboundedSender;
use rand::Rng;
use std::fmt::{Debug, Display};
use std::ops::Add;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use crate::proto::raftpb::*;
use crate::raft::errors::Error::NotLeader;
use crate::raft::errors::*;
use crate::rlog::{raft_log, State, Topic};

use super::persister::Persister;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(300);

#[derive(Clone, Debug)]
pub struct DebugState {
    pub me: usize,
    pub term: u64,
    pub status: PeerStatus,
    pub vote_progress: bool,
    pub last_idx: u64,
    pub commit_idx: u64,
    pub last_applied: u64,
}

pub enum RaftCoreEvent {
    AppendEntries {
        data: AppendEntriesData,
    },
    CommitMessage {
        index: u64,
        data: Vec<u8>,
    },
    BecomeFollower,
    BecomeLeader,
    BecomeCandidate {
        was_candidate: bool,
        data: RequestVoteData,
    },
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
    pub match_index_to_set: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    Init,
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
    pub voted_for: Option<u64>,
}

pub struct RaftCore {
    inner: Mutex<InnerRaft>,
}

struct InnerRaft {
    me: usize,
    peers_count: usize,
    quorum: usize,
    term: u64,
    status: PeerStatus,
    voted_for: Option<u64>,
    received_heartbeat_time: Option<SystemTime>,
    vote_progress: Option<VoteProgress>,
    log: LogContainer,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    commit_index: u64,
    last_applied: u64,
    persister: Box<dyn Persister>,
    event_ch: UnboundedSender<RaftCoreEvent>,
}

impl InnerRaft {
    fn new(
        me: usize,
        peers_count: usize,
        persister: Box<dyn Persister>,
        event_ch: UnboundedSender<RaftCoreEvent>,
    ) -> InnerRaft {
        let quorum = peers_count / 2 + 1;
        InnerRaft {
            me,
            term: 0,
            peers_count,
            quorum,
            status: PeerStatus::Init,
            voted_for: None,
            received_heartbeat_time: None,
            vote_progress: Option::None,
            log: LogContainer::new(Vec::new()),
            next_index: vec![0; peers_count],
            match_index: vec![0; peers_count],
            commit_index: 0,
            last_applied: 0,
            persister,
            event_ch,
        }
    }

    fn log(&self, topic: Topic, msg: &str) {
        let state = match self.status {
            PeerStatus::Init => State::Init,
            PeerStatus::Follower => State::Follower,
            PeerStatus::Candidate => State::Candidate,
            PeerStatus::Leader => State::Leader,
        };

        raft_log(topic, self.me, state, self.term, msg);
    }

    fn update_term(&mut self, term: u64) {
        if term == self.term {
            return;
        }

        self.term = term;
        self.voted_for = Option::None;
        self.vote_progress = Option::None;

        self.log(Topic::State, format!("update term={}", term).as_str());
    }

    fn tick_append_entries(&mut self) {
        if self.status != PeerStatus::Leader {
            return;
        }

        for i in 0..self.peers_count {
            if i == self.me {
                continue;
            }
            let next_index = self.next_index[i];
            let prev_log_index = next_index - 1;
            let prev_log_term = self
                .log
                .get_entry(prev_log_index)
                .map(|e| e.term)
                .unwrap_or(0);

            let entries = self.log.copy_from(next_index);
            let match_index_to_set = prev_log_index + entries.len() as u64;

            self.send_event(RaftCoreEvent::AppendEntries {
                data: AppendEntriesData {
                    peer: i,
                    me: self.me,
                    term: self.term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                    match_index_to_set,
                },
            });
        }
    }

    fn handle_append_entries_reply(
        &mut self,
        from_peer: usize,
        request_term: u64,
        match_idx_to_set: u64,
        reply: AppendEntriesReply,
    ) {
        self.log(
            Topic::Rpc,
            format!(
                "recv APPEND_ENTRIES_REPLY[S{},T={},s={},c_t={},c_idx={}](RT={},m_idx={})",
                from_peer,
                reply.term,
                reply.success,
                reply.conflicting_entry_term,
                reply.conflicting_entry_idx,
                request_term,
                match_idx_to_set
            )
            .as_str(),
        );

        if reply.term > self.term {
            self.become_follower();
            self.update_term(reply.term);
            self.persist();
            return;
        }

        if request_term != self.term {
            return;
        }

        if !reply.success {
            if reply.conflicting_entry_term > 0 {
                let idx = self
                    .log
                    .find_last_idx_for_term(reply.conflicting_entry_term);
                self.next_index[from_peer] = if idx > 0 {
                    idx + 1
                } else {
                    reply.conflicting_entry_idx
                };
            } else {
                self.next_index[from_peer] = reply.conflicting_entry_idx;
            }
            return;
        }

        self.next_index[from_peer] = match_idx_to_set + 1;
        self.match_index[from_peer] = match_idx_to_set;

        for i in self.commit_index + 1..=self.log.get_last_index() {
            let entry_term = self.log.get_entry(i).map(|e| e.term).unwrap_or(0);

            if entry_term != self.term {
                continue;
            }

            let mut match_count = 1;

            for peer_idx in 0..self.peers_count {
                if self.match_index[peer_idx] >= i {
                    match_count += 1;
                }
            }
            if match_count >= self.quorum {
                self.apply_commit(i);
            }
        }
    }

    fn check_election_timeout(&mut self, timeout: Duration) {
        let now = SystemTime::now();

        if self.status == PeerStatus::Leader {
            return;
        }

        let duration_since = self
            .received_heartbeat_time
            .map(|t| now.duration_since(t).unwrap());

        let expired = duration_since.map(|d| d > timeout).unwrap_or(true);

        if !expired {
            return;
        }

        let dur_ms = duration_since.map(|d| d.as_millis()).unwrap_or(0);

        self.log(
            Topic::Vote,
            format!("election timeout: {}, initiating new vote", dur_ms).as_str(),
        );

        self.become_candidate();
    }

    fn handle_request_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        self.log(
            Topic::Rpc,
            format!(
                "recv req REQUEST_VOTE[S{},T={},l_term={},l_idx={}]",
                args.candidate_id, args.term, args.last_log_term, args.last_log_index
            )
            .as_str(),
        );

        if args.term > self.term {
            self.become_follower();
            self.update_term(args.term);
        }

        let last_log_idx = self.log.get_last_index();
        let last_log_term = self.log.get_last_entry().map(|e| e.term).unwrap_or(0);

        let matches_term = self.term == args.term;
        let already_voted = self
            .voted_for
            .map(|v| v != args.candidate_id as u64)
            .unwrap_or(false);
        let not_expired_log = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_idx);

        let mut vote_granted = false;

        if matches_term && !already_voted && not_expired_log {
            self.voted_for = Some(args.candidate_id as u64);
            self.reset_heartbeat_timeout();
            vote_granted = true;
        }

        self.log(
            Topic::Vote,
            format!(
                "{} to S{}",
                if vote_granted { "approve" } else { "reject" },
                args.candidate_id
            )
            .as_str(),
        );

        self.persist();

        RequestVoteReply {
            term: args.term,
            vote_granted,
        }
    }

    fn handle_append_entries_request(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        self.log(
            Topic::Rpc,
            format!(
                "recv APPEND_ENTRIES_ARGS[S{},T={},p_idx={},p_term={},l_commit={},e={}]",
                args.leader_id,
                args.term,
                args.prev_log_idx,
                args.prev_log_term,
                args.leader_commit,
                args.entries.len()
            )
            .as_str(),
        );

        if args.term < self.term {
            return AppendEntriesReply {
                term: self.term,
                success: false,
                conflicting_entry_idx: 0,
                conflicting_entry_term: 0,
            };
        }

        self.update_term(args.term);
        self.reset_heartbeat_timeout();
        self.become_follower();

        let mut succeed = false;

        let (ok, conflict_entry) = self.log.check_entry_conflict(&args);

        if ok {
            succeed = true;

            self.replicate_log_entries(&args);

            let current_commit_idx = self.commit_index;

            if args.leader_commit > current_commit_idx {
                let new_commit_index = std::cmp::min(args.leader_commit, self.log.len());
                self.apply_commit(new_commit_index);
            }
        }

        self.persist();

        AppendEntriesReply {
            term: self.term,
            success: succeed,
            conflicting_entry_idx: conflict_entry.map(|e| e.index).unwrap_or(0),
            conflicting_entry_term: conflict_entry.map(|e| e.term).unwrap_or(0),
        }
    }

    fn replicate_log_entries(&mut self, args: &AppendEntriesArgs) {
        let mut log_insert_idx = args.prev_log_idx + 1;
        let mut from_args_idx = 0;

        let mut conflict_or_missing = false;

        for i in 0..args.entries.len() {
            let log_idx = args.prev_log_idx + 1 + i as u64;

            let arg_entry = &args.entries[i];
            let log_entry = self.log.get_entry(log_idx);

            conflict_or_missing = log_entry.map(|e| e.term != arg_entry.term).unwrap_or(true);

            if conflict_or_missing {
                break;
            }

            log_insert_idx += 1;
            from_args_idx += 1;
        }

        // nothing to replicate,
        if !conflict_or_missing {
            return;
        }

        self.log.truncate(log_insert_idx - 1);

        let entries: Vec<LogEntry> = args.entries[from_args_idx..]
            .iter()
            .map(|e| LogEntry {
                data: e.data.clone(),
                string_data: e.string_data.clone(),
                term: e.term,
                from_leader: e.from_leader,
            })
            .collect();

        let range = LogEntryRange {
            start_index: log_insert_idx,
            entries,
        };

        self.log(
            Topic::Log,
            format!("replicate entries: {}", &range).as_str(),
        );
        self.log.push_range(range);
    }

    fn handle_request_vote_reply(
        &mut self,
        from_peer: usize,
        request_term: u64,
        reply: RequestVoteReply,
    ) {
        self.log(
            Topic::Rpc,
            format!(
                "recv REQUEST_VOTE_REPLY[S{}, T={}, v={}] (RT={})",
                from_peer, reply.term, reply.vote_granted, request_term
            )
            .as_str(),
        );

        if self.status != PeerStatus::Candidate || request_term != self.term {
            self.log(Topic::Vote, "ignoring vote reply");
            return;
        }

        let progress = self.vote_progress.as_mut().unwrap();

        if !reply.vote_granted {
            progress.update(false);
            return;
        }

        progress.update(true);
        let count = progress.succeed_count();

        if count >= self.quorum {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.status = PeerStatus::Leader;

        let last_index = self.log.get_last_index();

        for i in 0..self.peers_count {
            self.match_index[i] = 0;
            self.next_index[i] = last_index + 1;
        }
        self.voted_for = Option::None;
        self.vote_progress = Option::None;

        self.log(Topic::State, "become leader");

        self.send_event(RaftCoreEvent::BecomeLeader);
    }

    fn become_follower(&mut self) {
        if self.status == PeerStatus::Follower {
            return;
        }

        self.status = PeerStatus::Follower;
        self.voted_for = Option::None;
        self.vote_progress = Option::None;

        self.log(Topic::State, "become follower");

        self.send_event(RaftCoreEvent::BecomeFollower);
    }

    fn become_candidate(&mut self) {
        let was_candidate = self.status == PeerStatus::Candidate;

        self.status = PeerStatus::Candidate;
        self.term += 1;
        self.vote_progress = Option::Some(VoteProgress::new());

        self.log(Topic::State, "become candidate");

        // self-vote
        self.voted_for = Some(self.me as u64);
        self.vote_progress.as_mut().unwrap().update(true);

        let last_log_idx = self.log.get_last_index();
        let last_log_term = self.log.get_last_entry().map(|e| e.term).unwrap_or(0);

        self.reset_heartbeat_timeout();

        self.persist();

        self.send_event(RaftCoreEvent::BecomeCandidate {
            was_candidate,
            data: RequestVoteData {
                me: self.me,
                term: self.term,
                last_log_idx: last_log_idx,
                last_log_term: last_log_term,
            },
        });
    }

    fn handle_request_vote_error(&mut self, from_peer: usize, request_term: u64, err: Error) {
        self.log(
            Topic::RpcError,
            format!(
                "REQUEST_VOTE[S{}, RT={}], error: {}",
                from_peer, request_term, err
            )
            .as_str(),
        );

        if self.status != PeerStatus::Candidate {
            return;
        }
        let progress = self.vote_progress.as_mut().unwrap();
        progress.update(false);
    }

    fn append_log<M>(&mut self, data: Vec<u8>, msg: &M) -> (u64, u64)
    where
        M: labcodec::Message,
    {
        let entry = LogEntry {
            term: self.term,
            data,
            string_data: format!("{:?}", msg),
            from_leader: self.me as u64,
        };
        self.log.append(entry);
        let index = self.log.len();

        self.log(
            Topic::Client,
            format!("append_log, idx: {}, entry: {:?}", index, msg).as_str(),
        );
        self.persist();
        (index, self.term)
    }

    fn reset_heartbeat_timeout(&mut self) {
        self.received_heartbeat_time = Some(SystemTime::now());
    }

    fn persist(&self) {
        let state = PersistState {
            term: self.term,
            log: self.log.logs.clone(),
            voted_for: self.voted_for.clone(),
        };

        let mut buf: Vec<u8> = vec![];
        labcodec::encode(&state, &mut buf).unwrap();

        self.persister.save_raft_state(buf);

        self.log(Topic::Persist, "save state");
    }

    fn restore(&mut self, data: &[u8]) {
        let state: PersistState = labcodec::decode(data).unwrap();
        self.term = state.term;
        self.log = LogContainer::new(state.log);
        self.voted_for = state.voted_for;

        self.log(Topic::Persist, "restore state");
    }

    fn apply_commit(&mut self, commit_index: u64) {
        if self.commit_index > commit_index {
            return;
        }

        self.commit_index = commit_index;

        let mut states_to_apply = Vec::new();

        while self.commit_index > self.last_applied {
            let idx = self.last_applied + 1;

            let entry = self.log.get_entry(idx).unwrap();

            states_to_apply.push(idx);

            self.send_event(RaftCoreEvent::CommitMessage {
                index: idx,
                data: entry.data.clone(),
            });

            self.last_applied = idx;
        }

        if !states_to_apply.is_empty() {
            self.log(
                Topic::Log,
                format!(
                    "commit: [{}..{}]",
                    states_to_apply[0],
                    states_to_apply[states_to_apply.len() - 1]
                )
                .as_str(),
            );
        }
    }

    fn send_event(&self, event: RaftCoreEvent) {
        let res = self.event_ch.unbounded_send(event);
        match res {
            Ok(_) => {}
            Err(err) => {
                self.log(Topic::Warn, format!("send_event err: {}", err).as_str());
            }
        }
    }
}

impl RaftCore {
    pub fn new(
        me: usize,
        peers_count: usize,
        persister: Box<dyn Persister>,
        event_ch: UnboundedSender<RaftCoreEvent>,
    ) -> RaftCore {
        RaftCore {
            inner: Mutex::new(InnerRaft::new(me, peers_count, persister, event_ch)),
        }
    }

    pub fn log(&self, topic: Topic, msg: &str) {
        let inner = self.inner.lock().unwrap();
        inner.log(topic, msg);
    }

    // request vote
    pub fn handle_request_vote_request(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut inner = self.inner.lock().unwrap();
        let reply = inner.handle_request_vote_request(args);

        inner.log(
            Topic::Rpc,
            format!(
                "send REQUEST_VOTE_REPLY[T={},v={}]",
                reply.term, reply.vote_granted
            )
            .as_str(),
        );

        reply
    }

    pub fn handle_request_vote_result(
        &self,
        request_term: u64,
        from_peer: usize,
        result: Result<RequestVoteReply>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        match result {
            Ok(reply) => inner.handle_request_vote_reply(from_peer, request_term, reply),
            Err(err) => inner.handle_request_vote_error(from_peer, request_term, err),
        }
    }

    pub fn handle_append_entries_request(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut inner = self.inner.lock().unwrap();
        let reply = inner.handle_append_entries_request(args);

        inner.log(
            Topic::Rpc,
            format!(
                "send APPEND_ENTRIES_REPLY[T={},s={},c_t={},c_idx={}]",
                reply.term,
                reply.success,
                reply.conflicting_entry_term,
                reply.conflicting_entry_idx
            )
            .as_str(),
        );

        reply
    }

    pub fn handle_append_entries_result(
        &self,
        from_peer: usize,
        request_term: u64,
        match_idx_to_set: u64,
        reply_result: Result<AppendEntriesReply>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        match reply_result {
            Ok(reply) => {
                inner.handle_append_entries_reply(from_peer, request_term, match_idx_to_set, reply)
            }
            Err(err) => inner.log(
                Topic::RpcError,
                format!(
                    "APPEND_ENTRIES[S{},RT={},m_idx={}]",
                    from_peer, request_term, match_idx_to_set
                )
                .as_str(),
            ),
        }
    }

    pub fn get_peer_status(&self) -> PeerStatus {
        let inner = self.inner.lock().unwrap();
        inner.status
    }

    pub fn get_state(&self) -> (bool, u64) {
        let inner = self.inner.lock().unwrap();
        let is_leader = inner.status == PeerStatus::Leader;
        (is_leader, inner.term)
    }

    pub fn tick_append_entries(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.tick_append_entries()
    }

    pub fn check_election_timeout(&self, timeout: Duration) {
        let mut inner = self.inner.lock().unwrap();
        inner.check_election_timeout(timeout);
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

    pub fn become_follower(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.become_follower();
    }
}

struct VoteProgress {
    failed_count: usize,
    succeed_count: usize,
}

impl VoteProgress {
    fn new() -> VoteProgress {
        VoteProgress {
            failed_count: 0,
            succeed_count: 0,
        }
    }

    fn succeed_count(&self) -> usize {
        self.succeed_count
    }

    fn update(&mut self, success: bool) {
        if !success {
            self.failed_count += 1;
        } else {
            self.succeed_count += 1;
        }
    }
}

pub fn get_heartbeat_delay() -> Duration {
    HEARTBEAT_INTERVAL
}

pub fn get_election_timeout() -> Duration {
    randomize_duration(ELECTION_TIMEOUT)
}

fn randomize_duration(duration: Duration) -> Duration {
    let offset: u64 = rand::thread_rng().gen_range(0, 200);
    duration.add(Duration::from_millis(offset))
}

#[derive(Clone, Copy, Debug)]
struct ConflictEntry {
    term: u64,
    index: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    #[prost(bytes, tag = "1")]
    pub data: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(string, tag = "3")]
    pub string_data: String,
    #[prost(uint64, tag = "4")]
    pub from_leader: u64,
}

struct LogContainer {
    logs: Vec<LogEntry>,
}

impl Display for LogContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // f.write_str("[")?;
        // for i in 0..self.logs.len() {
        //     let entry = &self.logs[i];
        //     write!(f, "[")?;
        //     write!(f, "{},", i + 1)?;
        //     write!(f, "{},", entry.term)?;
        //     write!(f, "{}]", entry.string_data)?;

        //     if i != self.logs.len() - 1 {
        //         write!(f, ",")?;
        //     }
        // }
        // f.write_str("]")
        write!(f, "[1..{}]", self.len())?;
        Ok(())
    }
}

impl LogContainer {
    fn new(logs: Vec<LogEntry>) -> LogContainer {
        LogContainer { logs }
    }

    fn len(&self) -> u64 {
        self.logs.len() as u64
    }

    fn copy_from(&self, start_index: u64) -> Vec<LogEntry> {
        self.logs[(start_index - 1) as usize..].to_vec()
    }

    fn get_last_index(&self) -> u64 {
        self.logs.len() as u64
    }

    fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        let vec_idx = (index - 1) as usize;
        if vec_idx >= self.logs.len() {
            return None;
        }
        Some(&self.logs[vec_idx])
    }

    fn get_last_entry(&self) -> Option<&LogEntry> {
        self.get_entry(self.get_last_index())
    }

    fn find_last_idx_for_term(&self, term: u64) -> u64 {
        for idx in (0..self.logs.len()).rev() {
            let entry = &self.logs[idx];
            if entry.term == term {
                return (idx + 1) as u64;
            }
        }
        return 0;
    }

    fn push(&mut self, index: u64, entry: LogEntry) {
        let len = self.len();
        if index <= len {
            let log_idx = (index - 1) as usize;
            self.logs[log_idx] = entry;
            return;
        }

        if index != len + 1 {
            panic!(
                "pushing gap in logs, index: {}, len: {}, entry: {:?}",
                index, len, entry
            )
        }

        self.logs.push(entry);
    }

    fn push_range(&mut self, range: LogEntryRange) {
        let mut idx = range.start_index;
        for entry in range.entries {
            self.push(idx, entry);
            idx += 1;
        }
    }

    fn truncate(&mut self, index: u64) {
        self.logs.truncate(index as usize);
    }

    fn append(&mut self, entry: LogEntry) {
        self.push(self.get_last_index() + 1, entry);
    }

    fn check_entry_conflict(&self, args: &AppendEntriesArgs) -> (bool, Option<ConflictEntry>) {
        if args.prev_log_idx == 0 {
            return (true, Option::None);
        }

        let missing_entries = args.prev_log_idx > self.len();

        if missing_entries {
            let len = self.len();
            return (
                false,
                Option::Some(ConflictEntry {
                    term: 0,
                    index: if len > 0 { len } else { 1 },
                }),
            );
        }

        let prev_entry = self.get_entry(args.prev_log_idx).unwrap();

        if prev_entry.term != args.prev_log_term {
            let conflict_term = prev_entry.term;
            let mut conflict_idx = args.prev_log_idx;
            loop {
                if conflict_idx == 0 {
                    break;
                }
                let entry = self.get_entry(conflict_idx - 1);
                if entry.is_none() {
                    break;
                }
                if conflict_term != entry.unwrap().term {
                    break;
                }
                conflict_idx -= 1;
            }
            return (
                false,
                Option::Some(ConflictEntry {
                    term: conflict_term,
                    index: conflict_idx,
                }),
            );
        }

        (true, Option::None)
    }
}

struct LogEntryRange {
    start_index: u64,
    entries: Vec<LogEntry>,
}

impl Display for LogEntryRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // f.write_str("[")?;
        // for i in 0..self.entries.len() {
        //     let entry = &self.entries[i];
        //     write!(f, "[")?;
        //     write!(f, "{},", i as u64 + self.start_index)?;
        //     write!(f, "{},", entry.term)?;
        //     write!(f, "{}]", entry.string_data)?;
        //     if i != self.entries.len() - 1 {
        //         write!(f, ",")?;
        //     }
        // }
        // f.write_str("]")?;
        // let first = self.entries[0]
        // write!(f, "[[idx={},term={}]..[idx={},term={}]]",)
        write!(
            f,
            "[{}..{}]",
            self.start_index,
            self.start_index + self.entries.len() as u64
        )?;
        Ok(())
    }
}
