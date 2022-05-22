use futures::channel::mpsc::UnboundedSender;
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
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(500);

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
            vote_progress: ProgressContainer::new(),
            log: Vec::new(),
            next_index: vec![0; peers_count],
            match_index: vec![0; peers_count],
            commit_index: 0,
            last_applied: 0,
            persister,
            event_ch,
        }
    }

    fn is_leader(&self) -> bool {
        self.status == PeerStatus::Leader
    }

    fn update_term(&mut self, term: u64) {
        if term != self.term {
            self.term = term;
            self.voted_for = Option::None;
            self.vote_progress.clear();
        }
    }

    fn tick_append_entries(&mut self) {
        if self.status != PeerStatus::Leader {
            return;
        }

        info!(
            "peer#{} - tick append_entries, term: {}, next_index: {:?}",
            self.me, self.term, self.next_index
        );

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

            self.send_event(RaftCoreEvent::AppendEntries {
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
    }

    fn handle_append_entries_reply(&mut self, peer: usize, peer_reply: AppendEntriesPeerReply) {
        let reply = peer_reply.reply;

        if reply.term > self.term {
            info!("peer#{} - append_entries reject, loosing leadership, peer:{}, reply.term: {}, my.term: {}",
                self.me, peer, reply.term, self.term
            );
            self.become_follower();
            self.update_term(reply.term);
            self.persist();
            return;
        }

        if !self.is_leader() || peer_reply.request_term != reply.term {
            info!("peer#{} - append_entries ignore, peer:{}, state: {:?}, req.term: {}, reply.term: {}, my.term: {}",
                self.me, peer, self.status, peer_reply.request_term, reply.term, self.term);
            return;
        }

        if !reply.success {
            let log: Vec<_> = self.log.iter().map(|e| (e.index, e.term)).collect();

            info!(
                "peer#{} - append_entries response conflict, peer:{}, conflict_idx: {}, conflict_term: {}, log: {:?}",
                self.me, peer, reply.conflicting_entry_idx, reply.conflicting_entry_term, log
            );

            if reply.conflicting_entry_term > 0 {
                let idx = self.find_last_for_term(reply.conflicting_entry_term);
                // info!("peer#{} - FIND LAST: {}", self.me, idx);
                self.next_index[peer] = if idx > 0 {
                    // std::cmp::min(self.log.len() as u64, idx + 1)
                    idx + 1
                } else {
                    // std::cmp::min(self.log.len() as u64, reply.conflicting_entry_idx)
                    reply.conflicting_entry_idx
                };
            } else {
                // self.next_index[peer] = std::cmp::min(self.log.len() as u64, reply.conflicting_entry_idx);
                self.next_index[peer] = reply.conflicting_entry_idx;
            }

            info!("peer#{} - SET NEXT_INDEX FOR PEER[{}] = {}", self.me, peer, self.next_index[peer]);
            // self.next_index[peer] = self.next_index[peer] - 1;

            // let has_conflict_entry = self
            //     .get_log_entry(reply.conflicting_entry_idx)
            //     .map(|e| e.term == reply.conflicting_entry_term)
            //     .unwrap_or(false);

            // self.next_index[peer] = if has_conflict_entry {
            //     self.find_last_for_term(reply.conflicting_entry_term)
            // } else {
            //     self.find_first_for_term(reply.conflicting_entry_term)
            // };

            return;
        }

        let next_idx = self.next_index[peer];
        self.next_index[peer] = next_idx + peer_reply.entries_len as u64;
        self.match_index[peer] = self.next_index[peer] - 1;

        info!("peer#{} - SET NEXT_INDEX FOR PEER[{}] = {}", self.me, peer, self.next_index[peer]);


        for i in self.commit_index + 1..=self.get_log_last_index() {
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

    fn check_leader_heartbeat(&mut self) {
        let now = SystemTime::now();

        if self.status == PeerStatus::Leader {
            return;
        }

        let duration_since = self
            .received_heartbeat_time
            .map(|t| now.duration_since(t).unwrap());

        let expired = duration_since
            .map(|d| d > HEARTBEAT_TIMEOUT)
            .unwrap_or(true);

        if !expired {
            return;
        }

        let dur_ms = duration_since.map(|d| d.as_millis()).unwrap_or(0);

        info!(
            "peer#{} - last heartbeat: {}, initiating new vote",
            self.me, dur_ms
        );

        self.become_candidate();
    }

    fn check_election_timeout(&mut self) {
        if self.status != PeerStatus::Candidate || !self.vote_progress.contains() {
            return;
        }

        let started_at = self.vote_progress.get().started_at;
        let duration_since = SystemTime::now().duration_since(started_at).unwrap();

        if duration_since > ELECTION_TIMEOUT {
            info!(
                "peer#{} - vote election timeout, initiating new vote",
                self.me
            );
            self.become_candidate();
        }
    }

    fn handle_request_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.term {
            info!(
                "peer#{} - request_vote, my term: {}, candidate_term: {}, become follower",
                self.me, self.term, args.term
            );
            self.become_follower();
            self.update_term(args.term);
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

    fn handle_append_entries_request(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.term {
            info!("peer#{} - FCUK YOU", self.me);
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

        let (ok, conflict_entry) = self.check_entry_conflict(&args);

        if ok {
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
                self.apply_commit(new_commit_index);
            }
        } else {
            let entries: Vec<_> = self.log.iter().map(|e| (e.index, e.term)).collect();
            info!(
                "peer#{} - append_entries conflict: {:?}, arg_prev_term: {}, arg_prev_idx: {}, logs: {:?}",
                self.me,
                conflict_entry.unwrap(),
                args.prev_log_term,
                args.prev_log_idx,
                entries
            );
        }

        self.persist();

        AppendEntriesReply {
            term: self.term,
            success: succeed,
            conflicting_entry_idx: conflict_entry.map(|e| e.index).unwrap_or(0),
            conflicting_entry_term: conflict_entry.map(|e| e.term).unwrap_or(0),
        }
    }

    fn replicate_log_entries(
        &mut self,
        entries: &[EntryItem],
        insert_idx: usize,
        new_entries_idx: usize,
    ) {
        let mut log_entry_idx = insert_idx;

        let mut indices_to_replicate = Vec::new();

        for i in new_entries_idx - 1..entries.len() {
            let entry = &entries[i];
            let log_entry = LogEntry {
                data: entry.data.clone(),
                term: entry.term,
                index: log_entry_idx as u64,
            };

            // info!("peer#{} - replicate log entry: {:?}", self.me, &log_entry);

            indices_to_replicate.push(log_entry_idx);

            if log_entry_idx > self.log.len() {
                self.log.push(log_entry);
            } else {
                self.log[log_entry_idx - 1] = log_entry;
            }

            log_entry_idx += 1;
        }

        info!(
            "peer#{} - replicate log entries: {:?}",
            self.me, indices_to_replicate
        );
    }

    fn check_entry_conflict(&self, args: &AppendEntriesArgs) -> (bool, Option<ConflictEntry>) {
        if args.prev_log_idx == 0 {
            return (true, Option::None);
        }

        let missing_entries = args.prev_log_idx > self.log.len() as u64;

        if missing_entries {
            return (
                false,
                Option::Some(ConflictEntry {
                    term: 0,
                    index: self.get_log_last_index() + 1,
                }),
            );
        }

        let prev_entry = self.get_log_entry(args.prev_log_idx).unwrap();

        if prev_entry.term != args.prev_log_term {
            let conflict_term = prev_entry.term;
            let mut conflict_idx = prev_entry.index;
            loop {
                let entry = self.get_log_entry(conflict_idx);
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
                    index: conflict_idx + 1,
                }),
            );
        }

        (true, Option::None)

        // let matches = args.prev_log_idx <= self.log.len() as u64
        //     && args.prev_log_term == self.get_log_entry(args.prev_log_idx).unwrap().term;

        // if matches {
        //     return (true, Option::None);
        // }

        // if  args.prev_log_idx <= self.log.len() as u64.
    }

    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) {
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

    fn find_last_for_term(&self, term: u64) -> u64 {
        let mut idx = 0;
        for entry in &self.log {
            if entry.term == term {
                idx = entry.index;
            }
        }
        idx

        // for i
    }

    fn become_leader(&mut self) {
        self.status = PeerStatus::Leader;

        let last_index = self.get_log_last_index();

        for i in 0..self.peers_count {
            self.match_index[i] = 0;
            self.next_index[i] = last_index + 1;
        }
        self.send_event(RaftCoreEvent::BecomeLeader);
    }

    fn become_follower(&mut self) {
        if self.status == PeerStatus::Follower {
            return;
        }
        self.status = PeerStatus::Follower;
        self.send_event(RaftCoreEvent::BecomeFollower);
    }

    fn become_candidate(&mut self) {
        let was_candidate = self.status == PeerStatus::Candidate;

        self.status = PeerStatus::Candidate;
        self.term += 1;
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
            "peer#{} - append_log, term: {}, index: {}, entry: {:?}",
            self.me, self.term, index, msg
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

    fn apply_commit(&mut self, commit_index: u64) {
        if self.commit_index > commit_index {
            return;
        }

        self.commit_index = commit_index;

        info!("peer#{} - apply_commit to index {}", self.me, commit_index);

        let mut states_to_apply = Vec::new();

        while self.commit_index > self.last_applied {
            let idx = self.last_applied + 1;

            let entry = self.get_log_entry(idx).unwrap();

            // info!("peer#{} - apply_state index: {}", self.me, idx);

            states_to_apply.push(idx);

            self.send_event(RaftCoreEvent::CommitMessage {
                index: idx,
                data: entry.data.clone(),
            });

            self.last_applied = idx;
        }

        info!("peer#{} - apply_states: {:?}", self.me, states_to_apply);
    }

    fn send_event(&self, event: RaftCoreEvent) {
        let res = self.event_ch.unbounded_send(event);
        match res {
            Ok(_) => {}
            Err(err) => warn!("peer#{} - send_event err: {}", self.me, err),
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
            me,
            inner: Mutex::new(InnerRaft::new(me, peers_count, persister, event_ch)),
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

    pub fn handle_append_entries_request(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut inner = self.inner.lock().unwrap();
        inner.handle_append_entries_request(args)
    }

    pub fn handle_append_entries_result(
        &self,
        peer: usize,
        result: Result<AppendEntriesPeerReply>,
    ) {
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
            }
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

    pub fn check_leader_heartbeat(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.check_leader_heartbeat()
    }

    pub fn check_election_timeout(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.check_election_timeout();
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

    pub fn get_debug_state(&self) -> DebugState {
        let inner = self.inner.lock().unwrap();
        DebugState {
            me: inner.me,
            term: inner.term,
            status: inner.status,
            vote_progress: inner.vote_progress.contains(),
            last_idx: inner.get_log_last_index(),
            last_applied: inner.last_applied,
            commit_idx: inner.commit_index,
        }
    }

    pub fn get_heartbeat_timeout(&self) -> Duration {
        randomize_duration(HEARTBEAT_TIMEOUT)
    }
}

pub struct AppendEntriesPeerReply {
    pub reply: AppendEntriesReply,
    pub request_term: u64,
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

    // fn check_expired(&mut self, term: u64) {
    //     if self.progress.is_none() {
    //         return;
    //     }

    //     let expired = self.progress.as_ref().unwrap().is_expired(term);

    //     if expired {
    //         self.progress = Option::None;
    //     }
    // }

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

    fn get(&self) -> &Progress {
        self.progress.as_ref().unwrap()
    }
}

struct Progress {
    started_at: SystemTime,
    term: u64,
    failed_count: usize,
    succeed_count: usize,
}

impl Progress {
    fn new(term: u64) -> Progress {
        Progress {
            started_at: SystemTime::now(),
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
