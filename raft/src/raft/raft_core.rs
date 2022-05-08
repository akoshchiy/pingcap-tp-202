use rand::Rng;
use std::ops::Add;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use crate::proto::raftpb::*;
use crate::raft::errors::Error::NotLeader;
use crate::raft::errors::*;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);

pub enum RaftCoreEvent {
    RequestVote { me: usize, term: u64 },
    AppendEntries(AppendEntriesData),
    CommitMessage { index: u64, data: Vec<u8> },
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

#[derive(Copy, Clone, Debug)]
struct Vote {
    peer: usize,
    term: u64,
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub data: Vec<u8>,
    pub term: u64,
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
}

impl InnerRaft {
    fn new(me: usize, peers_count: usize) -> InnerRaft {
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

            events.push(RaftCoreEvent::AppendEntries(AppendEntriesData {
                peer: i,
                me: self.me,
                term: self.term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            }));
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
            let matches_term = self
                .get_log_entry(i)
                .map(|e| e.term == self.term)
                .unwrap_or(false);

            if matches_term {
                let mut match_count = 1;

                for peer_idx in 0..self.peers_count {
                    if self.match_index[peer_idx] >= i {
                        match_count += 1;
                    }
                }

                if match_count >= self.quorum {
                    self.commit_index = i;

                    events.push(RaftCoreEvent::CommitMessage {
                        index: i,
                        data: self.get_log_entry(i).unwrap().data.clone(),
                    });
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
            peer: self.me,
            term: self.term,
        });
        self.vote_progress.get_mut().update(true);

        Some(RaftCoreEvent::RequestVote {
            term: self.term,
            me: self.me,
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
        let already_voted = self.voted_for
            .map(|v| v.term >= args.term && v.peer != args.candidate_id as usize)
            .unwrap_or(false);
        let not_expired_log = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_idx);


        let mut vote_granted = false;

        if matches_term && !already_voted && not_expired_log {
            self.voted_for = Some(Vote {
                peer: args.candidate_id as usize,
                term: args.term,
            });
            self.reset_heartbeat_timeout();
            vote_granted = true;
        }

        info!(
            "peer#{} - request_vote, my term: {}, candidate_term: {}, candidate_id: {}, {}",
            self.me,
            self.term,
            args.term,
            args.candidate_id,
            if vote_granted { "approve" } else { "reject" }
        );

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
                self.commit_index = std::cmp::min(args.leader_commit, self.log.len() as u64);

                for i in current_commit_idx + 1..=self.commit_index {
                    events.push(RaftCoreEvent::CommitMessage {
                        index: i,
                        data: self.get_log_entry(i).unwrap().data.clone(),
                    });
                }
            }
        }
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

            if log_entry_idx > self.log.len() {
                self.log.push(log_entry);
            } else {
                self.log[log_entry_idx] = log_entry;
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

    fn append_log(&mut self, data: Vec<u8>) -> (u64, u64) {
        let index = self.get_log_last_index() + 1;
        self.log.push(LogEntry {
            term: self.term,
            data,
            index,
        });
        (index, self.term)
    }

    fn reset_heartbeat_timeout(&mut self) {
        self.received_heartbeat_time = Some(SystemTime::now());
    }
}

impl RaftCore {
    pub fn new(me: usize, peers_count: usize) -> RaftCore {
        RaftCore {
            me,
            inner: Mutex::new(InnerRaft::new(me, peers_count)),
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

        Ok(inner.append_log(buf))
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
