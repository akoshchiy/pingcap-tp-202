use std::ops::Add;
use futures::StreamExt;
use std::panic::resume_unwind;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use rand::Rng;

use crate::proto::raftpb::*;
use crate::raft::errors::*;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);

const HEARTBEAT_PROGRESS_TIMEOUT: Duration = Duration::from_millis(1000);
const VOTE_PROGRESS_TIMEOUT: Duration = Duration::from_millis(2000);

pub enum RaftCoreEvent {
    RequestVote { me: usize, req: RequestVoteRequest },
    AppendEntries {
    },
    Heartbeat { me: usize, term: u64 },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u32,
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
    sent_heartbeat_time: Option<SystemTime>,
    heartbeat_progress: ProgressContainer,
    vote_progress: ProgressContainer,
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
            voted_for: Option::None,
            received_heartbeat_time: Option::None,
            sent_heartbeat_time: Option::None,
            heartbeat_progress: ProgressContainer::new(),
            vote_progress: ProgressContainer::new(),
        }
    }

    fn update_heartbeat(&mut self) -> (bool, u64) {
        if self.status != PeerStatus::Leader {
            return (false, 0);
        }

        let skip_heartbeat = self
            .sent_heartbeat_time
            .map(|t| SystemTime::now().duration_since(t).unwrap())
            .map(|d| d < HEARTBEAT_INTERVAL)
            .unwrap_or(false);

        if skip_heartbeat {
            return (false, 0);
        }

        info!("peer#{} - try send heartbeat, term: {}", self.me, self.term);

        self.sent_heartbeat_time = Option::Some(SystemTime::now());

        self.heartbeat_progress.start(Progress::new(
            VOTE_PROGRESS_TIMEOUT,
            self.quorum,
            self.peers_count,
        ));

        (true, self.term)
    }

    fn handle_heartbeat_request(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if self.term > args.term {
            return AppendEntriesReply {
                peer: self.me as u32,
                term: self.term,
                success: false
            }
        }

        self.received_heartbeat_time = Option::Some(SystemTime::now());
        self.term = args.term;
        self.status = PeerStatus::Follower;

        AppendEntriesReply {
            peer: self.me as u32,
            term: args.term,
            success: true,
        }
    }

    fn handle_heartbeat_reply(&mut self, reply: AppendEntriesReply) {
        self.heartbeat_progress.check_expired();

        if !self.heartbeat_progress.contains() {
            return;
        }

        let progress = self.heartbeat_progress.get_mut();

        if reply.success {
            progress.update(true);
            if progress.completed() {
                self.heartbeat_progress.clear();
            }
            return;
        }

        info!(
            "peer#{} - heartbeat reject, loosing leadership, peer: {}, term: {}",
            self.me, reply.peer, reply.term
        );

        self.heartbeat_progress.clear();
        self.status = PeerStatus::Follower;
        self.term = reply.term;
    }

    fn handle_heartbeat_error(&mut self, err: Error) {
        self.heartbeat_progress.check_expired();

        if !self.heartbeat_progress.contains() {
            return;
        }

        warn!(
            "peer#{} - send_heartbeat error resp: {}",
            self.me,
            err.to_string()
        );

        let progress = self.heartbeat_progress.get_mut();
        progress.update(false);

        if progress.failed_quorum() {
            info!(
                "peer#{} - send_heartbeat error quorum, loosing leadership",
                self.me
            );
            self.status = PeerStatus::Follower;
            self.heartbeat_progress.clear();
        }
    }

    fn check_leader_heartbeat(&mut self) -> Option<RequestVoteRequest> {
        let now = SystemTime::now();

        if self.status == PeerStatus::Leader {
            return Option::None;
        }

        let duration_since = self
            .received_heartbeat_time
            .map(|t| now.duration_since(t).unwrap());

        let expired = duration_since.map(|d| d > HEARTBEAT_TIMEOUT).unwrap_or(true);

        if !expired {
            return Option::None;
        }

        let dur_ms = duration_since.map(|d| d.as_millis()).unwrap_or(0);

        info!(
            "peer#{} - last heartbeat: {}, initiating new vote",
            self.me, dur_ms
        );

        self.term += 1;
        self.status = PeerStatus::Candidate;
        self.vote_progress.start(Progress::new(
            Duration::from_secs(2),
            self.quorum,
            self.peers_count,
        ));

        // self-vote
        self.vote_progress.get_mut().update(true);

        Option::Some(RequestVoteRequest {
            term: self.term,
            candidate_id: self.me as u32,
        })
    }

    fn handle_request_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if self.term > args.term {
            info!(
                "peer#{} - request_vote, my term: {}, candidate_term: {}, candidate_id: {}, reject",
                self.me, self.term, args.term, args.candidate_id
            );

            return RequestVoteReply {
                term: self.term,
                vote_granted: false,
            };
        }

        let already_voted = self.voted_for.map(|v| v.term >= args.term).unwrap_or(false);

        let already_candidate = self.status == PeerStatus::Candidate && self.term >= args.term;

        if !already_voted && !already_candidate {
            self.term = args.term;
            self.voted_for = Option::Some(Vote {
                peer: args.candidate_id as usize,
                term: args.term,
            });
            self.status = PeerStatus::Candidate;
            self.received_heartbeat_time = Option::Some(SystemTime::now());
        }

        info!(
            "peer#{} - request_vote, my term: {}, candidate_term: {}, candidate_id: {}, {}",
            self.me,
            self.term,
            args.term,
            args.candidate_id,
            if !already_voted { "approve" } else { "reject" }
        );

        RequestVoteReply {
            term: args.term,
            vote_granted: !already_voted,
        }
    }

    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) {
        self.vote_progress.check_expired();

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

            self.status = PeerStatus::Leader;
            self.vote_progress.clear();
        }
    }

    fn handle_request_vote_error(&mut self, err: Error) {
        self.vote_progress.check_expired();

        if !self.vote_progress.contains() {
            return;
        }
        let mut progress = self.vote_progress.get_mut();
        progress.update(false);
        warn!(
            "peer#{} - request_vote[term={}] error resp: {}",
            self.me,
            self.term,
            err.to_string()
        );
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
    //

    pub fn handle_append_entries_request(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut inner = self.inner.lock().unwrap();
        // TODO handle append_entries
        inner.handle_heartbeat_request(args)
    }

    pub fn handle_heartbeat_result(&self, result: Result<AppendEntriesReply>) {
        let mut inner = self.inner.lock().unwrap();
        match result {
            Ok(reply) => inner.handle_heartbeat_reply(reply),
            Err(err) => inner.handle_heartbeat_error(err),
        }
    }

    pub fn get_state(&self) -> (bool, u64) {
        let mut inner = self.inner.lock().unwrap();
        let is_leader = inner.status == PeerStatus::Leader;
        (is_leader, inner.term)
    }

    pub fn send_heartbeat(&self) -> Option<RaftCoreEvent> {
        let mut inner = self.inner.lock().unwrap();
        let result = inner.update_heartbeat();
        if result.0 {
            return Option::Some(RaftCoreEvent::Heartbeat {
                me: self.me,
                term: result.1,
            });
        }
        Option::None
    }

    pub fn check_leader_heartbeat(&self) -> Option<RaftCoreEvent> {
        let mut inner = self.inner.lock().unwrap();
        inner.check_leader_heartbeat()
            .map(|req| RaftCoreEvent::RequestVote {
                me: self.me,
                req,
            })
    }

    pub fn get_heartbeat_delay(&self) -> Duration {
        let mut inner = self.inner.lock().unwrap();
        inner.sent_heartbeat_time
            .map(|t| SystemTime::now().duration_since(t).unwrap())
            .map(|d| {
                if d > HEARTBEAT_INTERVAL {
                    Duration::from_millis(0)
                } else {
                    HEARTBEAT_INTERVAL - d
                }
            })
            .unwrap_or(HEARTBEAT_INTERVAL)
    }
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

    fn check_expired(&mut self) {
        if self.progress.is_none() {
            return;
        }

        let expired;
        {
            let progress = self.progress.as_mut().unwrap();
            expired = progress.is_expired();
        }

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
    expire_time: SystemTime,
    quorum_count: usize,
    peers_count: usize,
    failed_count: usize,
    succeed_count: usize,
}

impl Progress {
    fn new(time: Duration, quorum: usize, peers_count: usize) -> Progress {
        Progress {
            expire_time: SystemTime::now() + time,
            quorum_count: quorum,
            peers_count,
            failed_count: 0,
            succeed_count: 0,
        }
    }

    fn total_count(&self) -> usize {
        self.failed_count + self.succeed_count
    }

    fn failed_count(&self) -> usize {
        self.failed_count
    }

    fn succeed_count(&self) -> usize {
        self.succeed_count
    }

    fn is_expired(&self) -> bool {
        self.expire_time < SystemTime::now()
    }

    fn achieved_quorum(&self) -> bool {
        self.succeed_count >= self.quorum_count
    }

    fn failed_quorum(&self) -> bool {
        self.failed_count >= self.quorum_count
    }

    fn completed(&self) -> bool {
        self.total_count() >= self.peers_count
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

fn randomize_duration(duration: Duration) -> Duration {
    let offset: u64 = rand::thread_rng().gen_range(0, 200);
    duration.add(Duration::from_millis(offset))
}
