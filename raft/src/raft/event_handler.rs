use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use futures::channel::mpsc::UnboundedSender;
use futures::executor::ThreadPool;
use futures_timer::Delay;

use crate::proto::raftpb::*;
use crate::raft::core::{
    get_heartbeat_delay, AppendEntriesData, RaftCore, RaftCoreEvent,
};
use crate::rlog::Topic;

use super::core::{get_election_timeout, PeerStatus, RequestVoteData};
use super::errors::Error;
use super::ApplyMsg;

pub struct RaftEventHandler {
    peers: Vec<RaftClient>,
    raft_core: Arc<RaftCore>,
    apply_ch: UnboundedSender<ApplyMsg>,
    stop: Arc<AtomicBool>,
    pool: Arc<ThreadPool>,
}

impl RaftEventHandler {
    pub fn new(
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

    pub fn handle(&self, event: RaftCoreEvent) {
        match event {
            RaftCoreEvent::AppendEntries { data } => self.on_append_entries(data),
            RaftCoreEvent::CommitMessage { index, data } => self.on_commit_message(index, data),
            RaftCoreEvent::BecomeCandidate {
                was_candidate,
                data,
            } => self.on_become_candidate(!was_candidate, data),
            RaftCoreEvent::BecomeFollower => self.on_become_follower(),
            RaftCoreEvent::BecomeLeader => self.on_become_leader(),
        }
    }

    fn on_become_leader(&self) {
        let stop = self.stop.clone();
        let raft = self.raft_core.clone();

        self.pool.spawn_ok(async move {
            raft.log(Topic::Timer, "start leader heartbeat loop");
            loop {
                let stop = stop.load(SeqCst);
                let is_leader = raft.get_peer_status() == PeerStatus::Leader;
                if stop || !is_leader {
                    raft.log(Topic::Timer, "stop leader heartbeat loop");
                    return;
                }
                raft.tick_append_entries();
                Delay::new(get_heartbeat_delay()).await;
            }
        });
    }

    fn on_become_candidate(&self, spawn_timeout_loop: bool, data: RequestVoteData) {
        if spawn_timeout_loop {
            self.spawn_election_timeout_loop();
        }
        self.send_request_vote(data);
    }

    fn spawn_election_timeout_loop(&self) {
        let stop = self.stop.clone();
        let raft = self.raft_core.clone();

        self.pool.spawn_ok(async move {
            raft.log(Topic::Timer, "start election timeout loop");
            loop {
                let stop = stop.load(SeqCst);
                let is_leader = raft.get_peer_status() == PeerStatus::Leader;
                if stop || is_leader {
                    raft.log(Topic::Timer, "stop election timeout loop");
                    return;
                }
                let timeout = get_election_timeout();
                Delay::new(timeout).await;
                raft.check_election_timeout(timeout);
            }
        });
    }

    fn send_request_vote(&self, data: RequestVoteData) {
        let term = data.term;

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

                raft_clone.log(
                    Topic::Rpc,
                    format!(
                        "send REQUEST_VOTE_ARGS TO S{} [S{},T={},l_term={},l_idx={}]",
                        i, args.candidate_id, args.term, args.last_log_term, args.last_log_index
                    )
                    .as_str(),
                );

                let result = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
                raft_clone.handle_request_vote_result(term, i, result);
            });
        }
    }

    fn on_become_follower(&self) {
        self.spawn_election_timeout_loop();
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
                    string_data: entry.string_data,
                    data: entry.data,
                    from_leader: entry.from_leader,
                })
                .collect();

            let term = data.term;
            let match_index_to_set = data.match_index_to_set;

            let args = AppendEntriesArgs {
                term: data.term,
                leader_id: data.me as u32,
                entries,
                leader_commit: data.leader_commit,
                prev_log_idx: data.prev_log_index,
                prev_log_term: data.prev_log_term,
            };

            raft_clone.log(
                Topic::Rpc,
                format!(
                    "send APPEND_ENTRIES_ARGS TO S{} [S{},T={},p_idx={},p_term={},l_commit={},e={}]",
                    data.peer,
                    args.leader_id,
                    args.term,
                    args.prev_log_idx,
                    args.prev_log_term,
                    args.leader_commit,
                    args.entries.len()
                )
                .as_str(),
            );

            let reply = peer_clone.append_entries(&args).await.map_err(Error::Rpc);

            raft_clone.handle_append_entries_result(data.peer, term, match_index_to_set, reply);
        });
    }
}
