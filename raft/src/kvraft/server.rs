use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::channel::oneshot;
use futures::executor::ThreadPoolBuilder;
use futures::StreamExt;

use crate::proto::kvraftpb::*;
use crate::raft::errors::Error::NotLeader;
use crate::raft::{self, ApplyMsg};

#[derive(Clone, PartialEq, Eq, Message)]
struct LogEntry {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, optional, tag = "2")]
    value: Option<String>,
    #[prost(enumeration = "EntryType", tag = "3")]
    entry_type: i32,
    #[prost(string, tag = "4")]
    client_id: String,
    #[prost(uint64, tag = "5")]
    client_op_seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
enum EntryType {
    Get = 0,
    Put = 1,
    Append = 2,
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // apply_ch: UnboundedReceiver<ApplyMsg>,
    // Your definitions here.
    state: Arc<Mutex<KvServerState>>,
}

struct KvServerState {
    values: HashMap<String, String>,
    request_channels: HashMap<u64, oneshot::Sender<WrappedLogEntry>>,
    client_op_seqs: HashMap<String, u64>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let rf_node = raft::Node::new(rf);
        let pool = ThreadPoolBuilder::new().pool_size(1).create().unwrap();

        let server = KvServer {
            rf: rf_node,
            me,
            maxraftstate,
            // apply_ch,
            state: Arc::new(Mutex::new(KvServerState {
                values: HashMap::new(),
                request_channels: HashMap::new(),
                client_op_seqs: HashMap::new(),
            })),
        };

        pool.spawn_ok(read_apply_ch(server.state.clone(), apply_ch));

        server
    }

    fn kill(&self) {
        self.rf.kill();
    }

    fn lock_state(&self) -> MutexGuard<KvServerState> {
        self.state.lock().unwrap()
    }

    fn add_request_ch(&self, idx: u64, ch: oneshot::Sender<WrappedLogEntry>) {
        let mut state = self.lock_state();
        state.request_channels.insert(idx, ch);
    }

    async fn get(&self, arg: GetRequest) -> GetReply {
        let entry = LogEntry {
            key: arg.key.clone(),
            value: None,
            entry_type: 0,
            client_id: arg.client_id,
            client_op_seq: arg.client_op_seq,
        };

        let result = self.append_entry(entry).await;

        match result {
            Ok(entry) => {
                GetReply {
                    value: entry.value.unwrap_or_else(|| "".to_string()),
                    wrong_leader: false,
                    err: "".to_owned(),
                }
            }
            Err(err) => match err {
                AppendEntryError::NotLeader => GetReply {
                    wrong_leader: true,
                    err: "".to_string(),
                    value: "".to_string(),
                },
                AppendEntryError::Other(msg) => GetReply {
                    wrong_leader: false,
                    err: msg,
                    value: "".to_string(),
                },
            },
        }
    }

    async fn put_append(&self, arg: PutAppendRequest) -> PutAppendReply {
        let entry = match arg.op() {
            Op::Put => LogEntry {
                key: arg.key,
                value: Some(arg.value),
                entry_type: 1,
                client_id: arg.client_id,
                client_op_seq: arg.client_op_seq,
            },
            Op::Append => LogEntry {
                key: arg.key,
                value: Some(arg.value),
                entry_type: 2,
                client_id: arg.client_id,
                client_op_seq: arg.client_op_seq,
            },
            Op::Unknown => {
                return PutAppendReply {
                    wrong_leader: false,
                    err: "undefined Op".to_owned(),
                }
            }
        };

        let result = self.append_entry(entry).await;

        match result {
            Ok(_) => PutAppendReply {
                wrong_leader: false,
                err: "".to_string(),
            },
            Err(err) => match err {
                AppendEntryError::NotLeader => PutAppendReply {
                    wrong_leader: true,
                    err: "".to_owned(),
                },
                AppendEntryError::Other(msg) => PutAppendReply {
                    wrong_leader: false,
                    err: msg,
                },
            },
        }
    }

    async fn append_entry(&self, entry: LogEntry) -> AppendEntryResult {
        let (idx, _) = self.rf.start(&entry).map_err(|err| match err {
            NotLeader => AppendEntryError::NotLeader,
            _ => AppendEntryError::Other(err.to_string()),
        })?;

        let (sender, recv) = oneshot::channel();

        self.add_request_ch(idx, sender);

        let recv_entry = recv.await.unwrap();
        if entry != recv_entry.entry {
            return AppendEntryResult::Err(AppendEntryError::NotLeader);
        }

        AppendEntryResult::Ok(recv_entry)
    }
}

async fn read_apply_ch(
    state: Arc<Mutex<KvServerState>>,
    mut apply_ch: UnboundedReceiver<ApplyMsg>,
) {
    while let Some(msg) = apply_ch.next().await {
        match msg {
            ApplyMsg::Command { data, index } => {
                let mut state = state.lock().unwrap();
                process_apply_command(&mut state, index, data);
            },
            _ => {},
        }
    }
}

fn process_apply_command(state: &mut KvServerState, index: u64, data: Vec<u8>) {
    let entry = decode_data(&data);
    let key = entry.key.clone();

    let client_id = entry.client_id.clone();
    let seq_op = state.client_op_seqs.get(&client_id)
        .map(|op| *op)
        .unwrap_or(0);

    let duplicate = seq_op >= entry.client_op_seq;

    let wrapped_entry = match entry.entry_type() {
        EntryType::Get => {
            WrappedLogEntry {
                entry,
                value: state.values.get(&key).map(|v| v.clone()),
            }
        },
        EntryType::Put => {
            if !duplicate {
                state.values.insert(key, entry.value().to_string());
            }
            WrappedLogEntry {
                entry,
                value: None,
            }
        },
        EntryType::Append => {
            if !duplicate {
                let entry_val = entry.value
                    .as_ref()
                    .map(|v| v.clone())
                    .unwrap_or_else(|| "".to_string());

                let new_val = state.values
                    .get(&key)
                    .map(|v| std::format!("{}{}", v, entry_val))
                    .unwrap_or_else(|| entry_val);

                state.values.insert(key, new_val);
            }
            WrappedLogEntry {
                entry,
                value: None
            }
        },
    };

    if !duplicate {
        state.client_op_seqs.insert(client_id, seq_op);
    }

    state.request_channels
        .remove(&index)
        .map(|ch| ch.send(wrapped_entry).unwrap());
}

#[derive(Debug)]
struct WrappedLogEntry {
    entry: LogEntry,
    value: Option<String>,
}

enum AppendEntryError {
    NotLeader,
    Other(String),
}

type AppendEntryResult = std::result::Result<WrappedLogEntry, AppendEntryError>;

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

fn decode_data(data: &[u8]) -> LogEntry {
    labcodec::decode(data).unwrap()
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<KvServer>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        Node { server: Arc::new(kv) }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        self.server.kill();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        self.server.rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        info!("GET REQ s{} : {:?}", self.server.me, arg);
        let reply = self.server.get(arg).await;
        info!("GET REPLY s{} : {:?}", self.server.me, reply);
        Ok(reply)
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        info!("PUT_APPEND REQ s{} : {:?}", self.server.me, arg);
        let reply = self.server.put_append(arg).await;
        info!("PUT_APPEND REPLY s{} : {:?}", self.server.me, reply);
        Ok(reply)
    }
}
