use std::collections::HashMap;
use std::future::{self, Future};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use futures::StreamExt;
use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::channel::oneshot;

use crate::kvraft::errors;
use crate::proto::kvraftpb::*;
use crate::raft::errors::{Result, Error};
use crate::raft::{self, ApplyMsg};
use crate::raft::errors::Error::NotLeader;

#[derive(Clone, PartialEq, Message)]
struct LogEntry {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, optional, tag = "2")]
    value: Option<String>,
    #[prost(enumeration = "EntryType", tag = "3")]
    entry_type: i32,
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
    apply_ch: UnboundedReceiver<ApplyMsg>,
    // Your definitions here.
    state: Mutex<KvServerState>,
}

struct KvServerState {
    index: HashMap<String, String>,
    requests: HashMap<u64, RequestState>,
}

struct RequestState {
    entry: LogEntry,
    callback: Box<dyn Fn(&LogEntry, &LogEntry)>,
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



        KvServer {
            rf: rf_node,
            me,
            maxraftstate,
            apply_ch,
        }
    }

    fn state(&self) -> MutexGuard<KvServerState> {
        self.state.lock().unwrap()
    }

    fn handle_apply_msg(&self, idx: u64, data: Vec<u8>) -> Result<()> {
        let entry = decode_data(&data)?;

        let mut state = self.state();

        let request = state.requests.remove(&idx);
        match request {
            Some(state) => {
                state.callback(&entry, &state.entry);
            },
            None => {},
        }





        Ok(())
    }

    async fn get(&self, arg: GetRequest) -> GetReply {
        unimplemented!()
    }

    async fn put_append(&self, arg: PutAppendRequest) -> PutAppendReply {
        let entry = match arg.op() {
            Op::Put => LogEntry {
                key: arg.key,
                value: Some(arg.value),
                entry_type: 1,
            },
            Op::Append => LogEntry {
                key: arg.key,
                value: Some(arg.value),
                entry_type: 2,
            },
            Op::Unknown => {
                return PutAppendReply {
                    wrong_leader: false,
                    err: "undefined Op".to_owned(),
                }
            }
        };

        match self.rf.start(&entry) {
            Ok((idx, term)) => self.wait_apply(idx, term, &entry).await,
            Err(err) => match err {
                NotLeader => PutAppendReply {
                    wrong_leader: true,
                    err: "".to_owned(),
                },
                _ => PutAppendReply {
                    wrong_leader: false,
                    err: err.to_string(),
                },
            },
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

fn decode_data(data: &[u8]) -> Result<LogEntry> {
    labcodec::decode(data)
        .map_err(|err| Error::Decode(err))        
}

fn read_apply_ch(server: Arc<KvServer>, apply_ch: UnboundedReceiver<ApplyMsg>) -> impl Future {
    apply_ch.for_each(|msg| match msg {
        ApplyMsg::Command { data, index } => {
            


            future::ready(())
        },
        _ => {
            future::ready(())
        },
    })
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
        // Your code here.
        crate::your_code_here(kv);
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
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
        // Your code here.
        crate::your_code_here(arg)
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        crate::your_code_here(arg)
    }
}
