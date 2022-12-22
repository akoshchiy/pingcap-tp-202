use std::{fmt, cell::Cell};

use futures::{executor::{LocalPool, block_on}, task::{SpawnExt, LocalSpawnExt, Spawn, LocalSpawn}};

use crate::proto::kvraftpb::*;

#[derive(Clone)]
enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    leader: Cell<usize>,
    op_seq: Cell<u64>,
    // You will have to modify this struct.
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk { name, servers, leader: Cell::new(0), op_seq: Cell::new(0) }
        // crate::your_code_here((name, servers))
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        let mut leader = self.leader.get();
        let op_seq = self.inc_op_seq();
        loop {
            let result = self.do_get(key.clone(), leader, op_seq);
            match result {
                Ok(reply) => {
                    if reply.wrong_leader {
                        leader = self.switch_leader();
                    } else {
                        return reply.value;
                    }
                },
                Err(_) => {},
            }
        }
    }

    fn do_get(&self, key: String, server_idx: usize, op_seq: u64) -> labrpc::Result<GetReply> {
        block_on(async {
            let args = GetRequest {
                client_id: self.name.clone(),
                client_op_seq: op_seq,
                key,
            };
            self.servers[server_idx].get(&args).await
        })
    }

    fn inc_op_seq(&self) -> u64 {
        let val = self.op_seq.get() + 1;
        self.op_seq.set(val);
        val
    }

    fn switch_leader(&self) -> usize {
        let current_leader = self.leader.get();
        let new_leader = (current_leader + 1) % self.servers.len();
        self.leader.set(new_leader);
        new_leader
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        let mut leader = self.leader.get();
        let op_seq = self.inc_op_seq();
        loop {
            let result = self.do_put_append(op.clone(), leader, op_seq);
            match result {
                Ok(reply) => {
                    if reply.wrong_leader {
                        leader = self.switch_leader();
                    } else {
                        return;
                    }
                },
                Err(_) => {},
            }
        }
    }

    fn do_put_append(&self, op: Op, server_idx: usize, op_seq: u64) -> labrpc::Result<PutAppendReply> {
        let args = match op {
            Op::Put(k, v) => PutAppendRequest {
                client_id: self.name.clone(),
                client_op_seq: op_seq,
                key: k,
                value: v,
                op: 1,
            },
            Op::Append(k, v) => PutAppendRequest { 
                client_id: self.name.clone(),
                client_op_seq: op_seq,
                key: k,
                value: v,
                op: 2 
            },
        };
        block_on(async {
            self.servers[server_idx].put_append(&args).await
        })
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
