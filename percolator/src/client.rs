use std::{thread, time::Duration};

use futures::executor::block_on;
use labrpc::*;

use crate::{
    msg::{TimestampRequest, PrewriteRequest, Mutation, PrewriteResponse, CommitRequest},
    service::{TSOClient, TransactionClient},
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso_client: TSOClient,
    txn_client: TransactionClient,
    transaction: Option<Transaction>,
}

#[derive(Clone)]
struct Write {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct Transaction {
    writes: Vec<Write>,
    start_ts: u64,
}

impl Transaction {
    fn new(start_ts: u64) -> Self {
        Self {
            writes: vec![],
            start_ts,
        }
    }

    fn append(&mut self, write: Write) {
        self.writes.push(write);
    }
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_client,
            txn_client,
            transaction: None,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        self.backoff(|| {
            block_on(async {
                self.tso_client
                    .get_timestamp(&TimestampRequest {})
                    .await
                    .map(|resp| resp.time)
            })
        })
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        let start_ts = self.get_timestamp().expect("Failed to get_timestamp");
        self.transaction = Some(Transaction::new(start_ts));
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let mut tx = self.transaction
            .as_mut()
            .expect("Missing transaction");
        tx.append(Write { key, value });
    }

    /// Commits a transaction.
    pub fn commit(&mut self) -> Result<bool> {
        let mut tx = self.transaction.clone().expect("Missing transaction");
        self.transaction = None;

        if (tx.writes.is_empty()) {
            panic!("empty writes");
        }

        let primary = &tx.writes[0];

        let prewrite_resp = self.prewrite(primary, &tx)?;
        if !prewrite_resp.ok {
            return Ok(false);
        }

        let start_ts = tx.start_ts;
        let commit_ts = prewrite_resp.commit_ts;

        let primary_resp = self.do_commit(true, start_ts, commit_ts, vec![primary.key.clone()])?;
        if !primary_resp {
            return Ok(false);
        }

        let secondary_keys: Vec<_> = tx.writes[1..]
            .iter()
            .map(|w| w.key.clone())
            .collect();
        self.do_commit(false, start_ts, commit_ts, secondary_keys)
    }

    fn prewrite(&self, primary: &Write, tx: &Transaction) -> Result<PrewriteResponse> {
        let args = &PrewriteRequest {
            start_ts: tx.start_ts,
            primary: Some(Mutation {
                key: primary.key.clone(),
                value: primary.value.clone()
            }),
            mutations: tx.writes.iter()
                .map(|w| Mutation {
                    key: w.key.clone(),
                    value: w.value.clone()
                })
                .collect()
        };

        self.backoff(move || {
            block_on(async {
                self.txn_client
                    .prewrite(args)
                    .await
            })
        })
    }

    fn do_commit(
        &self,
        primary: bool,
        start_ts: u64,
        commit_ts: u64,
        keys: Vec<Vec<u8>>
    ) -> Result<bool> {
        let args = CommitRequest {
            is_primary: primary,
            start_ts,
            commit_ts,
            keys,
        };
        self.backoff(|| {
            block_on(async {
                self.txn_client
                .commit(&args)
                .await
                .map(|resp| resp.ok)
            })
        })
    }

    fn backoff<F, R>(&self, runnable: F) -> Result<R>
    where
        F: Fn() -> Result<R>,
    {
        let mut retry_count = 1;
        let mut backoff_ms = BACKOFF_TIME_MS;
        loop {
            let result = runnable();
            if retry_count >= RETRY_TIMES || result.is_ok() {
                return result;
            }
            thread::sleep(Duration::from_millis(backoff_ms));
            backoff_ms *= 2;
            retry_count += 1;
        }
    }
}
