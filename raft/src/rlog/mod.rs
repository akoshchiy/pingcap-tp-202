use std::{time::{SystemTime, UNIX_EPOCH}, fmt};

pub enum Topic {
    Vote,
    Log,
    Rpc,
    Persist,
    Timer,
    State,
    RpcError,
    Client,
    Warn
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Topic::Vote => write!(f, "VT"),
            Topic::Log => write!(f, "LOG"),
            Topic::Rpc => write!(f, "RPC"),
            Topic::Persist => write!(f, "PS"),
            Topic::Timer => write!(f, "TR"),
            Topic::State => write!(f, "ST"),
            Topic::RpcError => write!(f, "RPCERR"),
            Topic::Warn => write!(f, "WARN"),
            Topic::Client => write!(f, "CL"),
        }
    }
}

pub enum State {
    Init,
    Follower,
    Candidate,
    Leader,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Init => write!(f, "I"),
            State::Follower => write!(f, "F"),
            State::Candidate => write!(f, "C"),
            State::Leader => write!(f, "L"),
        }
    }
}

pub fn raft_log(topic: Topic, server: usize, state: State, term: u64, msg: &str) {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // println!("{} T{} S{} {} {} -- {}", time, term, server, state.to_string(), topic.to_string(), msg);
}
