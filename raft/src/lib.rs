#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;
extern crate core;

pub mod kvraft;
mod proto;
pub mod raft;
mod rlog;

/// A place holder for suppressing unused_variables warning.
fn your_code_here<T>(_: T) -> ! {
    unimplemented!()
}
