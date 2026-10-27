//! Core library module for `nitro-compute`.

#[doc(hidden)]
pub use ::core::*;

pub mod network;
pub mod runtime;

pub use network::{create_swarm, run_event_loop, NitroBehavior};
pub use runtime::NitroRuntime;
