//! Phase 3 replay: reconstruct recorded market data and feed it through the
//! same order-book / trade pipeline deterministically.

pub mod engine;
pub mod reader;
pub mod report;
pub mod timing;

pub use engine::{run_replay, ReplayConfig, ReplayOutcome};
pub use reader::{load_raw_events, load_session, ReplayEvent, SessionData};
pub use report::ReplayReport;
pub use timing::ReplayTiming;
