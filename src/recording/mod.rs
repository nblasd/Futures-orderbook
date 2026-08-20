//! Phase 3 recording: sessions, recorder, bounded queue, storage worker.

pub mod metrics;
pub mod recorder;
pub mod session;
pub mod worker;

pub use metrics::{RecorderMetrics, StorageHealth, StorageState};
pub use recorder::{start_recorder, NewTrade, Recorder, RecorderHandle};
pub use session::{detect_git_commit, SessionRecord, SessionStatus};
pub use worker::{Record, RecordingConfig, StorageWorker};
