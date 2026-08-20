//! Phase 3 tests: recorder → storage → replay/verify pipeline.
//!
//! All tests use the in-memory storage backend — no network, no ClickHouse.

use std::sync::Arc;
use std::time::Duration;

use futures_orderbook::binance::types::DepthUpdate;
use futures_orderbook::orderbook::book::OrderBook;
use futures_orderbook::orderbook::level::price_str_to_ticks;
use futures_orderbook::orderbook::synchronizer::Synchronizer;
use futures_orderbook::recording::{start_recorder, NewTrade, RecordingConfig, SessionRecord};
use futures_orderbook::replay::{load_session, run_replay, ReplayConfig};
use futures_orderbook::storage::{FlakyStorage, MemoryStorage, Storage};
use futures_orderbook::verify::verify_session;

fn depth(u: u64, pu: u64, first: u64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> DepthUpdate {
    DepthUpdate {
        event_type: "depthUpdate".to_string(),
        event_time: 1234567890,
        transaction_time: 1234567890,
        symbol: "BTCUSDT".to_string(),
        first_update_id: first,
        final_update_id: u,
        previous_final_update_id: pu,
        bids: bids
            .iter()
            .map(|(p, q)| (p.to_string(), q.to_string()))
            .collect(),
        asks: asks
            .iter()
            .map(|(p, q)| (p.to_string(), q.to_string()))
            .collect(),
    }
}

fn snapshot_bids() -> Vec<(String, String)> {
    [
        ("50000.10", "1.500"),
        ("50000.00", "2.300"),
        ("49999.90", "0.800"),
        ("49999.50", "5.000"),
        ("49999.00", "3.200"),
    ]
    .iter()
    .map(|(p, q)| (p.to_string(), q.to_string()))
    .collect()
}

fn snapshot_asks() -> Vec<(String, String)> {
    [
        ("50000.20", "0.500"),
        ("50000.50", "1.800"),
        ("50001.00", "2.100"),
        ("50002.00", "0.300"),
        ("50005.00", "1.000"),
    ]
    .iter()
    .map(|(p, q)| (p.to_string(), q.to_string()))
    .collect()
}

/// Build the live scenario:
///   e1,e2,e3 buffered before snapshot; snapshot lastUpdateId=160; e4,e5 live.
#[allow(clippy::type_complexity)]
fn scenario_events() -> (
    Vec<DepthUpdate>,
    u64,
    Vec<(String, String)>,
    Vec<(String, String)>,
) {
    let e1 = depth(
        153,
        149,
        150,
        &[("50000.10", "2.0")],
        &[("50000.20", "1.0")],
    );
    let e2 = depth(
        157,
        153,
        154,
        &[("50000.00", "3.0")],
        &[("50000.50", "2.0")],
    );
    let e3 = depth(
        160,
        157,
        158,
        &[("49999.90", "1.5")],
        &[("50001.00", "0.8")],
    );
    let e4 = depth(
        164,
        160,
        161,
        &[("50001.00", "4.0")],
        &[("50001.50", "2.5")],
    );
    let e5 = depth(167, 164, 165, &[("50001.00", "0")], &[("50002.00", "1.1")]);
    (
        vec![e1, e2, e3, e4, e5],
        160,
        snapshot_bids(),
        snapshot_asks(),
    )
}

/// Process the scenario exactly as the live engine would and return the book.
fn live_process() -> OrderBook {
    let (events, snapshot_id, bids, asks) = scenario_events();
    let mut sync = Synchronizer::new();
    let mut book = OrderBook::new();
    sync.on_connected();

    // e1..e3 arrive while Buffering
    for e in &events[..3] {
        sync.buffer_event(e.clone());
    }
    // Snapshot arrives
    sync.on_snapshot_loading();
    book.apply_snapshot(&bids, &asks, snapshot_id).unwrap();
    for e in sync.reconcile(snapshot_id).unwrap() {
        book.apply_depth_update(&e.bids, &e.asks, e.final_update_id)
            .unwrap();
    }
    // e4, e5 arrive live
    for e in &events[3..] {
        use futures_orderbook::orderbook::synchronizer::ProcessResult;
        match sync.process_live_event(e) {
            ProcessResult::Apply => {
                book.apply_depth_update(&e.bids, &e.asks, e.final_update_id)
                    .unwrap();
            }
            _ => panic!("expected apply"),
        }
    }
    book
}

fn make_trades() -> Vec<NewTrade> {
    vec![
        NewTrade {
            symbol: "BTCUSDT".to_string(),
            trade_id: 1001,
            first_trade_id: None,
            last_trade_id: None,
            price: price_str_to_ticks("50000.00").unwrap(),
            quantity: 1_000_000,
            aggressor_side: "BUY".to_string(),
            exchange_event_time_ms: 1234567891000,
            trade_time_ms: 1234567891000,
            local_receive_time_ns: 1_700_000_000_000_000_000,
            order_type: "MARKET".to_string(),
        },
        NewTrade {
            symbol: "BTCUSDT".to_string(),
            trade_id: 1002,
            first_trade_id: None,
            last_trade_id: None,
            price: price_str_to_ticks("50001.00").unwrap(),
            quantity: 2_000_000,
            aggressor_side: "SELL".to_string(),
            exchange_event_time_ms: 1234567892000,
            trade_time_ms: 1234567892000,
            local_receive_time_ns: 1_700_000_000_000_000_000,
            order_type: "MARKET".to_string(),
        },
    ]
}

async fn setup_recorder(
    storage: Arc<dyn Storage>,
    batch_size: usize,
    flush_ms: u64,
    queue_cap: usize,
) -> (
    Arc<futures_orderbook::recording::Recorder>,
    futures_orderbook::recording::RecorderHandle,
    SessionRecord,
) {
    let session = SessionRecord::new(
        "BTCUSDT",
        "Binance",
        "USDⓈ-M",
        "PERPETUAL",
        "wss://fstream.binance.com/ws/btcusdt@depth@100ms",
        "wss://fstream.binance.com/ws/btcusdt@trade",
        "test",
        "testcommit",
    );
    storage.insert_session(&session).await.unwrap();
    let config = RecordingConfig::new(batch_size, flush_ms, queue_cap);
    let (recorder, handle) = start_recorder(storage, session.clone(), config);
    (recorder, handle, session)
}

fn local_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[tokio::test]
async fn test_offline_e2e_replay_matches_live_book() {
    let (events, snapshot_id, bids, asks) = scenario_events();

    // Live book (reference).
    let live = live_process();

    // Record the same scenario through the recorder.
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let (recorder, handle, session) =
        setup_recorder(Arc::clone(&storage), 1000, 60000, 100_000).await;
    let ns = local_ns();

    for (i, e) in events.iter().enumerate() {
        recorder.record_raw(
            "BTCUSDT",
            "depth",
            format!(r#"{{"e":"depthUpdate","u":{}}}"#, e.final_update_id),
            e.event_time,
            Some(e.transaction_time),
            ns,
        );
        recorder.record_depth_event(e, ns);
        // Snapshot is recorded after the first three buffered events, exactly
        // as the live engine applies it after reconcile.
        if i == 2 {
            recorder.record_snapshot("BTCUSDT", snapshot_id, 1234567890000, &bids, &asks);
        }
    }

    for t in make_trades() {
        recorder.record_trade(t);
    }

    recorder.request_shutdown();
    handle.join().await.unwrap();

    // Replay through the same Synchronizer/OrderBook/TradeProcessor.
    let data = load_session(storage.as_ref(), session.session_id)
        .await
        .unwrap();
    let outcome = run_replay(data, ReplayConfig::default()).await.unwrap();

    assert_eq!(outcome.depth_events, 5);
    assert_eq!(outcome.snapshots_applied, 1);
    assert_eq!(outcome.events_applied, 3, "e3 (bridge) + e4 + e5 applied");
    assert_eq!(outcome.sequence_errors, 0);
    assert_eq!(outcome.trades_processed, 2);

    // Book state matches the live-processed book exactly.
    assert_eq!(outcome.book_bid_levels, live.bid_count());
    assert_eq!(outcome.book_ask_levels, live.ask_count());
    assert_eq!(outcome.best_bid, live.best_bid());
    assert_eq!(outcome.best_ask, live.best_ask());
    assert_eq!(outcome.final_update_id, live.last_update_id());
    assert_eq!(
        outcome.best_bid,
        Some(price_str_to_ticks("50000.10").unwrap())
    );
    assert_eq!(
        outcome.best_ask,
        Some(price_str_to_ticks("50000.20").unwrap())
    );

    // Verify the recorded session passes integrity checks.
    let report = verify_session(storage.as_ref(), session.session_id)
        .await
        .unwrap();
    assert!(report.verified, "verify failed: {}", report);
}

#[tokio::test]
async fn test_shutdown_flushes_pending_events() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let (recorder, handle, _session) =
        setup_recorder(Arc::clone(&storage), 1000, 60000, 100_000).await;
    let ns = local_ns();

    // Well below the batch threshold; only the shutdown flush persists these.
    for i in 0..5 {
        let e = depth(
            200 + i,
            199 + i,
            199 + i,
            &[("50000.10", "1.0")],
            &[("50000.20", "1.0")],
        );
        recorder.record_depth_event(&e, ns);
    }

    recorder.request_shutdown();
    handle.join().await.unwrap();

    let db = storage
        .as_any()
        .downcast_ref::<MemoryStorage>()
        .unwrap()
        .snapshot_db();
    // 5 events x (1 bid + 1 ask) level rows.
    assert_eq!(db.level_changes.len(), 10);
}

#[tokio::test]
async fn test_batch_flush_threshold() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let (recorder, handle, _session) =
        setup_recorder(Arc::clone(&storage), 2, 60000, 100_000).await;
    let ns = local_ns();

    // Each event carries 2 level rows; with batch_size = 2 the worker should
    // flush after every event.
    for i in 0..2 {
        let e = depth(
            300 + i,
            299 + i,
            299 + i,
            &[("50000.10", "1.0")],
            &[("50000.20", "1.0")],
        );
        recorder.record_depth_event(&e, ns);
    }

    // Wait until the threshold flush lands (2 events x 2 rows = 4 rows).
    let memory = storage.as_any().downcast_ref::<MemoryStorage>().unwrap();
    let mut stored = 0;
    for _ in 0..50 {
        stored = memory.snapshot_db().level_changes.len();
        if stored >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(stored, 4, "threshold flush should have stored 4 of 6 rows");

    // Third event sits pending until the shutdown flush.
    let e = depth(
        302,
        301,
        301,
        &[("50000.10", "1.0")],
        &[("50000.20", "1.0")],
    );
    recorder.record_depth_event(&e, ns);
    recorder.request_shutdown();
    handle.join().await.unwrap();
    assert_eq!(memory.snapshot_db().level_changes.len(), 6);
}

#[tokio::test]
async fn test_invalid_depth_levels_rejected() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let (recorder, handle, _session) =
        setup_recorder(Arc::clone(&storage), 1000, 60000, 100_000).await;
    let ns = local_ns();

    // One valid bid, one invalid bid (bad price), one valid ask.
    let e = DepthUpdate {
        event_type: "depthUpdate".to_string(),
        event_time: 1,
        transaction_time: 1,
        symbol: "BTCUSDT".to_string(),
        first_update_id: 1,
        final_update_id: 5,
        previous_final_update_id: 0,
        bids: vec![
            ("50000.10".to_string(), "1.0".to_string()),
            ("not_a_number".to_string(), "2.0".to_string()),
        ],
        asks: vec![("50000.20".to_string(), "1.0".to_string())],
    };
    recorder.record_depth_event(&e, ns);

    recorder.request_shutdown();
    handle.join().await.unwrap();

    let db = storage
        .as_any()
        .downcast_ref::<MemoryStorage>()
        .unwrap()
        .snapshot_db();
    // Only the valid bid + valid ask are stored; the invalid level is skipped.
    assert_eq!(db.level_changes.len(), 2);
    assert!(db
        .level_changes
        .iter()
        .all(|r| r.side == "BID" || r.side == "ASK"));
}

#[tokio::test]
async fn test_retry_recovers_with_no_data_loss() {
    let inner: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    // Fail the first two insert calls, then succeed.
    let flaky = Arc::new(FlakyStorage::new(inner.clone(), 2));

    let (recorder, handle, _session) =
        setup_recorder(Arc::clone(&flaky) as Arc<dyn Storage>, 1000, 60000, 100_000).await;
    let ns = local_ns();

    for i in 0..5 {
        let e = depth(
            400 + i,
            399 + i,
            399 + i,
            &[("50000.10", "1.0")],
            &[("50000.20", "1.0")],
        );
        recorder.record_depth_event(&e, ns);
    }

    recorder.request_shutdown();
    handle.join().await.unwrap();

    // Retries must recover every row — none dropped.
    let db = inner
        .as_any()
        .downcast_ref::<MemoryStorage>()
        .unwrap()
        .snapshot_db();
    assert_eq!(db.level_changes.len(), 10);
    let metrics = recorder.metrics.lock().unwrap();
    assert_eq!(metrics.level_changes_stored, 10);
    assert!(
        metrics.retries >= 2,
        "expected at least two retried attempts"
    );
    drop(metrics);
}

#[tokio::test]
async fn test_queue_overflow_marks_degraded() {
    let inner: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    // Storage that never succeeds quickly keeps the worker busy, so the bounded
    // channel overflows and the recorder degrades instead of blocking.
    let stuck = Arc::new(FlakyStorage::new(inner.clone(), 1_000_000));
    let mut config = RecordingConfig::new(1000, 60000, 16);
    config.retry_backoff = Duration::from_millis(50);

    let session = SessionRecord::new(
        "BTCUSDT",
        "Binance",
        "USDⓈ-M",
        "PERPETUAL",
        "wss://fstream.binance.com/ws/btcusdt@depth@100ms",
        "wss://fstream.binance.com/ws/btcusdt@trade",
        "test",
        "testcommit",
    );
    let storage: Arc<dyn Storage> = stuck.clone();
    storage.insert_session(&session).await.unwrap();
    let (recorder, handle) = start_recorder(storage, session, config);
    let ns = local_ns();

    // Burst far more events than the queue capacity while the worker is stuck.
    for i in 0..2000 {
        let e = depth(
            500 + i,
            499 + i,
            499 + i,
            &[("50000.10", "1.0")],
            &[("50000.20", "1.0")],
        );
        recorder.record_depth_event(&e, ns);
    }

    // Give the worker a moment to fill the channel and mark degraded.
    tokio::time::sleep(Duration::from_millis(100)).await;

    {
        let health = recorder.health.lock().unwrap();
        assert!(health.degraded, "queue overflow must mark storage DEGRADED");
        assert!(health.queue_overflows > 0);
    }

    {
        let metrics = recorder.metrics.lock().unwrap();
        assert!(
            metrics.depth_dropped > 0,
            "expected dropped events under overflow"
        );
    }
    recorder.request_shutdown();

    handle.join().await.unwrap();
}
