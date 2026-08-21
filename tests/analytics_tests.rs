//! Phase 4 analytics integration tests.
//!
//! These exercise the [`AnalyticsEngine`] end-to-end over synthetic
//! `MarketEvent` streams (deterministic — no network) and the persistence
//! pipeline through `MemoryStorage`.

use std::sync::Arc;

use futures_orderbook::analytics::config::AnalyticsConfig;
use futures_orderbook::analytics::engine::AnalyticsEngine;
use futures_orderbook::analytics::events::AnalyticsEventKind;
use futures_orderbook::events::market::MarketEvent;
use futures_orderbook::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks};
use futures_orderbook::storage::{
    start_analytics_sink, AnalyticsBatch, AnalyticsSnapshotRow, MemoryStorage, Storage,
};
use futures_orderbook::trades::trade::{AggressorSide, TradeEvent};

// ============================================================================
// Helpers
// ============================================================================

fn default_engine() -> AnalyticsEngine {
    AnalyticsEngine::new(AnalyticsConfig::btcusdt_default())
}

fn price(s: &str) -> u64 {
    price_str_to_ticks(s).unwrap()
}

fn qty(s: &str) -> u64 {
    quantity_str_to_ticks(s).unwrap()
}

fn trade(id: u64, p: u64, q: u64, side: AggressorSide, ts: u64) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        trade_id: id,
        price_ticks: p,
        quantity_ticks: q,
        event_time: ts,
        trade_time: ts,
        local_receive_time_ns: 0,
        aggressor: side,
        order_type: "MARKET".to_string(),
    })
}

/// A realistic seeded book: best bid 68000.00 (10 BTC), best ask 68000.10
/// (5 BTC).
fn seed_book() -> MarketEvent {
    MarketEvent::OrderBookSnapshot {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        bids: vec![(price("68000.00"), qty("10.0"))],
        asks: vec![(price("68000.10"), qty("5.0"))],
    }
}

/// Run an engine over a sequence of events, collecting all snapshots.
fn run_events(
    engine: &mut AnalyticsEngine,
    events: &[MarketEvent],
) -> Vec<futures_orderbook::analytics::snapshot::MarketMicrostructureSnapshot> {
    let mut snaps = Vec::new();
    for e in events {
        let out = engine.process_event(e);
        snaps.extend(out.snapshots);
    }
    if let Some(s) = engine.force_snapshot() {
        snaps.push(s);
    }
    snaps
}

// ============================================================================
// Delta / CVD
// ============================================================================

#[test]
fn test_cvd_and_delta_accumulate() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    // BUY 2 BTC, SELL 1 BTC, BUY 4 BTC → cvd = +2 -1 +4 = +5.
    let events = vec![
        trade(1, price("68000.10"), qty("2.0"), AggressorSide::Buy, 1000),
        trade(2, price("68000.00"), qty("1.0"), AggressorSide::Sell, 1001),
        trade(3, price("68000.10"), qty("4.0"), AggressorSide::Buy, 1002),
    ];
    let snaps = run_events(&mut engine, &events);

    let last = snaps.last().expect("a snapshot");
    // cvd is session-cumulative; interval fields reset at the first snapshot
    // (fired on the first trade at t=1000).
    assert_eq!(last.cvd, 5 * 100_000_000);
    assert_eq!(last.delta, 3 * 100_000_000);
    assert_eq!(last.trade_volume, 5 * 100_000_000);
    assert_eq!(last.buy_volume, 4 * 100_000_000);
    assert_eq!(last.sell_volume, 100_000_000);
}

#[test]
fn test_volume_by_price_profile() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    // Three trades at 68000.10, two at 68000.00.
    let events = vec![
        trade(1, price("68000.10"), qty("1.0"), AggressorSide::Buy, 1000),
        trade(2, price("68000.10"), qty("2.0"), AggressorSide::Sell, 1001),
        trade(3, price("68000.00"), qty("3.0"), AggressorSide::Buy, 1002),
        trade(4, price("68000.00"), qty("4.0"), AggressorSide::Sell, 1003),
        trade(5, price("68000.10"), qty("5.0"), AggressorSide::Buy, 1004),
    ];
    let _ = run_events(&mut engine, &events);

    let vp = engine.flow().volume_at(price("68000.10")).unwrap();
    assert_eq!(vp.total_volume, 8 * 100_000_000);
    assert_eq!(vp.buy_volume, 6 * 100_000_000);
    assert_eq!(vp.sell_volume, 2 * 100_000_000);
    assert_eq!(vp.delta, 4 * 100_000_000);

    let vp = engine.flow().volume_at(price("68000.00")).unwrap();
    assert_eq!(vp.total_volume, 7 * 100_000_000);
    assert_eq!(vp.delta, -100_000_000);
}

// ============================================================================
// Imbalance & microprice
// ============================================================================

#[test]
fn test_imbalance_and_microprice_exact() {
    let mut engine = default_engine();
    // bid 10 BTC @ 68000.00, ask 5 BTC @ 68000.10.
    engine.process_event(&seed_book());
    let events = vec![trade(
        1,
        price("68000.10"),
        qty("1.0"),
        AggressorSide::Buy,
        1000,
    )];
    let snaps = run_events(&mut engine, &events);

    let snap = snaps.first().unwrap();
    assert_eq!(snap.best_bid, Some(price("68000.00")));
    assert_eq!(snap.best_ask, Some(price("68000.10")));
    let imb = snap.book_imbalance.expect("imbalance");
    assert!((imb - 5.0 / 15.0).abs() < 1e-9);

    let mp = snap.microprice_f64().expect("microprice");
    // (68000.10*10 + 68000.00*5)/15 = (680001 + 340000)/15 = 1020001/15 = 68000.066...
    assert!((mp - 68_000.066666).abs() < 0.001);
    assert_eq!(snap.bid_depth, qty("10.0"));
    assert_eq!(snap.ask_depth, qty("5.0"));
}

// ============================================================================
// Liquidity tracking (5 → 8 → 2)
// ============================================================================

#[test]
fn test_liquidity_changes_emitted() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());
    // Establish the snapshot baseline with a trade so the two updates below
    // land in a single interval (no mid-stream snapshot reset).
    engine.process_event(&trade(
        1,
        price("68000.10"),
        qty("0.1"),
        AggressorSide::Buy,
        1000,
    ));

    // Bid 68000.00: 10 → 8 → 2.
    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 2,
        event_time_ms: 1900,
        bid_changes: vec![(price("68000.00"), qty("8.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let out = engine.process_event(&ev);
    let kinds: Vec<AnalyticsEventKind> = out.events.iter().map(|e| e.kind).collect();
    assert!(kinds.contains(&AnalyticsEventKind::LiquidityDecreased));

    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 3,
        event_time_ms: 1901,
        bid_changes: vec![(price("68000.00"), qty("2.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let out = engine.process_event(&ev);
    let kinds: Vec<AnalyticsEventKind> = out.events.iter().map(|e| e.kind).collect();
    assert!(kinds.contains(&AnalyticsEventKind::LiquidityDecreased));

    // 10 → 8 → 2 net removed = 8 BTC.
    assert_eq!(engine.liquidity_tracker().removed_ticks, 8 * 100_000_000);
}

#[test]
fn test_replenishment_within_window_emitted() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    // 68000.10 ask: 5 → 2 (decrease), then 2 → 9 within 250ms.
    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 2,
        event_time_ms: 3000,
        bid_changes: vec![],
        ask_changes: vec![(price("68000.10"), qty("2.0"), None)],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let _ = engine.process_event(&ev);

    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 3,
        event_time_ms: 3100,
        bid_changes: vec![],
        ask_changes: vec![(price("68000.10"), qty("9.0"), None)],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let out = engine.process_event(&ev);
    assert!(out
        .events
        .iter()
        .any(|e| e.kind == AnalyticsEventKind::LiquidityReplenishment));
    assert_eq!(engine.liquidity_tracker().replenishment_count, 1);
}

// ============================================================================
// Sweep
// ============================================================================

#[test]
fn test_sweep_detected_from_cluster() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    // Aggressive BUY at 4 consecutive levels (0.10 apart) within 100ms.
    let p = price("68000.00");
    let mut events = Vec::new();
    for i in 0..4 {
        events.push(trade(
            i + 1,
            p + i * 10_000_000,
            qty("2.0"),
            AggressorSide::Buy,
            4000 + i * 10,
        ));
    }
    // A far-away trade closes the cluster.
    events.push(trade(
        99,
        price("69000.00"),
        qty("0.1"),
        AggressorSide::Sell,
        5000,
    ));

    let mut found = false;
    for e in &events {
        let out = engine.process_event(e);
        found |= out
            .events
            .iter()
            .any(|ev| ev.kind == AnalyticsEventKind::SweepCandidate);
    }
    assert!(found, "expected a sweep candidate");
    assert!(engine.digest().sweep_candidate_count >= 1);
}

#[test]
fn test_same_price_not_sweep() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    // 4 trades at the SAME level → not a sweep.
    let p = price("68000.10");
    let mut events = Vec::new();
    for i in 0..4 {
        events.push(trade(
            i + 1,
            p,
            qty("2.0"),
            AggressorSide::Buy,
            6000 + i * 10,
        ));
    }
    events.push(trade(
        99,
        price("69000.00"),
        qty("0.1"),
        AggressorSide::Sell,
        7000,
    ));

    let mut found = false;
    for e in &events {
        let out = engine.process_event(e);
        found |= out
            .events
            .iter()
            .any(|ev| ev.kind == AnalyticsEventKind::SweepCandidate);
    }
    assert!(!found, "same-price cluster must not be a sweep");
}

// ============================================================================
// Absorption
// ============================================================================

#[test]
fn test_absorption_detected_no_displacement() {
    let mut engine = default_engine();
    // Ask sits at 68000.10; aggressive SELL hits the bid at 68000.00 while the
    // best ask does not move (no favorable displacement for the seller).
    engine.process_event(&seed_book());

    let p = price("68000.00");
    let mut events = Vec::new();
    for i in 0..5 {
        events.push(trade(
            i + 1,
            p,
            qty("5.0"),
            AggressorSide::Sell,
            8000 + i * 5,
        ));
    }
    // A later trade at the same price (after the 1000ms window) triggers
    // evaluation of the expired window.
    events.push(trade(99, p, qty("0.1"), AggressorSide::Sell, 10_000));

    let mut found = false;
    for e in &events {
        let out = engine.process_event(e);
        found |= out
            .events
            .iter()
            .any(|ev| ev.kind == AnalyticsEventKind::AbsorptionCandidate);
    }
    assert!(found, "expected an absorption candidate");
}

#[test]
fn test_absorption_fails_on_displacement() {
    let mut engine = default_engine();
    // Wide book (10 ticks apart) so the best bid can move up without crossing.
    engine.process_event(&MarketEvent::OrderBookSnapshot {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        bids: vec![(price("67999.50"), qty("10.0"))],
        asks: vec![(price("68000.50"), qty("10.0"))],
    });

    // BUY aggressor hits 68000.00 while the best bid keeps advancing 2 ticks
    // per trade → max favorable displacement quickly exceeds the 3-tick
    // excursion, so the flow was NOT absorbed.
    let p = price("68000.00");
    let mut events = Vec::new();
    for i in 0..5 {
        let ev = MarketEvent::OrderBookUpdated {
            symbol: "BTCUSDT".to_string(),
            update_id: 10 + i,
            event_time_ms: 9000 + i * 5,
            bid_changes: vec![(
                price("67999.50") + (i + 1) * 2 * 10_000_000,
                qty("10.0"),
                None,
            )],
            ask_changes: vec![],
            best_bid: None,
            best_ask: None,
            mid_price: None,
        };
        engine.process_event(&ev);
        events.push(trade(
            i + 1,
            p,
            qty("5.0"),
            AggressorSide::Buy,
            9000 + i * 5,
        ));
    }
    events.push(trade(99, p, qty("0.1"), AggressorSide::Buy, 12_000));

    let mut found = false;
    for e in &events {
        let out = engine.process_event(e);
        found |= out
            .events
            .iter()
            .any(|ev| ev.kind == AnalyticsEventKind::AbsorptionCandidate);
    }
    assert!(!found, "displacement beyond excursion must not be absorbed");
}

// ============================================================================
// Large trades
// ============================================================================

#[test]
fn test_large_trade_detected() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());
    let out = engine.process_event(&trade(
        1,
        price("68000.10"),
        qty("6.0"),
        AggressorSide::Buy,
        1000,
    ));
    assert!(out
        .events
        .iter()
        .any(|e| e.kind == AnalyticsEventKind::LargeTrade));
    assert_eq!(engine.large_trade_detector().count, 1);
    assert_eq!(engine.digest().large_trade_count, 1);
}

// ============================================================================
// Crossed book
// ============================================================================

#[test]
fn test_crossed_book_anomaly() {
    let mut engine = default_engine();
    let out = engine.process_event(&MarketEvent::OrderBookSnapshot {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        bids: vec![(price("68000.00"), qty("1.0"))],
        asks: vec![(price("67999.90"), qty("1.0"))],
    });
    assert!(out
        .events
        .iter()
        .any(|e| e.kind == AnalyticsEventKind::BookAnomaly));
    assert!(engine.book.is_crossed());

    let events = vec![trade(
        1,
        price("68000.10"),
        qty("1.0"),
        AggressorSide::Buy,
        1000,
    )];
    let snaps = run_events(&mut engine, &events);
    let snap = snaps.first().unwrap();
    assert!(snap.book_crossed);
    // No microprice/spread/imbalance on a crossed book.
    assert_eq!(snap.spread_ticks, None);
    assert_eq!(snap.book_imbalance, None);
    assert_eq!(snap.microprice_num, None);
}

// ============================================================================
// Determinism
// ============================================================================

#[test]
fn test_replay_twice_identical_digest() {
    let events = {
        let mut v = vec![seed_book()];
        v.push(trade(
            1,
            price("68000.10"),
            qty("1.0"),
            AggressorSide::Buy,
            1000,
        ));
        v.push(trade(
            2,
            price("68000.00"),
            qty("0.5"),
            AggressorSide::Sell,
            1001,
        ));
        v.push(trade(
            3,
            price("68000.10"),
            qty("2.0"),
            AggressorSide::Buy,
            1002,
        ));
        let ev = MarketEvent::OrderBookUpdated {
            symbol: "BTCUSDT".to_string(),
            update_id: 2,
            event_time_ms: 1003,
            bid_changes: vec![(price("68000.00"), qty("9.0"), None)],
            ask_changes: vec![],
            best_bid: None,
            best_ask: None,
            mid_price: None,
        };
        v.push(ev);
        v
    };

    let mut a = default_engine();
    let mut b = default_engine();
    for e in &events {
        a.process_event(e);
        b.process_event(e);
    }
    assert_eq!(a.digest(), b.digest());
    // Sanity on the expected values.
    assert_eq!(a.digest().trade_count, 3);
    // BUY 1 + BUY 2 − SELL 0.5 = +2.5 BTC.
    assert_eq!(a.digest().cvd, 250_000_000);
}

// ============================================================================
// Persistence through MemoryStorage
// ============================================================================

#[tokio::test]
async fn test_analytics_sink_persists_to_memory() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let (sink, handle) = start_analytics_sink(storage.clone(), 10);

    let mut engine = default_engine();
    engine.process_event(&seed_book());
    let out = engine.process_event(&trade(
        1,
        price("68000.10"),
        qty("6.0"),
        AggressorSide::Buy,
        1000,
    ));
    let _ = out;

    let session_id = uuid::Uuid::new_v4();
    let mut batch = AnalyticsBatch::default();
    if let Some(snap) = engine.force_snapshot() {
        batch
            .snapshots
            .push(AnalyticsSnapshotRow::from_snapshot(&snap, session_id));
    }
    sink.submit(batch);

    // Sink worker flush happens when the channel closes (handle dropped) or
    // on capacity; drop the sender and join.
    drop(sink);
    handle.join().await;

    let snaps = storage.read_analytics_snapshots(session_id).await.unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].cvd, 6 * 100_000_000);
    assert_eq!(
        storage.count_analytics_snapshots(session_id).await.unwrap(),
        1
    );
}

#[tokio::test]
async fn test_storage_insert_read_roundtrip() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let session_id = uuid::Uuid::new_v4();

    let mut engine = default_engine();
    engine.process_event(&seed_book());
    let out = engine.process_event(&trade(
        1,
        price("68000.10"),
        qty("6.0"),
        AggressorSide::Buy,
        1000,
    ));
    let snap = engine.force_snapshot().unwrap();

    let mut batch = futures_orderbook::storage::AnalyticsBatch::default();
    batch
        .snapshots
        .push(AnalyticsSnapshotRow::from_snapshot(&snap, session_id));
    batch.events.extend(
        out.events
            .iter()
            .map(|e| futures_orderbook::storage::AnalyticsEventRow::from_event(e, session_id)),
    );
    storage
        .insert_analytics_snapshots(&batch.snapshots)
        .await
        .unwrap();
    storage
        .insert_analytics_events(&batch.events)
        .await
        .unwrap();

    let snaps = storage.read_analytics_snapshots(session_id).await.unwrap();
    assert_eq!(snaps.len(), 1);
    let events = storage.read_analytics_events(session_id).await.unwrap();
    assert!(!events.is_empty());
    assert!(events.iter().any(|e| e.kind == "large_trade"));
}

// ============================================================================
// Snapshots & time buckets
// ============================================================================

#[test]
fn test_snapshot_interval_produces_snapshots() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    // Trades at 1000ms and 2100ms (default snapshot interval 1000ms).
    let e1 = trade(1, price("68000.10"), qty("1.0"), AggressorSide::Buy, 1000);
    let e2 = trade(2, price("68000.00"), qty("1.0"), AggressorSide::Sell, 2100);
    let snaps = run_events(&mut engine, &[e1, e2]);

    assert!(snaps.len() >= 3, "snapshot + final force snapshot");
    let first = &snaps[0];
    assert_eq!(first.cvd, 100_000_000);
}

#[test]
fn test_cvd_is_session_cumulative() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());
    // Two intervals; the second snapshot's cvd must include the first.
    let e1 = trade(1, price("68000.10"), qty("2.0"), AggressorSide::Buy, 1000);
    let e2 = trade(2, price("68000.10"), qty("3.0"), AggressorSide::Buy, 2100);
    let snaps = run_events(&mut engine, &[e1, e2]);

    let first = &snaps[0];
    assert_eq!(first.cvd, 2 * 100_000_000);
    let any_second = snaps
        .iter()
        .find(|s| s.timestamp_ms >= 2100)
        .expect("second interval snapshot");
    assert_eq!(any_second.cvd, 5 * 100_000_000);
}

#[test]
fn test_heatmap_cell_count_nonzero() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());
    let e = trade(1, price("68000.10"), qty("1.0"), AggressorSide::Buy, 1000);
    let _ = run_events(&mut engine, &[e]);
    assert!(engine.heatmap().cell_count() >= 1);
}
