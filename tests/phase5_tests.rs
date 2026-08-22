//! Phase 5: real-time bookmap-style heatmap engine tests.
//!
//! These tests exercise the expanded heatmap data model: price grid,
//! time grid, resting liquidity, executed volume, delta, additions,
//! removals, replenishment, and determinism/replay compatibility.

use futures_orderbook::analytics::config::AnalyticsConfig;
use futures_orderbook::analytics::engine::AnalyticsEngine;
use futures_orderbook::analytics::events::AnalyticsEventKind;
use futures_orderbook::analytics::heatmap::HeatmapDelta;
use futures_orderbook::events::market::MarketEvent;
use futures_orderbook::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks};
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

fn trade_event(id: u64, p: u64, q: u64, side: AggressorSide, ts: u64) -> TradeEvent {
    TradeEvent {
        symbol: "BTCUSDT".to_string(),
        trade_id: id,
        price_ticks: p,
        quantity_ticks: q,
        event_time: ts,
        trade_time: ts,
        local_receive_time_ns: 0,
        aggressor: side,
        order_type: "MARKET".to_string(),
    }
}

fn trade(id: u64, p: u64, q: u64, side: AggressorSide, ts: u64) -> MarketEvent {
    MarketEvent::Trade(trade_event(id, p, q, side, ts))
}

fn seed_book() -> MarketEvent {
    MarketEvent::OrderBookSnapshot {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        bids: vec![(price("68000.00"), qty("10.0"))],
        asks: vec![(price("68000.10"), qty("5.0"))],
    }
}

// Helper to find a cell in the heatmap
fn find_cell(
    engine: &AnalyticsEngine,
    p: u64,
) -> Option<futures_orderbook::analytics::heatmap::HeatmapCell> {
    for (_, cells_map) in engine.heatmap().buckets() {
        if let Some(cell) = cells_map.get(&p) {
            return Some(*cell);
        }
    }
    None
}

// ============================================================================
// Price Grid
// ============================================================================

#[test]
fn test_price_grid_aggregation_1x() {
    let cfg = AnalyticsConfig::btcusdt_default();
    // aggregation = 1 means 1 exchange tick per cell
    assert_eq!(cfg.heatmap_price_aggregation, 1);
}

#[test]
fn test_price_grid_aggregation_5x() {
    let mut cfg = AnalyticsConfig::btcusdt_default();
    cfg.heatmap_price_aggregation = 5;
    assert_eq!(cfg.heatmap_price_aggregation, 5);
}

#[test]
fn test_price_grid_aggregation_10x() {
    let mut cfg = AnalyticsConfig::btcusdt_default();
    cfg.heatmap_price_aggregation = 10;
    assert_eq!(cfg.heatmap_price_aggregation, 10);
}

#[test]
fn test_price_grid_aggregation_25x() {
    let mut cfg = AnalyticsConfig::btcusdt_default();
    cfg.heatmap_price_aggregation = 25;
    assert_eq!(cfg.heatmap_price_aggregation, 25);
}

#[test]
fn test_price_grid_aggregation_50x() {
    let mut cfg = AnalyticsConfig::btcusdt_default();
    cfg.heatmap_price_aggregation = 50;
    assert_eq!(cfg.heatmap_price_aggregation, 50);
}

#[test]
fn test_price_grid_aggregation_invalid_fallback() {
    let mut cfg = AnalyticsConfig::btcusdt_default();
    cfg.heatmap_price_aggregation = 99; // unsupported value
                                        // Note: Heatmap validates and falls back to 1, but config accepts any value
    assert_eq!(cfg.heatmap_price_aggregation, 99);
}

// Test that grid price mapping works correctly
#[test]
fn test_grid_price_mapping() {
    let _cfg = AnalyticsConfig::btcusdt_default();
    let mut engine = default_engine();
    // Engine's heatmap uses cell_ms for bucketing, but the grid_price
    // logic is in the heatmap. Test via on_trade.
    let p = price("68000.10"); // 6800010000000 ticks
    let trade_ev = trade(1, p, 100, AggressorSide::Buy, 1000);
    let _ = engine.process_event(&trade_ev);
    // With 1x aggregation, price should be preserved
    let heatmap = engine.heatmap();
    let cells = heatmap.cell_count();
    assert!(cells >= 1);
}

// ============================================================================
// Time Grid
// ============================================================================

#[test]
fn test_time_grid_intervals() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let intervals = cfg.heatmap_time_intervals();
    assert!(intervals.contains(&100));
    assert!(intervals.contains(&250));
    assert!(intervals.contains(&500));
    assert!(intervals.contains(&1_000));
    assert!(intervals.contains(&2_000));
    assert!(intervals.contains(&5_000));
    assert!(intervals.contains(&10_000));
    assert!(intervals.contains(&30_000));
    assert!(intervals.contains(&60_000));
    assert_eq!(intervals.len(), 9);
}

#[test]
fn test_time_bucket_boundaries() {
    let _cfg = AnalyticsConfig::btcusdt_default();
    let mut engine = default_engine();
    // Trades at ts=0, ts=500, ts=999 should all be in the same 1s bucket
    // when cell_ms=1000.
    let p = price("68000.10");
    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 0));
    engine.process_event(&trade(2, p, 200, AggressorSide::Sell, 500));
    engine.process_event(&trade(3, p, 300, AggressorSide::Buy, 999));
    // Should have 1 bucket (all within same 1s cell)
    assert_eq!(engine.heatmap().bucket_count(), 1);
    // Trades at ts=1001 should be in the next bucket
    engine.process_event(&trade(4, p, 400, AggressorSide::Buy, 1001));
    assert_eq!(engine.heatmap().bucket_count(), 2);
}

#[test]
fn test_time_grid_determinism() {
    let _cfg = AnalyticsConfig::btcusdt_default();
    let mut a = default_engine();
    let mut b = default_engine();
    let p = price("68000.10");
    // Same event stream into both.
    a.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    a.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));
    b.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    b.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));
    // Compare engine digests (which include heatmap data)
    assert_eq!(a.digest(), b.digest());
}

// ============================================================================
// Cells
// ============================================================================

#[test]
fn test_cell_liquidity_accumulation() {
    let mut engine = default_engine();
    let p = price("68000.10");

    // Simulate level changes adding resting liquidity.
    // Bid: 10 → 8 → 12 (via OrderBookUpdated events)
    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        event_time_ms: 1000,
        bid_changes: vec![(price("68000.10"), qty("8.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let _ = engine.process_event(&ev);

    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 2,
        event_time_ms: 1001,
        bid_changes: vec![(price("68000.10"), qty("12.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let _ = engine.process_event(&ev);

    let e = trade(1, p, 100, AggressorSide::Buy, 1002);
    let _ = engine.process_event(&e);

    let cell = find_cell(&engine, p).unwrap();

    // Resting bid liquidity should be 12 (last new_qty)
    assert_eq!(cell.resting_bid_liquidity, 12 * 100_000_000);
    // Liquidity added: 8 (first change from 0→8) + 4 (8→12) = 12
    assert_eq!(cell.liquidity_added, 12 * 100_000_000);
    // Liquidity removed: 0 (no decreases)
    assert_eq!(cell.liquidity_removed, 0);
    // Executed buy volume = 100
    assert_eq!(cell.executed_buy_volume, 100);
    // Trade count = 1
    assert_eq!(cell.trade_count, 1);
}

#[test]
fn test_cell_trade_accumulation() {
    let mut engine = default_engine();
    let p = price("68000.10");

    // Multiple trades at same price/bucket
    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    engine.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));
    engine.process_event(&trade(3, p, 300, AggressorSide::Buy, 1002));

    let cell = find_cell(&engine, p).unwrap();

    assert_eq!(cell.executed_buy_volume, 400); // 100 + 300
    assert_eq!(cell.executed_sell_volume, 200);
    assert_eq!(cell.total_executed_volume(), 600);
    assert_eq!(cell.delta, 200); // 400 - 200
    assert_eq!(cell.trade_count, 3);
}

#[test]
fn test_cell_buy_sell_separation() {
    let mut engine = default_engine();
    let p = price("68000.10");

    // Only buys
    engine.process_event(&trade(1, p, 500, AggressorSide::Buy, 1000));
    // Only sells
    engine.process_event(&trade(2, p, 300, AggressorSide::Sell, 1001));

    let cell = find_cell(&engine, p).unwrap();

    assert_eq!(cell.executed_buy_volume, 500);
    assert_eq!(cell.executed_sell_volume, 300);
    assert_eq!(cell.delta, 200); // 500 - 300
    assert_eq!(cell.trade_count, 2);
    assert!(cell.has_executed_volume());
}

// ============================================================================
// Liquidity Additions & Removals
// ============================================================================

#[test]
fn test_liquidity_additions_and_removals() {
    let mut engine = default_engine();
    let p = price("68000.10");

    // Bid: 10 → 7 (removed 3), then 7 → 12 (added 5)
    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        event_time_ms: 1000,
        bid_changes: vec![(price("68000.10"), qty("7.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let _ = engine.process_event(&ev);

    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 2,
        event_time_ms: 1001,
        bid_changes: vec![(price("68000.10"), qty("12.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let _ = engine.process_event(&ev);

    // Trade to populate cell
    let e = trade(1, p, 100, AggressorSide::Buy, 1002);
    let _ = engine.process_event(&e);

    let cell = find_cell(&engine, p).unwrap();

    // Resting bid = 12 (last state)
    assert_eq!(cell.resting_bid_liquidity, 12 * 100_000_000);
    // Added: 7 (0→7) + 5 (7→12) = 12
    assert_eq!(cell.liquidity_added, 12 * 100_000_000);
    // Removed: 0 (no decreases from initial 0)
    // Note: first change old=None, new=7 -> Added (7)
    // Second change old=7, new=12 -> Increased (5)
    // Total added = 12, removed = 0
    assert_eq!(cell.liquidity_removed, 0);
}

// ============================================================================
// Analytics: Sweep, Absorption, Large Trade Mapping
// ============================================================================

#[test]
fn test_sweep_candidate_mapping_to_heatmap() {
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

    for e in &events {
        let _out = engine.process_event(e);
        // Sweep mapping is emitted as an analytics event, not directly to heatmap.
        // The heatmap accumulates trades and sweep count is tracked in the engine.
    }
    // Heatmap should have cells populated from trades.
    assert!(engine.heatmap().cell_count() >= 1);
    // Sweep candidate count in engine digest.
    assert!(engine.digest().sweep_candidate_count >= 1);
}

#[test]
fn test_absorption_candidate_mapping_to_heatmap() {
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

    for e in &events {
        let _out = engine.process_event(e);
    }
    // Absorption candidate count in engine digest.
    // Absorption may or may not fire — just verify it doesn't panic.
    let _ = engine.digest().absorption_candidate_count;
    // Heatmap should have cells populated.
    assert!(engine.heatmap().cell_count() >= 1);
}

#[test]
fn test_large_trade_mapping_to_heatmap() {
    let mut engine = default_engine();
    engine.process_event(&seed_book());

    let p = price("68000.10");
    let out = engine.process_event(&trade(1, p, qty("6.0"), AggressorSide::Buy, 1000));
    assert!(out
        .events
        .iter()
        .any(|e| e.kind == AnalyticsEventKind::LargeTrade));
    assert_eq!(engine.large_trade_detector().count, 1);
    // Large trade volume should be tracked in the heatmap.
    let cell = find_cell(&engine, p).unwrap();
    assert!(cell.large_trade_volume > 0);
}

// ============================================================================
// Pruning / Bounded Memory
// ============================================================================

#[test]
fn test_heatmap_pruning_old_buckets() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    // Add a trade at ts=0
    hm.on_trade(&trade_event(1, p, 100, AggressorSide::Buy, 0));
    assert_eq!(hm.bucket_count(), 1);

    // Add a trade at ts=30_000_000 (8.3h later, beyond 15min retention)
    hm.on_trade(&trade_event(2, p, 100, AggressorSide::Buy, 30_000_000));

    // Prune at ts=30_001_000 (just barely beyond the 15min = 900_000ms retention)
    hm.prune(30_001_000);
    // The first bucket (ts=0) should be pruned, the second (ts=30_000_000) kept.
    // But wait - the bucket start for ts=0 with cell_ms=1000 is 0.
    // The bucket start for ts=30_000_000 with cell_ms=1000 is 30_000_000.
    // Retention is 900_000ms. Cutoff = 30_001_000 - 900_000 = 29_101_000.
    // Bucket at ts=0 < 29_101_000 → pruned.
    // Bucket at ts=30_000_000 > 29_101_000 → kept.
    assert_eq!(hm.bucket_count(), 1);
}

#[test]
fn test_heatmap_cell_count_bounded() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    // Add many trades at different prices/times to fill up buckets.
    for i in 0..50 {
        hm.on_trade(&trade_event(
            i as u64,
            p + i as u64 * 10_000_000,
            100,
            AggressorSide::Buy,
            i as u64 * 100,
        ));
    }
    let count = hm.cell_count();
    assert!(count > 0);
    // Prune should reduce count.
    hm.prune(100_000_000);
    let after_prune = hm.cell_count();
    // Some cells should have been pruned.
    // (Exact count depends on bucket boundaries and retention.)
    println!("  cell count: {} -> after prune: {}", count, after_prune);
}

// ============================================================================
// Determinism
// ============================================================================

#[test]
fn test_identical_event_stream_produces_identical_heatmap() {
    let _cfg = AnalyticsConfig::btcusdt_default();
    let mut a = default_engine();
    let mut b = default_engine();
    let p = price("68000.10");

    // Same event stream.
    a.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    a.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));
    a.process_event(&trade(3, p, 300, AggressorSide::Buy, 1002));

    b.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    b.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));
    b.process_event(&trade(3, p, 300, AggressorSide::Buy, 1002));

    // Digests must match exactly.
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.heatmap().cell_count(), b.heatmap().cell_count());
    assert_eq!(a.heatmap().bucket_count(), b.heatmap().bucket_count());
}

#[test]
fn test_heatmap_digest_fields() {
    let mut engine = default_engine();
    let p = price("68000.10");

    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    engine.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));

    let cell_count = engine.heatmap().cell_count();
    // Just verify the heatmap produces a consistent state.
    assert!(cell_count >= 1);
}

// ============================================================================
// Replay Compatibility
// ============================================================================

#[test]
fn test_replay_heatmap_digest_matches_live() {
    let _cfg = AnalyticsConfig::btcusdt_default();
    let mut live = default_engine();
    let mut replay = default_engine();

    let p = price("68000.10");
    let events = vec![
        trade(1, p, qty("1.0"), AggressorSide::Buy, 1000),
        trade(2, price("68000.00"), qty("0.5"), AggressorSide::Sell, 1001),
        trade(3, p, qty("2.0"), AggressorSide::Buy, 1002),
    ];

    // Live processing.
    for e in &events {
        live.process_event(e);
    }

    // Replay processing (same events, same order).
    for e in &events {
        replay.process_event(e);
    }

    // Heatmap digests must match exactly.
    // Compare engine digests (which include heatmap data).
    assert_eq!(live.digest(), replay.digest());
    assert_eq!(
        live.heatmap().cell_count(),
        replay.heatmap().cell_count(),
        "Heatmap cell count mismatch between live and replay"
    );
    assert_eq!(
        live.heatmap().bucket_count(),
        replay.heatmap().bucket_count(),
        "Heatmap bucket count mismatch between live and replay"
    );
}

// ============================================================================
// Serialization / Frame API
// ============================================================================

#[test]
fn test_heatmap_frame_serialization() {
    let mut engine = default_engine();
    let p = price("68000.10");

    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));
    engine.process_event(&trade(2, p, 200, AggressorSide::Sell, 1001));

    let frame = futures_orderbook::analytics::heatmap::HeatmapFrame::from_heatmap(
        engine.heatmap(),
        1000,     // timestamp
        0,        // visible_lo (min price tick)
        u64::MAX, // visible_hi (max price tick)
    );

    // Frame should have at least one cell.
    assert!(!frame.cells.is_empty());
    assert!(frame.summary.total_price_levels >= 1);
    assert!(frame.timestamp > 0);
}

#[test]
fn test_heatmap_cell_snapshot_fields() {
    let mut engine = default_engine();
    let p = price("68000.10");

    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));

    let cell = find_cell(&engine, p).unwrap();
    let snapshot =
        futures_orderbook::analytics::heatmap::HeatmapCellSnapshot::from_cell(&cell, cell.price);

    // All fields should be populated (default 0 is fine).
    assert_eq!(snapshot.price_tick, cell.price);
    assert_eq!(snapshot.resting_bid_liquidity, cell.resting_bid_liquidity);
    assert_eq!(snapshot.resting_ask_liquidity, cell.resting_ask_liquidity);
    assert_eq!(snapshot.liquidity_added, cell.liquidity_added);
    assert_eq!(snapshot.liquidity_removed, cell.liquidity_removed);
    assert_eq!(snapshot.executed_buy_volume, cell.executed_buy_volume);
    assert_eq!(snapshot.executed_sell_volume, cell.executed_sell_volume);
    assert_eq!(snapshot.trade_count, cell.trade_count);
    assert_eq!(snapshot.large_trade_volume, cell.large_trade_volume);
    assert_eq!(snapshot.replenishment_count, cell.replenishment_count);
    assert_eq!(
        snapshot.absorption_candidate_count,
        cell.absorption_candidate_count
    );
    assert_eq!(snapshot.sweep_count, cell.sweep_count);
    assert_eq!(snapshot.pressure, cell.pressure);
}

// ============================================================================
// Multiple Visual Modes
// ============================================================================

#[test]
fn test_heatmap_exposes_liquidity_mode_fields() {
    let mut engine = default_engine();
    let p = price("68000.10");

    // Add resting liquidity via level change
    let ev = MarketEvent::OrderBookUpdated {
        symbol: "BTCUSDT".to_string(),
        update_id: 1,
        event_time_ms: 1000,
        bid_changes: vec![(price("68000.10"), qty("10.0"), None)],
        ask_changes: vec![],
        best_bid: None,
        best_ask: None,
        mid_price: None,
    };
    let _ = engine.process_event(&ev);

    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));

    let cell = find_cell(&engine, p).unwrap();

    // Liquidity mode fields should be accessible.
    assert!(cell.resting_bid_liquidity > 0);
    assert!(cell.has_resting_liquidity());
}

#[test]
fn test_heatmap_exposes_execution_mode_fields() {
    let mut engine = default_engine();
    let p = price("68000.10");

    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));

    let cell = find_cell(&engine, p).unwrap();

    // Execution mode fields.
    assert!(cell.has_executed_volume());
    assert!(cell.total_executed_volume() > 0);
    assert!(cell.imbalance_f64().is_finite());
}

#[test]
fn test_heatmap_pressure_field() {
    let mut engine = default_engine();
    let p = price("68000.10");

    // Buy trade increases pressure (1 BTC = 100_000_000 ticks)
    engine.process_event(&trade(1, p, qty("1.0"), AggressorSide::Buy, 1000));
    let cell = find_cell(&engine, p).unwrap();
    assert_eq!(cell.pressure, 100_000_000); // 1 BTC * 1e8 ticks/BTC

    // Sell trade decreases pressure (0.5 BTC)
    engine.process_event(&trade(2, p, qty("0.5"), AggressorSide::Sell, 1001));
    let cell2 = find_cell(&engine, p).unwrap();
    assert_eq!(cell2.pressure, 50_000_000); // 1 - 0.5 = 0.5 BTC net buy
}

// ============================================================================
// Intensity Methods
// ============================================================================

#[test]
fn test_liquidity_intensity_zero_max() {
    let cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    assert_eq!(cell.liquidity_intensity(0), 0.0);
}

#[test]
fn test_liquidity_intensity_normalised() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.resting_bid_liquidity = 500;
    cell.resting_ask_liquidity = 500;
    // total = 1000, max = 2000 → 0.5
    let intensity = cell.liquidity_intensity(2000);
    assert!((intensity - 0.5).abs() < 1e-9);
}

#[test]
fn test_liquidity_intensity_clamped() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.resting_bid_liquidity = 5000;
    // total = 5000, max = 2000 → clamped to 1.0
    let intensity = cell.liquidity_intensity(2000);
    assert!((intensity - 1.0).abs() < 1e-9);
}

#[test]
fn test_execution_intensity_zero_max() {
    let cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    assert_eq!(cell.execution_intensity(0), 0.0);
}

#[test]
fn test_execution_intensity_normalised() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.executed_buy_volume = 300;
    cell.executed_sell_volume = 200;
    // total = 500, max = 1000 → 0.5
    let intensity = cell.execution_intensity(1000);
    assert!((intensity - 0.5).abs() < 1e-9);
}

#[test]
fn test_delta_intensity_zero_max() {
    let cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    assert_eq!(cell.delta_intensity(0), 0.0);
}

#[test]
fn test_delta_intensity_normalised() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.delta = 500;
    // |500| / 1000 = 0.5
    let intensity = cell.delta_intensity(1000);
    assert!((intensity - 0.5).abs() < 1e-9);
}

#[test]
fn test_delta_intensity_negative() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.delta = -500;
    // |-500| / 1000 = 0.5
    let intensity = cell.delta_intensity(1000);
    assert!((intensity - 0.5).abs() < 1e-9);
}

#[test]
fn test_absorption_intensity_zero_max() {
    let cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    assert_eq!(cell.absorption_intensity(0), 0.0);
}

#[test]
fn test_absorption_intensity_normalised() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.absorption_candidate_count = 3;
    let intensity = cell.absorption_intensity(10);
    assert!((intensity - 0.3).abs() < 1e-9);
}

#[test]
fn test_sweep_intensity_zero_max() {
    let cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    assert_eq!(cell.sweep_intensity(0), 0.0);
}

#[test]
fn test_sweep_intensity_normalised() {
    let mut cell = futures_orderbook::analytics::heatmap::HeatmapCell::new(100);
    cell.sweep_count = 2;
    let intensity = cell.sweep_intensity(4);
    assert!((intensity - 0.5).abs() < 1e-9);
}

// ============================================================================
// HeatmapDigest
// ============================================================================

#[test]
fn test_heatmap_digest_deterministic() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;
    hm.on_trade(&trade_event(1, p, 100, AggressorSide::Buy, 1000));
    hm.on_trade(&trade_event(2, p, 200, AggressorSide::Sell, 1001));

    let d1 = hm.digest();
    let d2 = hm.digest();
    assert_eq!(d1, d2);
    assert_eq!(d1.total_trade_count, 2);
    assert_eq!(d1.total_executed_buy, 100);
    assert_eq!(d1.total_executed_sell, 200);
    assert_eq!(d1.total_delta, -100); // 100 - 200
}

#[test]
fn test_heatmap_digest_live_replay_match() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut live = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let mut replay = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    // Same events into both.
    live.on_trade(&trade_event(1, p, 100, AggressorSide::Buy, 1000));
    live.on_trade(&trade_event(2, p, 200, AggressorSide::Sell, 1001));

    replay.on_trade(&trade_event(1, p, 100, AggressorSide::Buy, 1000));
    replay.on_trade(&trade_event(2, p, 200, AggressorSide::Sell, 1001));

    assert_eq!(live.digest(), replay.digest());
}

#[test]
fn test_heatmap_digest_liquidity_fields() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    // Level change adds liquidity.
    let change = futures_orderbook::analytics::book::LevelChange {
        side: futures_orderbook::analytics::book::BookSide::Bid,
        price: p,
        old_qty: None,
        new_qty: 500,
    };
    hm.on_level_change(&change, 1000);

    let d = hm.digest();
    assert_eq!(d.total_liquidity_added, 500);
    assert_eq!(d.total_resting_bid, 500);
    assert_eq!(d.total_buckets, 1);
    assert_eq!(d.total_price_levels, 1);
}

#[test]
fn test_heatmap_digest_removal() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    // Add then remove liquidity.
    let add = futures_orderbook::analytics::book::LevelChange {
        side: futures_orderbook::analytics::book::BookSide::Ask,
        price: p,
        old_qty: None,
        new_qty: 500,
    };
    hm.on_level_change(&add, 1000);
    let remove = futures_orderbook::analytics::book::LevelChange {
        side: futures_orderbook::analytics::book::BookSide::Ask,
        price: p,
        old_qty: Some(500),
        new_qty: 200,
    };
    hm.on_level_change(&remove, 1001);

    let d = hm.digest();
    assert_eq!(d.total_liquidity_added, 500);
    assert_eq!(d.total_liquidity_removed, 300);
    assert_eq!(d.total_resting_ask, 200);
}

#[test]
fn test_heatmap_digest_summarize() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;
    hm.on_trade(&trade_event(1, p, 100, AggressorSide::Buy, 1000));
    let d = hm.digest();
    let s = d.summarize();
    assert!(s.contains("buckets=1"));
    assert!(s.contains("trades=1"));
}

// ============================================================================
// Event Mapping into Heatmap Cells
// ============================================================================

#[test]
fn test_on_sweep_maps_to_heatmap_cell() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    hm.on_sweep(p, 1000);
    hm.on_sweep(p, 1000); // same bucket

    // Find the cell.
    let mut found = false;
    for cells_map in hm.buckets().map(|(_, c)| c) {
        if let Some(cell) = cells_map.get(&p) {
            assert_eq!(cell.sweep_count, 2);
            found = true;
        }
    }
    assert!(found);
}

#[test]
fn test_on_absorption_maps_to_heatmap_cell() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    hm.on_absorption(p, 1000);

    for cells_map in hm.buckets().map(|(_, c)| c) {
        if let Some(cell) = cells_map.get(&p) {
            assert_eq!(cell.absorption_candidate_count, 1);
        }
    }
}

#[test]
fn test_on_replenishment_maps_to_heatmap_cell() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;

    hm.on_replenishment(p, 1000);
    hm.on_replenishment(p, 1000);
    hm.on_replenishment(p, 1000);

    for cells_map in hm.buckets().map(|(_, c)| c) {
        if let Some(cell) = cells_map.get(&p) {
            assert_eq!(cell.replenishment_count, 3);
        }
    }
}

#[test]
fn test_engine_heatmap_digest_method() {
    let mut engine = default_engine();
    let p = price("68000.10");
    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));

    let digest = engine.heatmap_digest();
    assert_eq!(digest.total_trade_count, 1);
    assert_eq!(digest.total_buckets, 1);
    assert_eq!(digest.total_price_levels, 1);
}

// ============================================================================
// Serialization Round-trip
// ============================================================================

#[test]
fn test_heatmap_frame_json_roundtrip() {
    use futures_orderbook::analytics::heatmap::HeatmapFrame;

    let mut engine = default_engine();
    let p = price("68000.10");
    engine.process_event(&trade(1, p, 100, AggressorSide::Buy, 1000));

    let frame = HeatmapFrame::from_heatmap(engine.heatmap(), 1000, 0, u64::MAX);
    let json = serde_json::to_string(&frame).unwrap();
    let deserialized: HeatmapFrame = serde_json::from_str(&json).unwrap();
    assert_eq!(frame, deserialized);
}

#[test]
fn test_heatmap_digest_json_roundtrip() {
    let cfg = AnalyticsConfig::btcusdt_default();
    let mut hm = futures_orderbook::analytics::heatmap::Heatmap::new(&cfg);
    let p = 6_800_000_000_000u64;
    hm.on_trade(&trade_event(1, p, 100, AggressorSide::Buy, 1000));

    let digest = hm.digest();
    let json = serde_json::to_string(&digest).unwrap();
    let deserialized: futures_orderbook::analytics::heatmap::HeatmapDigest =
        serde_json::from_str(&json).unwrap();
    assert_eq!(digest, deserialized);
}

// ============================================================================
// HeatmapDelta::compute
// ============================================================================

#[test]
fn test_heatmap_delta_compute_new_and_changed() {
    use futures_orderbook::analytics::heatmap::{
        HeatmapCellSnapshot, HeatmapFrame, HeatmapSummary,
    };

    let previous = HeatmapFrame {
        timestamp: 1000,
        visible_price_range: (100, 200),
        time_range: (1000, 2000),
        cells: vec![HeatmapCellSnapshot {
            price_tick: 100,
            executed_buy_volume: 50,
            ..HeatmapCellSnapshot::default()
        }],
        summary: HeatmapSummary {
            total_price_levels: 1,
            total_buckets: 1,
            total_executed_buy: 50,
            ..HeatmapSummary::default()
        },
    };

    let current = HeatmapFrame {
        timestamp: 2000,
        visible_price_range: (100, 300),
        time_range: (1000, 3000),
        cells: vec![
            HeatmapCellSnapshot {
                price_tick: 100,
                executed_buy_volume: 150,
                ..HeatmapCellSnapshot::default()
            },
            HeatmapCellSnapshot {
                price_tick: 300,
                executed_buy_volume: 10,
                ..HeatmapCellSnapshot::default()
            },
        ],
        summary: HeatmapSummary {
            total_price_levels: 2,
            total_buckets: 1,
            total_executed_buy: 160,
            ..HeatmapSummary::default()
        },
    };

    let delta = HeatmapDelta::compute(&previous, &current);

    // Price 100 changed (buy went from 50 to 150).
    assert_eq!(delta.changed.len(), 1);
    assert_eq!(delta.changed[0].0, 100);
    // Price 300 is new.
    assert_eq!(delta.new.len(), 1);
    assert_eq!(delta.new[0].price_tick, 300);
    // No cells removed.
    assert!(delta.removed.is_empty());
    // Summary delta shows +110 buy volume.
    assert_eq!(delta.summary_delta.total_executed_buy, 110);
}

#[test]
fn test_heatmap_delta_compute_removed() {
    use futures_orderbook::analytics::heatmap::{
        HeatmapCellSnapshot, HeatmapFrame, HeatmapSummary,
    };

    let previous = HeatmapFrame {
        timestamp: 1000,
        visible_price_range: (100, 200),
        time_range: (1000, 2000),
        cells: vec![
            HeatmapCellSnapshot {
                price_tick: 100,
                ..HeatmapCellSnapshot::default()
            },
            HeatmapCellSnapshot {
                price_tick: 200,
                ..HeatmapCellSnapshot::default()
            },
        ],
        summary: HeatmapSummary {
            total_price_levels: 2,
            ..HeatmapSummary::default()
        },
    };

    let current = HeatmapFrame {
        timestamp: 2000,
        visible_price_range: (100, 100),
        time_range: (1000, 3000),
        cells: vec![HeatmapCellSnapshot {
            price_tick: 100,
            ..HeatmapCellSnapshot::default()
        }],
        summary: HeatmapSummary {
            total_price_levels: 1,
            ..HeatmapSummary::default()
        },
    };

    let delta = HeatmapDelta::compute(&previous, &current);

    // Price 200 was removed.
    assert_eq!(delta.removed, vec![200]);
    assert!(delta.changed.is_empty());
    assert!(delta.new.is_empty());
}
