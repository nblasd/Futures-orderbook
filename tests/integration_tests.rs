use futures_orderbook::binance::types::DepthUpdate;
use futures_orderbook::orderbook::book::OrderBook;
use futures_orderbook::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks, TICK_SCALE};
use futures_orderbook::orderbook::synchronizer::{ProcessResult, SyncState, Synchronizer};

// ============================================================================
// Helper functions
// ============================================================================

#[allow(non_snake_case, dead_code, clippy::too_many_arguments)]
fn make_depth_update(
    event_type: &str,
    event_time: u64,
    transaction_time: u64,
    symbol: &str,
    U: u64,
    u: u64,
    pu: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
) -> DepthUpdate {
    DepthUpdate {
        event_type: event_type.to_string(),
        event_time,
        transaction_time,
        symbol: symbol.to_string(),
        first_update_id: U,
        final_update_id: u,
        previous_final_update_id: pu,
        bids,
        asks,
    }
}

#[allow(non_snake_case)]
fn simple_update(U: u64, u: u64, pu: u64) -> DepthUpdate {
    make_depth_update(
        "depthUpdate",
        1234567890,
        1234567890,
        "BTCUSDT",
        U,
        u,
        pu,
        vec![],
        vec![],
    )
}

#[allow(non_snake_case)]
fn update_with_bids(U: u64, u: u64, pu: u64, bids: Vec<(&str, &str)>) -> DepthUpdate {
    make_depth_update(
        "depthUpdate",
        1234567890,
        1234567890,
        "BTCUSDT",
        U,
        u,
        pu,
        bids.into_iter()
            .map(|(p, q)| (p.to_string(), q.to_string()))
            .collect(),
        vec![],
    )
}

#[allow(non_snake_case, dead_code)]
fn update_with_asks(U: u64, u: u64, pu: u64, asks: Vec<(&str, &str)>) -> DepthUpdate {
    make_depth_update(
        "depthUpdate",
        1234567890,
        1234567890,
        "BTCUSDT",
        U,
        u,
        pu,
        vec![],
        asks.into_iter()
            .map(|(p, q)| (p.to_string(), q.to_string()))
            .collect(),
    )
}

// ============================================================================
// Test Group 1: Basic Book Tests (no sync needed)
// ============================================================================

#[test]
fn test_01_empty_book() {
    let book = OrderBook::new();
    assert!(!book.is_initialized());
    assert_eq!(book.best_bid(), None);
    assert_eq!(book.best_ask(), None);
    assert_eq!(book.mid_price(), None);
    assert_eq!(book.spread(), None);
    assert_eq!(book.bid_count(), 0);
    assert_eq!(book.ask_count(), 0);
    assert_eq!(book.last_update_id(), 0);
}

#[test]
fn test_02_snapshot_creates_bids() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("50000.10".to_string(), "1.500".to_string()),
        ("49999.90".to_string(), "2.000".to_string()),
    ];
    book.apply_snapshot(&bids, &[], 100).unwrap();
    assert!(book.is_initialized());
    assert_eq!(book.bid_count(), 2);
    assert_eq!(
        book.best_bid(),
        Some(price_str_to_ticks("50000.10").unwrap())
    );
    assert_eq!(book.last_update_id(), 100);
}

#[test]
fn test_03_snapshot_creates_asks() {
    let mut book = OrderBook::new();
    let asks = vec![
        ("50000.20".to_string(), "0.500".to_string()),
        ("50000.30".to_string(), "3.000".to_string()),
    ];
    book.apply_snapshot(&[], &asks, 200).unwrap();
    assert!(book.is_initialized());
    assert_eq!(book.ask_count(), 2);
    assert_eq!(
        book.best_ask(),
        Some(price_str_to_ticks("50000.20").unwrap())
    );
    assert_eq!(book.last_update_id(), 200);
}

#[test]
fn test_04_insert_new_bid() {
    let mut book = OrderBook::new();
    let bids = vec![("50000.10".to_string(), "1.000".to_string())];
    book.apply_snapshot(&bids, &[], 100).unwrap();

    let new_bids = vec![("50001.00".to_string(), "0.500".to_string())];
    book.apply_depth_update(&new_bids, &[], 101).unwrap();
    assert_eq!(
        book.best_bid(),
        Some(price_str_to_ticks("50001.00").unwrap())
    );
    assert_eq!(book.bid_count(), 2);
    assert_eq!(book.last_update_id(), 101);
}

#[test]
fn test_05_insert_new_ask() {
    let mut book = OrderBook::new();
    let asks = vec![("50001.00".to_string(), "1.000".to_string())];
    book.apply_snapshot(&[], &asks, 100).unwrap();

    let new_asks = vec![("50000.50".to_string(), "0.500".to_string())];
    book.apply_depth_update(&[], &new_asks, 101).unwrap();
    assert_eq!(
        book.best_ask(),
        Some(price_str_to_ticks("50000.50").unwrap())
    );
    assert_eq!(book.ask_count(), 2);
}

#[test]
fn test_06_update_existing_bid() {
    let mut book = OrderBook::new();
    let bids = vec![("50000.10".to_string(), "1.000".to_string())];
    book.apply_snapshot(&bids, &[], 100).unwrap();

    let update = vec![("50000.10".to_string(), "2.500".to_string())];
    book.apply_depth_update(&update, &[], 101).unwrap();
    assert_eq!(book.bid_count(), 1);
    let levels = book.bid_levels(1);
    assert_eq!(levels[0].quantity, quantity_str_to_ticks("2.500").unwrap());
}

#[test]
fn test_07_update_existing_ask() {
    let mut book = OrderBook::new();
    let asks = vec![("50001.00".to_string(), "1.000".to_string())];
    book.apply_snapshot(&[], &asks, 100).unwrap();

    let update = vec![("50001.00".to_string(), "3.000".to_string())];
    book.apply_depth_update(&[], &update, 101).unwrap();
    assert_eq!(book.ask_count(), 1);
    let levels = book.ask_levels(1);
    assert_eq!(levels[0].quantity, quantity_str_to_ticks("3.000").unwrap());
}

#[test]
fn test_08_quantity_zero_removes_bid() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("50000.10".to_string(), "1.000".to_string()),
        ("49999.90".to_string(), "2.000".to_string()),
    ];
    book.apply_snapshot(&bids, &[], 100).unwrap();
    assert_eq!(book.bid_count(), 2);

    let removal = vec![("50000.10".to_string(), "0".to_string())];
    book.apply_depth_update(&removal, &[], 101).unwrap();
    assert_eq!(book.bid_count(), 1);
    assert_eq!(
        book.best_bid(),
        Some(price_str_to_ticks("49999.90").unwrap())
    );
}

#[test]
fn test_09_quantity_zero_removes_ask() {
    let mut book = OrderBook::new();
    let asks = vec![
        ("50000.20".to_string(), "0.500".to_string()),
        ("50000.30".to_string(), "3.000".to_string()),
    ];
    book.apply_snapshot(&[], &asks, 100).unwrap();
    assert_eq!(book.ask_count(), 2);

    let removal = vec![("50000.20".to_string(), "0".to_string())];
    book.apply_depth_update(&[], &removal, 101).unwrap();
    assert_eq!(book.ask_count(), 1);
    assert_eq!(
        book.best_ask(),
        Some(price_str_to_ticks("50000.30").unwrap())
    );
}

#[test]
fn test_10_best_bid_is_highest() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("49998.00".to_string(), "1.000".to_string()),
        ("50000.10".to_string(), "2.000".to_string()),
        ("49999.50".to_string(), "3.000".to_string()),
    ];
    book.apply_snapshot(&bids, &[], 100).unwrap();
    assert_eq!(
        book.best_bid(),
        Some(price_str_to_ticks("50000.10").unwrap())
    );
}

#[test]
fn test_11_best_ask_is_lowest() {
    let mut book = OrderBook::new();
    let asks = vec![
        ("50001.50".to_string(), "1.000".to_string()),
        ("50000.20".to_string(), "2.000".to_string()),
        ("50002.00".to_string(), "3.000".to_string()),
    ];
    book.apply_snapshot(&[], &asks, 100).unwrap();
    assert_eq!(
        book.best_ask(),
        Some(price_str_to_ticks("50000.20").unwrap())
    );
}

#[test]
fn test_12_mid_price() {
    let mut book = OrderBook::new();
    let bids = vec![("50000.00".to_string(), "1.000".to_string())];
    let asks = vec![("50001.00".to_string(), "1.000".to_string())];
    book.apply_snapshot(&bids, &asks, 100).unwrap();
    let mid = book.mid_price().unwrap();
    assert!((mid - 50000.50).abs() < 0.01);
}

#[test]
fn test_13_spread() {
    let mut book = OrderBook::new();
    let bids = vec![("50000.00".to_string(), "1.000".to_string())];
    let asks = vec![("50000.50".to_string(), "1.000".to_string())];
    book.apply_snapshot(&bids, &asks, 100).unwrap();
    let spread = book.spread().unwrap();
    let expected =
        price_str_to_ticks("50000.50").unwrap() - price_str_to_ticks("50000.00").unwrap();
    assert_eq!(spread, expected);
}

// ============================================================================
// Test Group 2: Sequence Handling Tests
// ============================================================================

#[test]
fn test_14_valid_first_event_after_snapshot() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();

    // Buffer some events
    sync.buffer_event(simple_update(151, 155, 150));
    sync.buffer_event(simple_update(156, 160, 155));

    // Snapshot has lastUpdateId = 155
    let events = sync.reconcile(155).unwrap();

    // Event u=155: U=151 <= 155 AND u=155 >= 155 → bridge found
    assert!(!events.is_empty());
    assert_eq!(events[0].final_update_id, 155);
}

#[test]
fn test_15_valid_sequential_event() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    // Buffer an event that bridges snapshot=100 (U=95<=100, u=105>=100)
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap(); // first applied u=105

    // First event after sync: pu must be 105
    let event = simple_update(106, 110, 105);
    let result = sync.process_live_event(&event);
    assert_eq!(result, ProcessResult::Apply);

    // Second event with correct pu
    let event2 = simple_update(111, 115, 110);
    let result2 = sync.process_live_event(&event2);
    assert_eq!(result2, ProcessResult::Apply);
}

#[test]
fn test_16_stale_event_ignored() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap(); // first applied u=105

    // Now try a stale event (u=100 <= last_applied_u=105, but correct pu)
    let stale = make_depth_update(
        "depthUpdate",
        1234567890,
        1234567890,
        "BTCUSDT",
        100,
        100,
        105,
        vec![],
        vec![],
    ); // U=100, u=100, pu=105
    let result = sync.process_live_event(&stale);
    assert_eq!(result, ProcessResult::Stale);
    assert_eq!(sync.events_ignored(), 1);
}

#[test]
fn test_17_duplicate_event_handled_safely() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap(); // first applied u=105

    // Apply a live event (pu=105 matches, u=110 > 105)
    let event = simple_update(106, 110, 105);
    sync.process_live_event(&event); // last_applied_u=110

    // Try the exact same event again - pu=105 != last_applied_u=110 → stale
    // Actually, a true duplicate has same u as last_applied_u
    let duplicate = simple_update(106, 110, 110); // pu=110 matches, u=110 <= 110 → stale
    let result = sync.process_live_event(&duplicate);
    assert_eq!(result, ProcessResult::Stale);
}

#[test]
fn test_18_sequence_gap_detected() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap(); // first applied u=105

    // Event with wrong pu (pu should be 105)
    let gap_event = make_depth_update(
        "depthUpdate",
        1234567890,
        1234567890,
        "BTCUSDT",
        106,
        110,
        9999, // Wrong pu!
        vec![],
        vec![],
    );
    let result = sync.process_live_event(&gap_event);
    assert_eq!(result, ProcessResult::PuMismatch);
    assert_eq!(sync.sequence_errors(), 1);
}

#[test]
fn test_19_pu_continuity_failure_detected() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap(); // first applied u=105

    // Now send an event with mismatched pu
    let bad_event = make_depth_update(
        "depthUpdate",
        1234567890,
        1234567890,
        "BTCUSDT",
        106,
        110,
        500, // Should be 105
        vec![],
        vec![],
    );
    let result = sync.process_live_event(&bad_event);
    assert_eq!(result, ProcessResult::PuMismatch);
}

#[test]
fn test_20_resync_state_triggered() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap();
    assert_eq!(sync.state(), SyncState::Ready);

    sync.trigger_resync();
    assert_eq!(sync.state(), SyncState::Buffering);
    assert_eq!(sync.resync_count(), 1);
}

#[test]
fn test_21_buffered_events_correctly_reconciled() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();

    // Buffer events that span across a snapshot boundary
    // Event u=98: already covered by snapshot
    sync.buffer_event(simple_update(95, 98, 94));
    // Event u=101: bridges snapshot (U=99 <= 100, u=101 >= 100)
    sync.buffer_event(simple_update(99, 101, 98));
    // Event u=105: subsequent event (pu should be 101)
    sync.buffer_event(simple_update(102, 105, 101));

    // Snapshot lastUpdateId=100
    let events = sync.reconcile(100).unwrap();

    // Events with u < 100 are discarded
    // Event u=101 is the bridge
    // Event u=105 follows with correct pu
    assert!(events.len() >= 2);
    assert_eq!(events[0].final_update_id, 101);
    assert_eq!(events[1].final_update_id, 105);
}

#[test]
fn test_22_new_snapshot_replaces_stale_state() {
    let mut book = OrderBook::new();
    let bids = vec![("50000.00".to_string(), "1.000".to_string())];
    book.apply_snapshot(&bids, &[], 100).unwrap();
    assert_eq!(book.bid_count(), 1);
    assert_eq!(book.last_update_id(), 100);

    // Apply some updates
    let update = vec![("50001.00".to_string(), "2.000".to_string())];
    book.apply_depth_update(&update, &[], 150).unwrap();
    assert_eq!(book.bid_count(), 2);

    // New snapshot completely replaces state
    let new_bids = vec![("49000.00".to_string(), "5.000".to_string())];
    book.apply_snapshot(&new_bids, &[], 200).unwrap();
    assert_eq!(book.bid_count(), 1);
    assert_eq!(
        book.best_bid(),
        Some(price_str_to_ticks("49000.00").unwrap())
    );
    assert_eq!(book.last_update_id(), 200);
}

// ============================================================================
// Test Group 3: Precision Tests
// ============================================================================

#[test]
fn test_23_identical_price_strings_same_level() {
    let p1 = price_str_to_ticks("50000.10").unwrap();
    let p2 = price_str_to_ticks("50000.10").unwrap();
    assert_eq!(p1, p2);

    let mut book = OrderBook::new();
    book.apply_snapshot(&[("50000.10".to_string(), "1.0".to_string())], &[], 100)
        .unwrap();
    assert_eq!(book.bid_count(), 1);

    // Update with same price string
    book.apply_depth_update(&[("50000.10".to_string(), "2.0".to_string())], &[], 101)
        .unwrap();
    assert_eq!(book.bid_count(), 1); // Still one level, not two
}

#[test]
fn test_24_decimal_prices_no_fp_error() {
    // Classic floating point issue: 0.1 + 0.2 != 0.3
    let p1 = price_str_to_ticks("0.1").unwrap();
    let p2 = price_str_to_ticks("0.2").unwrap();
    let p3 = price_str_to_ticks("0.3").unwrap();

    // As ticks, these should be exact
    assert_eq!(p1, 10_000_000);
    assert_eq!(p2, 20_000_000);
    assert_eq!(p3, 30_000_000);
    assert_eq!(p1 + p2, p3);

    // More complex case
    let p4 = price_str_to_ticks("50000.10").unwrap();
    let p5 = price_str_to_ticks("50000.20").unwrap();
    let p6 = price_str_to_ticks("50000.30").unwrap();
    assert_eq!(p4 + (p5 - p4), p5);
    assert_eq!(p5 + (p6 - p5), p6);
}

#[test]
fn test_25_tick_size_handling_deterministic() {
    // All ticks should map deterministically
    let prices = [
        "0.01", "0.10", "1.00", "10.00", "100.00", "1000.00", "10000.00", "50000.10", "99999.99",
    ];

    for price in &prices {
        let t1 = price_str_to_ticks(price).unwrap();
        let t2 = price_str_to_ticks(price).unwrap();
        assert_eq!(t1, t2, "Price {} not deterministic", price);

        // Verify round-trip
        let display = format!("{:.2}", t1 as f64 / TICK_SCALE as f64);
        assert_eq!(display, *price, "Round-trip failed for {}", price);
    }
}

// ============================================================================
// Test Group 4: Invariant Tests
// ============================================================================

#[test]
fn test_26_best_bid_is_highest_bid() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("49000.00".to_string(), "1.0".to_string()),
        ("51000.00".to_string(), "2.0".to_string()),
        ("50000.00".to_string(), "3.0".to_string()),
    ];
    book.apply_snapshot(&bids, &[], 100).unwrap();

    let best = book.best_bid().unwrap();
    let all_bids = book.bid_levels(10);
    for level in &all_bids {
        assert!(
            level.price <= best,
            "Found bid level {} higher than best {}",
            level.price,
            best
        );
    }
    assert_eq!(best, price_str_to_ticks("51000.00").unwrap());
}

#[test]
fn test_27_best_ask_is_lowest_ask() {
    let mut book = OrderBook::new();
    let asks = vec![
        ("52000.00".to_string(), "1.0".to_string()),
        ("50000.00".to_string(), "2.0".to_string()),
        ("51000.00".to_string(), "3.0".to_string()),
    ];
    book.apply_snapshot(&[], &asks, 100).unwrap();

    let best = book.best_ask().unwrap();
    let all_asks = book.ask_levels(10);
    for level in &all_asks {
        assert!(
            level.price >= best,
            "Found ask level {} lower than best {}",
            level.price,
            best
        );
    }
    assert_eq!(best, price_str_to_ticks("50000.00").unwrap());
}

#[test]
fn test_28_zero_quantity_levels_absent() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("50000.00".to_string(), "1.0".to_string()),
        ("49999.00".to_string(), "2.0".to_string()),
    ];
    book.apply_snapshot(&bids, &[], 100).unwrap();

    // Remove one level
    book.apply_depth_update(&[("50000.00".to_string(), "0".to_string())], &[], 101)
        .unwrap();

    // Verify no zero-quantity levels exist
    let snapshot = book.snapshot();
    for level in &snapshot.bids {
        assert!(level.quantity > 0, "Found zero-quantity bid level");
    }
    for level in &snapshot.asks {
        assert!(level.quantity > 0, "Found zero-quantity ask level");
    }
}

#[test]
fn test_29_no_duplicate_price_levels() {
    let mut book = OrderBook::new();
    book.apply_snapshot(&[("50000.00".to_string(), "1.0".to_string())], &[], 100)
        .unwrap();

    // Apply multiple updates to same price
    book.apply_depth_update(&[("50000.00".to_string(), "2.0".to_string())], &[], 101)
        .unwrap();
    book.apply_depth_update(&[("50000.00".to_string(), "3.0".to_string())], &[], 102)
        .unwrap();

    assert_eq!(book.bid_count(), 1); // Should still be exactly one level
}

#[test]
fn test_30_ready_book_valid_sequencing() {
    let mut sync = Synchronizer::new();
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    sync.reconcile(100).unwrap(); // first applied u=105

    // Apply a chain of events with correct pu continuity
    let mut prev_u = 105u64;
    for i in 0..10 {
        let u_start = 106 + i * 5;
        let u_end = u_start + 4;
        let event = simple_update(u_start, u_end, prev_u);
        let result = sync.process_live_event(&event);
        assert_eq!(result, ProcessResult::Apply, "Event {} failed", i);
        prev_u = u_end;
    }

    // 1 event applied during reconcile + 10 in loop
    assert_eq!(sync.events_applied(), 11);
}

// ============================================================================
// Test Group 5: Additional Synchronization Tests
// ============================================================================

#[test]
fn test_full_synchronization_flow() {
    let mut book = OrderBook::new();
    let mut sync = Synchronizer::new();

    // Step 1: Connect
    sync.on_connecting();
    sync.on_connected();
    assert_eq!(sync.state(), SyncState::Buffering);

    // Step 2: Buffer events
    sync.buffer_event(update_with_bids(151, 155, 150, vec![("50000.10", "2.0")]));
    sync.buffer_event(update_with_bids(156, 160, 155, vec![("49999.50", "1.0")]));
    assert_eq!(sync.buffer_size(), 2);

    // Step 3: Fetch snapshot
    sync.on_snapshot_loading();
    assert_eq!(sync.state(), SyncState::SnapshotLoading);

    // Step 4: Apply snapshot
    let snapshot_bids = vec![
        ("50000.10".to_string(), "1.5".to_string()),
        ("50000.00".to_string(), "2.0".to_string()),
    ];
    let snapshot_asks = vec![("50000.20".to_string(), "0.5".to_string())];
    book.apply_snapshot(&snapshot_bids, &snapshot_asks, 155)
        .unwrap();

    // Step 5: Reconcile
    let events = sync.reconcile(155).unwrap();
    assert!(!events.is_empty());

    // Step 6: Apply reconciled events
    for event in &events {
        book.apply_depth_update(&event.bids, &event.asks, event.final_update_id)
            .unwrap();
    }

    assert_eq!(sync.state(), SyncState::Ready);
    assert!(book.is_initialized());

    // Step 7: Apply live events
    let live = update_with_bids(161, 165, 160, vec![("50000.10", "3.0")]);
    let result = sync.process_live_event(&live);
    assert_eq!(result, ProcessResult::Apply);
    book.apply_depth_update(&live.bids, &live.asks, live.final_update_id)
        .unwrap();
}

#[test]
fn test_resync_flow() {
    let mut book = OrderBook::new();
    let mut sync = Synchronizer::new();

    // Initial sync - event must bridge snapshot=100 (U<=100, u>=100)
    sync.on_connecting();
    sync.on_connected();
    sync.buffer_event(simple_update(95, 105, 94));
    sync.on_snapshot_loading();
    book.apply_snapshot(
        &[("50000.00".to_string(), "1.0".to_string())],
        &[("50001.00".to_string(), "1.0".to_string())],
        100,
    )
    .unwrap();
    sync.reconcile(100).unwrap();
    assert_eq!(sync.state(), SyncState::Ready);

    // Apply some events (pu=105 matches reconciled event's u)
    let event = simple_update(106, 110, 105);
    sync.process_live_event(&event);

    // Trigger resync (simulating a gap detection)
    sync.trigger_resync();
    assert_eq!(sync.state(), SyncState::Buffering);
    assert_eq!(sync.resync_count(), 1);

    // Buffer new events during resync
    sync.buffer_event(simple_update(200, 205, 199));

    // Re-fetch snapshot and reconcile
    sync.on_snapshot_loading();
    book.apply_snapshot(
        &[("50000.00".to_string(), "2.0".to_string())],
        &[("50001.00".to_string(), "2.0".to_string())],
        200,
    )
    .unwrap();
    let events = sync.reconcile(200).unwrap();
    for event in &events {
        book.apply_depth_update(&event.bids, &event.asks, event.final_update_id)
            .unwrap();
    }
    assert_eq!(sync.state(), SyncState::Ready);
}

#[test]
fn test_book_invariants_held_after_updates() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("50000.10".to_string(), "1.0".to_string()),
        ("49999.90".to_string(), "2.0".to_string()),
    ];
    let asks = vec![
        ("50000.20".to_string(), "0.5".to_string()),
        ("50000.50".to_string(), "1.5".to_string()),
    ];
    book.apply_snapshot(&bids, &asks, 100).unwrap();

    // Apply several updates
    book.apply_depth_update(
        &[("50000.10".to_string(), "3.0".to_string())],
        &[("50000.20".to_string(), "1.0".to_string())],
        101,
    )
    .unwrap();

    book.apply_depth_update(
        &[("49999.50".to_string(), "0.5".to_string())],
        &[("50000.60".to_string(), "0.5".to_string())],
        102,
    )
    .unwrap();

    book.apply_depth_update(
        &[("50000.10".to_string(), "0".to_string())],
        &[("50000.50".to_string(), "0".to_string())],
        103,
    )
    .unwrap();

    // Verify invariants
    book.verify_invariants().unwrap();
}

// ============================================================================
// Test Group 6: DepthUpdate JSON Deserialization
// ============================================================================

#[test]
fn test_deserialize_depth_update() {
    let json = r#"{
        "e": "depthUpdate",
        "E": 1234567890000,
        "T": 1234567890000,
        "s": "BTCUSDT",
        "U": 150,
        "u": 160,
        "pu": 149,
        "b": [["50000.10", "1.500"], ["49999.90", "0.000"]],
        "a": [["50000.20", "0.500"], ["50006.00", "0.750"]]
    }"#;

    let update: DepthUpdate = serde_json::from_str(json).unwrap();
    assert_eq!(update.event_type, "depthUpdate");
    assert_eq!(update.event_time, 1234567890000);
    assert_eq!(update.symbol, "BTCUSDT");
    assert_eq!(update.first_update_id, 150);
    assert_eq!(update.final_update_id, 160);
    assert_eq!(update.previous_final_update_id, 149);
    assert_eq!(update.bids.len(), 2);
    assert_eq!(update.asks.len(), 2);
    assert_eq!(update.bids[0].0, "50000.10");
    assert_eq!(update.bids[0].1, "1.500");
    assert_eq!(update.bids[1].1, "0.000"); // Zero quantity = removal
}

#[test]
fn test_deserialize_snapshot() {
    let json = r#"{
        "lastUpdateId": 160,
        "E": 1234567890000,
        "T": 1234567890000,
        "bids": [["50000.10", "1.500"], ["50000.00", "2.300"]],
        "asks": [["50000.20", "0.500"], ["50000.50", "1.800"]]
    }"#;

    let snapshot: futures_orderbook::binance::types::DepthSnapshot =
        serde_json::from_str(json).unwrap();
    assert_eq!(snapshot.last_update_id, 160);
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.asks.len(), 2);
    assert_eq!(snapshot.bids[0].0, "50000.10");
}

#[test]
fn test_zero_quantity_in_snapshot_filtered() {
    let mut book = OrderBook::new();
    let bids = vec![
        ("50000.10".to_string(), "1.0".to_string()),
        ("49999.00".to_string(), "0".to_string()), // Zero quantity
    ];
    book.apply_snapshot(&bids, &[], 100).unwrap();
    assert_eq!(book.bid_count(), 1); // Zero-quantity level should not be stored
}

// ============================================================================
// Test Group 7: Edge Cases
// ============================================================================

#[test]
fn test_removal_of_nonexistent_level() {
    // Per Binance docs: "Receiving an event that removes a price level that
    // is not in your local order book can happen and is normal."
    let mut book = OrderBook::new();
    book.apply_snapshot(&[("50000.00".to_string(), "1.0".to_string())], &[], 100)
        .unwrap();

    // Try to remove a level that doesn't exist
    let removal = vec![("99999.00".to_string(), "0".to_string())];
    book.apply_depth_update(&removal, &[], 101).unwrap();
    assert_eq!(book.bid_count(), 1); // Original level still there
}

#[test]
fn test_snapshot_replace_clears_all() {
    let mut book = OrderBook::new();
    book.apply_snapshot(
        &[
            ("50000.00".to_string(), "1.0".to_string()),
            ("49999.00".to_string(), "2.0".to_string()),
        ],
        &[
            ("50001.00".to_string(), "1.0".to_string()),
            ("50002.00".to_string(), "2.0".to_string()),
        ],
        100,
    )
    .unwrap();
    assert_eq!(book.bid_count(), 2);
    assert_eq!(book.ask_count(), 2);

    // New snapshot with different levels
    book.apply_snapshot(
        &[("48000.00".to_string(), "5.0".to_string())],
        &[("48001.00".to_string(), "5.0".to_string())],
        200,
    )
    .unwrap();
    assert_eq!(book.bid_count(), 1);
    assert_eq!(book.ask_count(), 1);
    assert_eq!(
        book.best_bid(),
        Some(price_str_to_ticks("48000.00").unwrap())
    );
    assert_eq!(
        book.best_ask(),
        Some(price_str_to_ticks("48001.00").unwrap())
    );
}

#[test]
fn test_multiple_bid_updates_same_price() {
    let mut book = OrderBook::new();
    book.apply_snapshot(&[("50000.00".to_string(), "1.0".to_string())], &[], 100)
        .unwrap();

    // Update same price multiple times
    book.apply_depth_update(&[("50000.00".to_string(), "2.0".to_string())], &[], 101)
        .unwrap();
    book.apply_depth_update(&[("50000.00".to_string(), "3.0".to_string())], &[], 102)
        .unwrap();
    book.apply_depth_update(&[("50000.00".to_string(), "4.0".to_string())], &[], 103)
        .unwrap();

    assert_eq!(book.bid_count(), 1);
    let levels = book.bid_levels(1);
    assert_eq!(levels[0].quantity, quantity_str_to_ticks("4.0").unwrap());
}

// ============================================================================
// Phase 2: Trade Ingestion Tests
// ============================================================================

use futures_orderbook::binance::trade_types::FuturesTrade;
use futures_orderbook::trades::normalizer::{normalize_trade, NormalizeResult};
use futures_orderbook::trades::trade::TradeEvent;

/// Helper: extract TradeEvent from NormalizeResult, panicking on non-Ok.
fn unwrap_trade(result: NormalizeResult) -> TradeEvent {
    match result {
        NormalizeResult::Ok(t) => t,
        other => panic!("Expected NormalizeResult::Ok, got {:?}", other),
    }
}
use futures_orderbook::trades::processor::{TradeProcessResult, TradeProcessor};
use futures_orderbook::trades::trade::AggressorSide;
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p
}

fn load_fixture(name: &str) -> String {
    let path = fixtures_dir().join(name);
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read fixture {}: {}", name, e))
}

fn make_raw_trade(trade_id: u64, price: &str, qty: &str, is_buyer_maker: bool) -> FuturesTrade {
    FuturesTrade {
        event_type: "trade".to_string(),
        event_time: 1787137583835,
        trade_time: 1787137583835,
        symbol: "BTCUSDT".to_string(),
        trade_id,
        price: price.to_string(),
        quantity: qty.to_string(),
        order_type: "MARKET".to_string(),
        is_buyer_maker,
        trade_type: 1,
    }
}

// --- Parsing tests ---

#[test]
fn test_29_parse_valid_buy_aggressor_fixture() {
    let json = load_fixture("trade_buy_aggressor.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    assert_eq!(trade.symbol, "BTCUSDT");
    assert_eq!(trade.trade_id, 7978350772);
    assert_eq!(trade.price, "64369.60");
    assert_eq!(trade.quantity, "0.002");
    assert!(!trade.is_buyer_maker);
}

#[test]
fn test_30_parse_valid_sell_aggressor_fixture() {
    let json = load_fixture("trade_sell_aggressor.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    assert_eq!(trade.symbol, "BTCUSDT");
    assert_eq!(trade.trade_id, 7978350773);
    assert_eq!(trade.price, "64369.70");
    assert_eq!(trade.quantity, "0.150");
    assert!(trade.is_buyer_maker);
}

#[test]
fn test_31_parse_invalid_json() {
    let json = load_fixture("trade_invalid.json");
    let result = serde_json::from_str::<FuturesTrade>(&json);
    assert!(result.is_err());
}

#[test]
fn test_32_parse_wrong_symbol() {
    let json = load_fixture("trade_wrong_symbol.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    assert_eq!(trade.symbol, "ETHUSDT"); // parsed but wrong symbol
}

#[test]
fn test_33_parse_bad_price() {
    let json = load_fixture("trade_bad_price.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    // Normalization should fail on bad price
    let result = normalize_trade(&trade);
    assert!(matches!(result, NormalizeResult::ParseError(_)));
}

#[test]
fn test_34_parse_small_trade_fixture() {
    let json = load_fixture("trade_small.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    assert_eq!(trade.quantity, "0.001");
}

#[test]
fn test_35_parse_large_trade_fixture() {
    let json = load_fixture("trade_large.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    assert_eq!(trade.quantity, "12.500");
}

// --- Normalization tests ---

#[test]
fn test_36_normalize_preserves_price_ticks() {
    let raw = make_raw_trade(1, "50000.10", "1.0", false);
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.price_ticks, price_str_to_ticks("50000.10").unwrap());
}

#[test]
fn test_37_normalize_preserves_quantity_ticks() {
    let raw = make_raw_trade(1, "100.00", "0.001", false);
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.quantity_ticks, 100_000); // 0.001 * 1e8
}

#[test]
fn test_38_normalize_preserves_timestamps() {
    let mut raw = make_raw_trade(1, "100.00", "0.01", false);
    raw.event_time = 12345;
    raw.trade_time = 67890;
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.event_time, 12345);
    assert_eq!(event.trade_time, 67890);
}

#[test]
fn test_39_normalize_preserves_trade_id() {
    let raw = make_raw_trade(9876543, "100.00", "0.01", false);
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.trade_id, 9876543);
}

#[test]
fn test_40_normalize_preserves_order_type() {
    let mut raw = make_raw_trade(1, "100.00", "0.01", false);
    raw.order_type = "LIMIT".to_string();
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.order_type, "LIMIT");
}

#[test]
fn test_41_normalize_local_receive_time_nonzero() {
    let raw = make_raw_trade(1, "100.00", "0.01", false);
    let event = unwrap_trade(normalize_trade(&raw));
    assert!(event.local_receive_time_ns > 0);
}

// --- Aggressor side tests ---

#[test]
fn test_42_buyer_maker_true_means_sell_aggressor() {
    let raw = make_raw_trade(1, "64000.00", "0.01", true);
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.aggressor, AggressorSide::Sell);
}

#[test]
fn test_43_buyer_maker_false_means_buy_aggressor() {
    let raw = make_raw_trade(1, "64000.00", "0.01", false);
    let event = unwrap_trade(normalize_trade(&raw));
    assert_eq!(event.aggressor, AggressorSide::Buy);
}

#[test]
fn test_44_aggressor_side_from_buyer_maker_direct() {
    assert_eq!(AggressorSide::from_buyer_maker(true), AggressorSide::Sell);
    assert_eq!(AggressorSide::from_buyer_maker(false), AggressorSide::Buy);
}

#[test]
fn test_45_aggressor_side_display() {
    assert_eq!(format!("{}", AggressorSide::Buy), "BUY");
    assert_eq!(format!("{}", AggressorSide::Sell), "SELL");
}

// --- Processor duplicate detection ---

#[test]
fn test_46_duplicate_trade_detected() {
    let mut proc = TradeProcessor::new();
    let raw = make_raw_trade(100, "64000.00", "0.01", false);
    let e1 = unwrap_trade(normalize_trade(&raw));
    let e2 = unwrap_trade(normalize_trade(&raw));
    assert_eq!(proc.process(e1), TradeProcessResult::Processed);
    assert_eq!(proc.process(e2), TradeProcessResult::Duplicate);
    assert_eq!(proc.duplicate_trades(), 1);
    assert_eq!(proc.trade_events_processed(), 1);
}

#[test]
fn test_47_stale_trade_detected() {
    let mut proc = TradeProcessor::new();
    let e1 = unwrap_trade(normalize_trade(&make_raw_trade(
        50, "64000.00", "0.01", false,
    )));
    let e2 = unwrap_trade(normalize_trade(&make_raw_trade(
        100, "64000.00", "0.01", false,
    )));
    let e3 = unwrap_trade(normalize_trade(&make_raw_trade(
        40, "64000.00", "0.01", false,
    )));
    assert_eq!(proc.process(e1), TradeProcessResult::Processed);
    assert_eq!(proc.process(e2), TradeProcessResult::Processed);
    assert_eq!(proc.process(e3), TradeProcessResult::Stale);
    assert_eq!(proc.stale_trades(), 1);
}

#[test]
fn test_48_aggressor_counts_in_processor() {
    let mut proc = TradeProcessor::new();
    let buy = unwrap_trade(normalize_trade(&make_raw_trade(
        1, "64000.00", "0.01", false,
    )));
    let sell = unwrap_trade(normalize_trade(&make_raw_trade(
        2, "64000.00", "0.01", true,
    )));
    proc.process(buy);
    proc.process(sell);
    assert_eq!(proc.buy_aggressor_count(), 1);
    assert_eq!(proc.sell_aggressor_count(), 1);
}

#[test]
fn test_49_sequential_trades_100() {
    let mut proc = TradeProcessor::new();
    for i in 1..=100 {
        let raw = make_raw_trade(i, "64000.00", "0.01", i % 2 == 0);
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(proc.process(event), TradeProcessResult::Processed);
    }
    assert_eq!(proc.trade_events_processed(), 100);
    assert_eq!(proc.last_trade_id(), Some(100));
}

#[test]
fn test_50_last_trade_stored() {
    let mut proc = TradeProcessor::new();
    assert!(proc.last_trade().is_none());
    let raw = make_raw_trade(42, "64000.00", "0.01", true);
    let event = unwrap_trade(normalize_trade(&raw));
    proc.process(event);
    let last = proc.last_trade().unwrap();
    assert_eq!(last.trade_id, 42);
    assert_eq!(last.aggressor, AggressorSide::Sell);
}

// --- Trade and order book are independent ---

#[test]
fn test_51_trade_does_not_affect_order_book() {
    let mut book = OrderBook::new();
    book.apply_snapshot(
        &[("50000.00".to_string(), "1.0".to_string())],
        &[("50001.00".to_string(), "1.0".to_string())],
        100,
    )
    .unwrap();

    let bid_before = book.best_bid();
    let ask_before = book.best_ask();

    // Process many trades — book must not change
    let mut proc = TradeProcessor::new();
    for i in 1..=10 {
        let raw = make_raw_trade(i, "50000.50", "0.001", i % 2 == 0);
        let event = unwrap_trade(normalize_trade(&raw));
        proc.process(event);
    }

    assert_eq!(book.best_bid(), bid_before);
    assert_eq!(book.best_ask(), ask_before);
    assert_eq!(proc.trade_events_processed(), 10);
}

// --- Config trade stream URL ---

#[test]
fn test_52_trade_stream_url_is_lowercase() {
    let config = futures_orderbook::config::Config::default();
    let url = config.trade_stream_url();
    assert!(
        url.ends_with("/ws/btcusdt@trade"),
        "URL should use lowercase: {}",
        url
    );
}

#[test]
fn test_53_depth_stream_url_is_lowercase() {
    let config = futures_orderbook::config::Config::default();
    let url = config.depth_stream_url();
    assert!(
        url.ends_with("/ws/btcusdt@depth@100ms"),
        "URL should use lowercase: {}",
        url
    );
}

// --- Marker event rejection tests ---

#[test]
fn test_54_marker_event_regression_fixture() {
    // Exact payload captured from live Binance Futures btcusdt@trade stream.
    // This is NOT a real trade — it is a synthetic marker with p:"0", q:"0", X:"NA".
    let json = load_fixture("trade_marker_event.json");
    let trade: FuturesTrade = serde_json::from_str(&json).unwrap();
    assert_eq!(trade.price, "0");
    assert_eq!(trade.quantity, "0");
    assert_eq!(trade.order_type, "NA");
    assert_eq!(trade.symbol, "BTCUSDT");
    assert_eq!(trade.trade_id, 7979198979);

    // Must be recognized as a marker event, NOT normalized into TradeEvent
    let result = normalize_trade(&trade);
    assert!(
        matches!(result, NormalizeResult::MarkerEvent(_)),
        "Marker event with p:0 q:0 X:NA must be rejected, got: {:?}",
        result
    );
}

#[test]
fn test_55_marker_event_never_produces_zero_price_ticks() {
    // Verify that no code path produces a TradeEvent with price_ticks=0
    let marker = FuturesTrade {
        event_type: "trade".to_string(),
        event_time: 1787150961497,
        trade_time: 1787150961497,
        symbol: "BTCUSDT".to_string(),
        trade_id: 7979198979,
        price: "0".to_string(),
        quantity: "0".to_string(),
        order_type: "NA".to_string(),
        is_buyer_maker: true,
        trade_type: 1,
    };
    let result = normalize_trade(&marker);
    match result {
        NormalizeResult::Ok(event) => {
            panic!(
                "Must not produce TradeEvent for marker. Got price_ticks={}",
                event.price_ticks
            );
        }
        NormalizeResult::MarkerEvent(_) => { /* expected */ }
        NormalizeResult::ParseError(e) => {
            panic!("ParseError is wrong type for marker: {}", e);
        }
    }
}

#[test]
fn test_56_all_real_trades_have_nonzero_price() {
    // Simulate the full normalization + processing pipeline
    let real_trades = vec![
        (1, "65000.00", "0.001", false),
        (2, "65000.10", "1.500", true),
        (3, "0.10", "0.001", false), // very small price but nonzero
        (4, "99999.99", "10.000", true),
    ];
    for (id, price, qty, maker) in real_trades {
        let raw = make_raw_trade(id, price, qty, maker);
        match normalize_trade(&raw) {
            NormalizeResult::Ok(event) => {
                assert!(
                    event.price_ticks > 0,
                    "trade_id={}: price_ticks must be > 0",
                    id
                );
                assert!(
                    event.quantity_ticks > 0,
                    "trade_id={}: quantity_ticks must be > 0",
                    id
                );
            }
            NormalizeResult::MarkerEvent(_) => {
                panic!("trade_id={}: real trade should not be marker", id);
            }
            NormalizeResult::ParseError(e) => {
                panic!("trade_id={}: parse error: {}", id, e);
            }
        }
    }
}

#[test]
fn test_57_processor_rejects_marker_without_processing() {
    let mut proc = TradeProcessor::new();
    // Simulate what main.rs does: normalize then process
    let marker = FuturesTrade {
        event_type: "trade".to_string(),
        event_time: 1787150961497,
        trade_time: 1787150961497,
        symbol: "BTCUSDT".to_string(),
        trade_id: 7979198979,
        price: "0".to_string(),
        quantity: "0".to_string(),
        order_type: "NA".to_string(),
        is_buyer_maker: true,
        trade_type: 1,
    };
    let result = normalize_trade(&marker);
    match result {
        NormalizeResult::MarkerEvent(_) => {
            proc.record_marker_rejected();
        }
        _ => panic!("Expected MarkerEvent"),
    }
    assert_eq!(proc.marker_events_rejected(), 1);
    assert_eq!(proc.trade_events_received(), 0);
    assert_eq!(proc.trade_events_processed(), 0);
}
