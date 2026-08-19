use futures_orderbook::binance::types::DepthUpdate;
use futures_orderbook::orderbook::book::OrderBook;
use futures_orderbook::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks, TICK_SCALE};
use futures_orderbook::orderbook::synchronizer::{ProcessResult, SyncState, Synchronizer};

// ============================================================================
// Helper functions
// ============================================================================

#[allow(non_snake_case, dead_code)]
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
