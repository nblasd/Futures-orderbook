use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use futures_orderbook::binance::TradeWsMessage;
use futures_orderbook::binance::{RestClient, TradeWebSocketClient, WebSocketClient, WsMessage};
use futures_orderbook::config::Config;
use futures_orderbook::diagnostics::format_diagnostics;
use futures_orderbook::events::MarketEvent;
use futures_orderbook::orderbook::{OrderBook, SyncState, Synchronizer};
use futures_orderbook::trades::normalizer::{normalize_trade, NormalizeResult};
use futures_orderbook::trades::processor::TradeProcessor;

/// Snapshot fetch timeout.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let config = Config::parse();
    let start_time = Instant::now();

    info!("========================================");
    info!("Binance USDⓈ-M Futures Order-Book Engine");
    info!("Symbol: {} PERPETUAL", config.symbol);
    info!("Market: Binance USDⓈ-M Futures");
    info!("REST base: {}", config.rest_base);
    info!("WebSocket base: {}", config.ws_base);
    info!("Depth stream: {}", config.depth_stream_url());
    info!("Trade stream: {}", config.trade_stream_url());
    info!("========================================");

    // Create shared state
    let book = Arc::new(RwLock::new(OrderBook::new()));
    let sync = Arc::new(RwLock::new(Synchronizer::new()));
    let trade_proc = Arc::new(RwLock::new(TradeProcessor::new()));
    let trade_connected = Arc::new(RwLock::new(false));

    // Set initial state to Connecting
    {
        let mut s = sync.write().await;
        s.on_connecting();
    }

    // Channels
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<WsMessage>();
    let (trade_ws_tx, mut trade_ws_rx) = mpsc::unbounded_channel::<TradeWsMessage>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<MarketEvent>();

    // Start depth WebSocket client
    let ws_client = WebSocketClient::new(config.clone());
    let ws_handle = tokio::spawn(async move {
        if let Err(e) = ws_client.run(ws_tx).await {
            error!("Depth WebSocket client fatal error: {}", e);
        }
    });

    // Start trade WebSocket client
    let trade_ws_client = TradeWebSocketClient::new(config.clone());
    let trade_ws_handle = tokio::spawn(async move {
        if let Err(e) = trade_ws_client.run(trade_ws_tx).await {
            error!("Trade WebSocket client fatal error: {}", e);
        }
    });

    // Main engine loop
    let mut diagnostic_interval =
        tokio::time::interval(Duration::from_secs(config.diagnostic_interval));
    diagnostic_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let duration_limit = if config.duration > 0 {
        Some(tokio::time::Instant::now() + Duration::from_secs(config.duration))
    } else {
        None
    };

    let mut awaiting_snapshot: bool = false;

    loop {
        tokio::select! {
            // Check duration limit
            _ = async {
                if let Some(limit) = duration_limit {
                    tokio::time::sleep_until(limit).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                info!("Duration limit reached, shutting down");
                break;
            }

            // Handle depth WebSocket messages
            msg = ws_rx.recv() => {
                match msg {
                    Some(WsMessage::Connected) => {
                        info!(">>> DEPTH CONNECTED — entering Buffering state");
                        let mut s = sync.write().await;
                        s.on_connected();
                        info!(">>> SNAPSHOT REQUESTED — fetching REST order book");
                        awaiting_snapshot = true;
                        let rest = RestClient::new(config.clone());
                        let book_clone = Arc::clone(&book);
                        let sync_clone = Arc::clone(&sync);
                        let evt_tx = event_tx.clone();
                        let symbol = config.symbol.clone();
                        tokio::spawn(async move {
                            const MAX_SNAPSHOT_RETRIES: u32 = 5;
                            for attempt in 1..=MAX_SNAPSHOT_RETRIES {
                                info!("Snapshot fetch attempt {}/{}", attempt, MAX_SNAPSHOT_RETRIES);
                                match tokio::time::timeout(SNAPSHOT_TIMEOUT, rest.fetch_depth_snapshot()).await {
                                    Err(_) => {
                                        error!("REST snapshot timed out after {:?}", SNAPSHOT_TIMEOUT);
                                        let mut s = sync_clone.write().await;
                                        s.trigger_resync();
                                        drop(s);
                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                        continue;
                                    }
                                    Ok(Err(e)) => {
                                        error!("REST snapshot failed: {}", e);
                                        let mut s = sync_clone.write().await;
                                        s.trigger_resync();
                                        drop(s);
                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                        continue;
                                    }
                                    Ok(Ok(snapshot)) => {
                                        info!(">>> SNAPSHOT RECEIVED — lastUpdateId={}", snapshot.last_update_id);

                                        let mut s = sync_clone.write().await;
                                        s.on_snapshot_loading();
                                        drop(s);

                                        let mut b = book_clone.write().await;
                                        if let Err(e) = b.apply_snapshot(
                                            &snapshot.bids,
                                            &snapshot.asks,
                                            snapshot.last_update_id,
                                        ) {
                                            error!("Failed to apply snapshot to book: {}", e);
                                            return;
                                        }
                                        drop(b);

                                        let mut s = sync_clone.write().await;
                                        info!(">>> SYNCHRONIZING — reconciling buffered events");
                                        match s.reconcile(snapshot.last_update_id) {
                                            Ok(events) => {
                                                info!(
                                                    "Reconciled {} buffered events",
                                                    events.len()
                                                );
                                                let mut b = book_clone.write().await;
                                                for event in &events {
                                                    if let Err(e) = b.apply_depth_update(
                                                        &event.bids,
                                                        &event.asks,
                                                        event.final_update_id,
                                                    ) {
                                                        error!("Failed to apply reconciled event: {}", e);
                                                        break;
                                                    }
                                                }
                                                drop(b);

                                                info!(">>> READY — order book synchronized");
                                                let _ = evt_tx.send(MarketEvent::OrderBookSynchronized {
                                                    symbol,
                                                    last_update_id: snapshot.last_update_id,
                                                    bid_levels: 0,
                                                    ask_levels: 0,
                                                });
                                                return;
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Reconcile failed (attempt {}/{}): {}",
                                                    attempt, MAX_SNAPSHOT_RETRIES, e
                                                );
                                                s.trigger_resync();
                                                drop(s);
                                                // Wait for depth events to accumulate before retrying
                                                tokio::time::sleep(Duration::from_millis(1000)).await;
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                            error!("Failed to synchronize order book after {} snapshot attempts", MAX_SNAPSHOT_RETRIES);
                        });
                    }
                    Some(WsMessage::Disconnected) => {
                        warn!(">>> DEPTH DISCONNECTED — WebSocket closed");
                        let mut s = sync.write().await;
                        s.on_reconnecting();
                        awaiting_snapshot = false;
                        let mut b = book.write().await;
                        b.set_initialized(false);
                        let _ = event_tx.send(MarketEvent::ConnectionStatusChanged {
                            symbol: config.symbol.clone(),
                            connected: false,
                            reconnect_count: s.reconnect_count(),
                        });
                    }
                    Some(WsMessage::DepthUpdate(update)) => {
                        let mut s = sync.write().await;

                        match s.state() {
                            SyncState::Buffering => {
                                s.buffer_event(update);
                                debug!(
                                    "Buffered event, buffer size={}, awaiting_snapshot={}",
                                    s.buffer_size(),
                                    awaiting_snapshot
                                );
                            }
                            SyncState::Ready => {
                                match s.process_live_event(&update) {
                                    futures_orderbook::orderbook::ProcessResult::Apply => {
                                        drop(s);
                                        let mut b = book.write().await;
                                        if let Err(e) = b.apply_depth_update(
                                            &update.bids,
                                            &update.asks,
                                            update.final_update_id,
                                        ) {
                                            error!("Failed to apply depth update: {}", e);
                                        } else {
                                            let best_bid = b.best_bid();
                                            let best_ask = b.best_ask();
                                            let mid = b.mid_price();
                                            drop(b);

                                            let _ = event_tx.send(MarketEvent::OrderBookUpdated {
                                                symbol: config.symbol.clone(),
                                                update_id: update.final_update_id,
                                                bid_changes: update.bids.iter().map(|(p, q)| {
                                                    let price_ticks = futures_orderbook::orderbook::level::price_str_to_ticks(p).unwrap_or(0);
                                                    let qty_ticks = futures_orderbook::orderbook::level::quantity_str_to_ticks(q).unwrap_or(0);
                                                    (price_ticks, qty_ticks, None)
                                                }).collect(),
                                                ask_changes: update.asks.iter().map(|(p, q)| {
                                                    let price_ticks = futures_orderbook::orderbook::level::price_str_to_ticks(p).unwrap_or(0);
                                                    let qty_ticks = futures_orderbook::orderbook::level::quantity_str_to_ticks(q).unwrap_or(0);
                                                    (price_ticks, qty_ticks, None)
                                                }).collect(),
                                                best_bid,
                                                best_ask,
                                                mid_price: mid,
                                            });
                                        }
                                    }
                                    futures_orderbook::orderbook::ProcessResult::PuMismatch => {
                                        warn!("SEQUENCE ERROR — pu continuity failure, triggering resync");
                                        let _ = event_tx.send(MarketEvent::OrderBookResyncStarted {
                                            symbol: config.symbol.clone(),
                                            reason: "pu continuity failure".to_string(),
                                        });
                                        s.trigger_resync();
                                        awaiting_snapshot = true;

                                        let rest = RestClient::new(config.clone());
                                        let book_clone = Arc::clone(&book);
                                        let sync_clone = Arc::clone(&sync);
                                        let evt_tx = event_tx.clone();
                                        let symbol = config.symbol.clone();
                                        tokio::spawn(async move {
                                            const MAX_RESYNC_RETRIES: u32 = 5;
                                            for attempt in 1..=MAX_RESYNC_RETRIES {
                                                match tokio::time::timeout(SNAPSHOT_TIMEOUT, rest.fetch_depth_snapshot()).await {
                                                    Err(_) => {
                                                        error!("RESYNC snapshot timed out (attempt {}/{})", attempt, MAX_RESYNC_RETRIES);
                                                        let mut s = sync_clone.write().await;
                                                        s.trigger_resync();
                                                        drop(s);
                                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                                        continue;
                                                    }
                                                    Ok(Err(e)) => {
                                                        error!("RESYNC snapshot failed: {} (attempt {}/{})", e, attempt, MAX_RESYNC_RETRIES);
                                                        let mut s = sync_clone.write().await;
                                                        s.trigger_resync();
                                                        drop(s);
                                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                                        continue;
                                                    }
                                                    Ok(Ok(snapshot)) => {
                                                        info!("RESYNC snapshot received: lastUpdateId={}", snapshot.last_update_id);
                                                        let mut s = sync_clone.write().await;
                                                        s.on_snapshot_loading();
                                                        drop(s);
                                                        let mut b = book_clone.write().await;
                                                        if let Err(e) = b.apply_snapshot(
                                                            &snapshot.bids,
                                                            &snapshot.asks,
                                                            snapshot.last_update_id,
                                                        ) {
                                                            error!("Failed to apply resync snapshot: {}", e);
                                                            return;
                                                        }
                                                        drop(b);
                                                        let mut s = sync_clone.write().await;
                                                        match s.reconcile(snapshot.last_update_id) {
                                                            Ok(events) => {
                                                                let mut b = book_clone.write().await;
                                                                for event in &events {
                                                                    let _ = b.apply_depth_update(
                                                                        &event.bids,
                                                                        &event.asks,
                                                                        event.final_update_id,
                                                                    );
                                                                }
                                                                drop(b);
                                                                info!("RESYNC completed — READY");
                                                                let _ = evt_tx.send(MarketEvent::OrderBookResyncCompleted {
                                                                    symbol,
                                                                    last_update_id: snapshot.last_update_id,
                                                                });
                                                                return;
                                                            }
                                                            Err(e) => {
                                                                warn!("RESYNC reconcile failed: {} (attempt {}/{})", e, attempt, MAX_RESYNC_RETRIES);
                                                                s.trigger_resync();
                                                                drop(s);
                                                                tokio::time::sleep(Duration::from_millis(1000)).await;
                                                                continue;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            error!("Failed to resync order book after {} attempts", MAX_RESYNC_RETRIES);
                                        });
                                    }
                                    futures_orderbook::orderbook::ProcessResult::Stale => {
                                        debug!("Stale event ignored: u={}", update.final_update_id);
                                    }
                                    futures_orderbook::orderbook::ProcessResult::Buffered => {}
                                    futures_orderbook::orderbook::ProcessResult::Ignored => {}
                                }
                            }
                            SyncState::Reconnecting => {
                                s.buffer_event(update);
                            }
                            SyncState::SnapshotLoading | SyncState::Synchronizing => {
                                s.buffer_event(update);
                            }
                            _ => {
                                debug!("Ignoring depth event in state {:?}", s.state());
                            }
                        }
                    }
                    Some(WsMessage::Error(e)) => {
                        warn!("DEPTH WS ERROR: {}", e);
                    }
                    None => {
                        info!("Depth WebSocket channel closed");
                        break;
                    }
                }
            }

            // Handle trade WebSocket messages
            trade_msg = trade_ws_rx.recv() => {
                match trade_msg {
                    Some(TradeWsMessage::Connected) => {
                        info!(">>> TRADE CONNECTED");
                        {
                            let mut tc = trade_connected.write().await;
                            *tc = true;
                        }
                    }
                    Some(TradeWsMessage::Disconnected) => {
                        warn!(">>> TRADE DISCONNECTED");
                        let mut tc = trade_connected.write().await;
                        *tc = false;
                        let mut tp = trade_proc.write().await;
                        tp.on_trade_reconnect();
                    }
                    Some(TradeWsMessage::Trade(raw)) => {
                        match normalize_trade(&raw) {
                            NormalizeResult::Ok(event) => {
                                let mut tp = trade_proc.write().await;
                                use futures_orderbook::trades::processor::TradeProcessResult;
                                match tp.process(event.clone()) {
                                    TradeProcessResult::Processed => {
                                        let _ = event_tx.send(MarketEvent::Trade(event));
                                    }
                                    TradeProcessResult::Duplicate => {
                                        debug!("Duplicate trade ID: {}", event.trade_id);
                                    }
                                    TradeProcessResult::Stale => {
                                        debug!("Stale trade ID: {}", event.trade_id);
                                    }
                                    TradeProcessResult::Ignored => {}
                                }
                                drop(tp);
                            }
                            NormalizeResult::MarkerEvent(marker) => {
                                debug!(
                                    "Rejected marker event: trade_id={}, price={}, qty={}, order_type={}",
                                    marker.trade_id, marker.price, marker.quantity, marker.order_type
                                );
                                let mut tp = trade_proc.write().await;
                                tp.record_marker_rejected();
                                drop(tp);
                            }
                            NormalizeResult::ParseError(e) => {
                                warn!("Failed to normalize trade: {}", e);
                                let mut tp = trade_proc.write().await;
                                tp.record_malformed();
                                drop(tp);
                            }
                        }
                    }
                    Some(TradeWsMessage::Error(e)) => {
                        warn!("TRADE WS ERROR: {}", e);
                    }
                    None => {
                        info!("Trade WebSocket channel closed");
                    }
                }
            }

            // Print diagnostics
            _ = diagnostic_interval.tick() => {
                let s = sync.read().await;
                let b = book.read().await;
                let tp = trade_proc.read().await;
                let tc = *trade_connected.read().await;
                let output = format_diagnostics(
                    &config.symbol,
                    s.state(),
                    &b,
                    &s,
                    &tp,
                    tc,
                    start_time,
                );
                print!("\x1B[2J\x1B[H{}", output);
                use std::io::Write;
                std::io::stdout().flush().unwrap_or(());
            }

            // Drain internal events
            _ = event_rx.recv() => {}
        }
    }

    // Shutdown
    {
        let mut s = sync.write().await;
        s.shutdown();
    }

    ws_handle.abort();
    trade_ws_handle.abort();
    info!("Engine shutdown complete");

    Ok(())
}
