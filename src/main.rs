use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::Parser;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use futures_orderbook::binance::{
    RestClient, TradeWebSocketClient, TradeWsMessage, WebSocketClient, WsMessage,
};
use futures_orderbook::config::{Command, Config, ReplayArgs, VerifyArgs};
use futures_orderbook::diagnostics::{format_diagnostics, format_storage_section};
use futures_orderbook::events::MarketEvent;
use futures_orderbook::orderbook::{OrderBook, SyncState, Synchronizer};
use futures_orderbook::recording::{
    detect_git_commit, start_recorder, NewTrade, RecordingConfig, SessionRecord, SessionStatus,
};
use futures_orderbook::replay::{load_session, run_replay, ReplayConfig, ReplayReport};
use futures_orderbook::storage::{ClickHouseStorage, Storage};
use futures_orderbook::trades::normalizer::{normalize_trade, NormalizeResult};
use futures_orderbook::trades::processor::TradeProcessor;
use futures_orderbook::verify::verify_session;

/// Snapshot fetch timeout.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let config = Config::parse();

    match &config.command {
        Some(Command::Replay(args)) => cmd_replay(&config, args).await,
        Some(Command::Verify(args)) => cmd_verify(&config, args).await,
        None => cmd_record(config).await,
    }
}

/// Connect to ClickHouse and ensure the schema exists.
async fn connect_storage(config: &Config) -> anyhow::Result<Arc<dyn Storage>> {
    let storage: Arc<dyn Storage> = Arc::new(
        ClickHouseStorage::connect(
            &config.clickhouse_url,
            &config.clickhouse_database,
            &config.clickhouse_user,
            &config.clickhouse_password,
        )
        .await?,
    );
    storage.ping().await?;
    storage.init_schema().await?;
    Ok(storage)
}

/// Resolve a session id from an explicit value, or fall back to the most
/// recent session for a symbol.
async fn resolve_session(
    storage: &dyn Storage,
    explicit: Option<&str>,
    symbol: Option<&str>,
) -> anyhow::Result<Uuid> {
    if let Some(id) = explicit {
        return Ok(Uuid::parse_str(id)?);
    }
    let sessions = storage.list_sessions(20).await?;
    let wanted = symbol.unwrap_or("BTCUSDT");
    let session = sessions
        .iter()
        .find(|s| s.symbol == wanted)
        .or_else(|| sessions.first())
        .ok_or_else(|| anyhow::anyhow!("no sessions found"))?;
    info!(
        "Using most recent session for symbol '{}': {}",
        wanted, session.session_id
    );
    Ok(session.session_id)
}

async fn cmd_replay(config: &Config, args: &ReplayArgs) -> anyhow::Result<()> {
    let storage = connect_storage(config).await?;
    let session_id = resolve_session(
        storage.as_ref(),
        args.session.as_deref(),
        args.symbol.as_deref(),
    )
    .await?;
    let session = storage
        .get_session(session_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("session {} not found", session_id))?;

    info!(
        "Replaying session {} ({} events expected) — speed {}x",
        session_id, session.depth_stream, args.speed
    );

    let data = load_session(storage.as_ref(), session_id).await?;
    let config = ReplayConfig {
        speed: args.speed,
        seq_start: None,
        seq_end: None,
    };
    let outcome = run_replay(data, config).await?;
    println!("{}", ReplayReport { session, outcome });
    Ok(())
}

async fn cmd_verify(config: &Config, args: &VerifyArgs) -> anyhow::Result<()> {
    let storage = connect_storage(config).await?;
    let session_id = resolve_session(storage.as_ref(), args.session.as_deref(), None).await?;
    let report = verify_session(storage.as_ref(), session_id).await?;
    println!("{}", report);
    Ok(())
}

// ============================================================================
// Record mode
// ============================================================================

async fn cmd_record(config: Config) -> anyhow::Result<()> {
    let start_time = Instant::now();

    info!("========================================");
    info!("Binance USDⓈ-M Futures Order-Book Engine");
    info!("Symbol: {} PERPETUAL", config.symbol);
    info!("Market: Binance USDⓈ-M Futures");
    info!("REST base: {}", config.rest_base);
    info!("WebSocket base: {}", config.ws_base);
    info!("Depth stream: {}", config.depth_stream_url());
    info!("Trade stream: {}", config.trade_stream_url());
    info!("Record mode: {}", if config.record { "ON" } else { "OFF" });
    if config.record {
        info!(
            "Recording to ClickHouse: {} db={}",
            config.clickhouse_url, config.clickhouse_database
        );
    }
    info!("========================================");

    // Create shared state
    let book = Arc::new(RwLock::new(OrderBook::new()));
    let sync = Arc::new(RwLock::new(Synchronizer::new()));
    let trade_proc = Arc::new(RwLock::new(TradeProcessor::new()));
    let trade_connected = Arc::new(RwLock::new(false));

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

    // Recording setup: storage, session row, recorder + storage worker.
    let mut recorder: Option<Arc<futures_orderbook::recording::Recorder>> = None;
    let mut recorder_handle = None;
    let mut recording_storage: Option<Arc<dyn Storage>> = None;

    if config.record {
        let storage = connect_storage(&config).await?;
        let session = SessionRecord::new(
            &config.symbol,
            "Binance",
            "USDⓈ-M",
            "PERPETUAL",
            &config.depth_stream_url(),
            &config.trade_stream_url(),
            env!("CARGO_PKG_VERSION"),
            &detect_git_commit(),
        );
        storage.insert_session(&session).await?;
        storage
            .update_session_status(session.session_id, SessionStatus::Recording.as_str(), None)
            .await?;

        let rconfig = RecordingConfig::new(
            config.batch_size,
            config.flush_interval_ms,
            config.queue_capacity,
        );
        let (rec, handle) = start_recorder(Arc::clone(&storage), session, rconfig);
        recorder = Some(rec);
        recorder_handle = Some(handle);
        recording_storage = Some(storage);
        info!(
            "Recording session started: {}",
            recorder.as_ref().unwrap().session_id()
        );
    }

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
                        let recorder = recorder.clone();
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

                                        if let Some(rec) = &recorder {
                                            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
                                            rec.record_snapshot(
                                                &symbol,
                                                snapshot.last_update_id,
                                                now_ms,
                                                &snapshot.bids,
                                                &snapshot.asks,
                                            );
                                        }

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
                    Some(WsMessage::DepthUpdate { update, raw, local_receive_time_ns }) => {
                        if let Some(rec) = &recorder {
                            rec.record_raw(
                                &config.symbol,
                                "depth",
                                raw,
                                update.event_time,
                                Some(update.transaction_time),
                                local_receive_time_ns,
                            );
                            rec.record_depth_event(&update, local_receive_time_ns);
                        }

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
                                        let recorder = recorder.clone();
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

                                                        if let Some(rec) = &recorder {
                                                            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
                                                            rec.record_snapshot(
                                                                &symbol,
                                                                snapshot.last_update_id,
                                                                now_ms,
                                                                &snapshot.bids,
                                                                &snapshot.asks,
                                                            );
                                                        }

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
                    Some(TradeWsMessage::Trade { trade, raw, local_receive_time_ns }) => {
                        if let Some(rec) = &recorder {
                            rec.record_raw(
                                &config.symbol,
                                "trade",
                                raw,
                                trade.event_time,
                                None,
                                local_receive_time_ns,
                            );
                        }

                        match normalize_trade(&trade) {
                            NormalizeResult::Ok(event) => {
                                let mut tp = trade_proc.write().await;
                                use futures_orderbook::trades::processor::TradeProcessResult;
                                match tp.process(event.clone()) {
                                    TradeProcessResult::Processed => {
                                        if let Some(rec) = &recorder {
                                            rec.record_trade(NewTrade {
                                                symbol: event.symbol.clone(),
                                                trade_id: event.trade_id,
                                                first_trade_id: None,
                                                last_trade_id: None,
                                                price: event.price_ticks,
                                                quantity: event.quantity_ticks,
                                                aggressor_side: event.aggressor.label().to_string(),
                                                exchange_event_time_ms: event.event_time,
                                                trade_time_ms: event.trade_time,
                                                local_receive_time_ns,
                                                order_type: event.order_type.clone(),
                                            });
                                        }
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
                                if let Some(rec) = &recorder {
                                    rec.record_marker_rejected();
                                }
                                let mut tp = trade_proc.write().await;
                                tp.record_marker_rejected();
                                drop(tp);
                            }
                            NormalizeResult::ParseError(e) => {
                                warn!("Failed to normalize trade: {}", e);
                                if let Some(rec) = &recorder {
                                    rec.record_invalid_rejected();
                                }
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
                let mut output = format_diagnostics(
                    &config.symbol,
                    s.state(),
                    &b,
                    &s,
                    &tp,
                    tc,
                    start_time,
                );
                if let Some(rec) = &recorder {
                    let metrics = rec.metrics.lock().unwrap().clone();
                    let health = rec.health.lock().unwrap().clone();
                    output.push('\n');
                    output.push_str(&format_storage_section(&metrics, &health, rec.session_id()));
                }
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

    // Stop recording: final flush, then persist the final session status.
    if let Some(rec) = &recorder {
        info!(
            "Stopping recording (session {}), final flush...",
            rec.session_id()
        );
        if let Some(storage) = &recording_storage {
            storage
                .update_session_status(
                    rec.session_id(),
                    SessionStatus::Stopping.as_str(),
                    Some(Utc::now()),
                )
                .await?;
        }
        rec.request_shutdown();
        if let Some(handle) = recorder_handle.take() {
            handle.join().await?;
        }
        let degraded = {
            let health = rec.health.lock().unwrap();
            health.degraded || health.insert_failures > 0
        };
        let final_status = if degraded {
            SessionStatus::Degraded
        } else {
            SessionStatus::Completed
        };
        if let Some(storage) = &recording_storage {
            storage
                .update_session_status(rec.session_id(), final_status.as_str(), Some(Utc::now()))
                .await?;
        }
        info!(
            "Recording finished: session {} -> {}",
            rec.session_id(),
            final_status
        );
    }

    info!("Engine shutdown complete");
    Ok(())
}
