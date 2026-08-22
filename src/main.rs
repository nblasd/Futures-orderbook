use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::Parser;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use futures_orderbook::analytics::engine::AnalyticsEngine;
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
use futures_orderbook::storage::{
    start_analytics_sink, AnalyticsBatch, AnalyticsEventRow, AnalyticsSink, AnalyticsSinkHandle,
    AnalyticsSnapshotRow, ClickHouseStorage, DeltaByPriceRow, LiquidityEventRow, Storage,
};
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

/// Convert engine output into a persistence batch for a session.
///
/// `TradeDelta` and `Cluster` events are computed but not persisted by default
/// (they are high-frequency and fully derivable from the raw trade stream).
fn analytics_batch_from_output(
    output: &futures_orderbook::analytics::engine::EngineOutput,
    session_id: uuid::Uuid,
) -> AnalyticsBatch {
    let mut batch = AnalyticsBatch::default();
    for s in &output.snapshots {
        batch
            .snapshots
            .push(AnalyticsSnapshotRow::from_snapshot(s, session_id));
    }
    for e in &output.events {
        if matches!(
            e.kind,
            futures_orderbook::analytics::events::AnalyticsEventKind::TradeDelta
                | futures_orderbook::analytics::events::AnalyticsEventKind::Cluster
        ) {
            continue;
        }
        batch
            .events
            .push(AnalyticsEventRow::from_event(e, session_id));
        // Liquidity-level changes are persisted separately as well.
        if let Some(liq) = liquidity_row_from_event(e, session_id) {
            batch.liquidity_events.push(liq);
        }
    }
    batch
}

/// Build a `LiquidityEventRow` from an analytics event, when applicable.
fn liquidity_row_from_event(
    e: &futures_orderbook::analytics::events::AnalyticsEvent,
    session_id: uuid::Uuid,
) -> Option<LiquidityEventRow> {
    use futures_orderbook::analytics::events::AnalyticsEventKind;
    let kind = match e.kind {
        AnalyticsEventKind::LiquidityAdded => "added",
        AnalyticsEventKind::LiquidityRemoved => "removed",
        AnalyticsEventKind::LiquidityIncreased => "increased",
        AnalyticsEventKind::LiquidityDecreased => "decreased",
        AnalyticsEventKind::LiquidityReplenishment => "replenishment",
        _ => return None,
    };
    let side = e.side.clone().unwrap_or_default();
    let price = e.price.unwrap_or(0);
    Some(LiquidityEventRow {
        session_id,
        symbol: e.symbol.clone(),
        ts_ms: futures_orderbook::storage::ms_to_datetime(e.ts_ms),
        kind: kind.to_string(),
        side,
        price,
        quantity_delta: e.quantity,
        is_replenishment: e.kind == AnalyticsEventKind::LiquidityReplenishment,
    })
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
        analytics: if args.analytics {
            Some(config.analytics_config())
        } else {
            None
        },
    };
    let outcome = run_replay(data, config).await?;
    println!(
        "{}",
        ReplayReport {
            session,
            outcome: outcome.clone()
        }
    );
    if let Some(digest) = &outcome.analytics_digest {
        println!("Analytics digest: {}", digest.summarize());
    }
    if let Some(hm_digest) = &outcome.heatmap_digest {
        println!("Heatmap digest: {}", hm_digest.summarize());
    }
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

    // Auto-enable analytics when WebSocket server is active (needs heatmap).
    let analytics_enabled = config.analytics || config.ws_server;
    if config.ws_server && !config.analytics {
        info!("WebSocket server requires analytics — enabling");
    }

    // Phase 4 analytics setup. The engine is synchronous and sits behind a
    // std Mutex; it consumes the same MarketEvent stream the book does.
    let analytics_engine: Option<std::sync::Mutex<AnalyticsEngine>> = if analytics_enabled {
        Some(std::sync::Mutex::new(AnalyticsEngine::new(
            config.analytics_config(),
        )))
    } else {
        None
    };
    let mut analytics_sink_handle: Option<AnalyticsSinkHandle> = None;
    let analytics_sink: Option<AnalyticsSink> = match (analytics_enabled, config.record) {
        (true, true) => {
            let storage = recording_storage
                .clone()
                .expect("storage exists when recording");
            let (sink, handle) = start_analytics_sink(storage, config.queue_capacity);
            analytics_sink_handle = Some(handle);
            Some(sink)
        }
        _ => None,
    };
    let analytics_session_id = if config.record {
        recorder.as_ref().unwrap().session_id()
    } else {
        uuid::Uuid::nil()
    };

    // --- Phase 6 WebSocket server (optional) ---
    let ws_frame_tx: Option<
        tokio::sync::broadcast::Sender<futures_orderbook::server::HeatmapFrameSerde>,
    > = if config.ws_server {
        let (tx, _) = tokio::sync::broadcast::channel(64);
        Some(tx)
    } else {
        None
    };
    let ws_delta_tx: Option<
        tokio::sync::broadcast::Sender<futures_orderbook::server::HeatmapDeltaSerde>,
    > = if config.ws_server {
        let (tx, _) = tokio::sync::broadcast::channel(64);
        Some(tx)
    } else {
        None
    };
    let ws_status_tx: Option<
        tokio::sync::broadcast::Sender<futures_orderbook::server::StatusInfo>,
    > = if config.ws_server {
        let (tx, _) = tokio::sync::broadcast::channel(64);
        Some(tx)
    } else {
        None
    };

    if config.ws_server {
        let addr: SocketAddr = ([0, 0, 0, 0], config.ws_port).into();
        let frontend_dir: PathBuf = config.frontend_dir.as_str().into();
        let replay_controller = std::sync::Arc::new(tokio::sync::RwLock::new(
            futures_orderbook::server_replay::ReplayController::new(
                ws_frame_tx.clone().unwrap(),
                ws_delta_tx.clone().unwrap(),
                ws_status_tx.clone().unwrap(),
            ),
        ));
        let storage_for_server = recording_storage.clone();
        futures_orderbook::server::start_server(
            addr,
            frontend_dir,
            ws_frame_tx.clone().unwrap(),
            ws_delta_tx.clone().unwrap(),
            ws_status_tx.clone().unwrap(),
            Some(replay_controller),
            storage_for_server,
        );
        info!("Phase 6 WebSocket server started on {addr}");

        // If --replay-session was provided, auto-start replay
        if let Some(ref session_id_str) = config.replay_session {
            if let Ok(sid) = uuid::Uuid::parse_str(session_id_str) {
                let storage = recording_storage.clone();
                let frame_tx = ws_frame_tx.clone().unwrap();
                let delta_tx = ws_delta_tx.clone().unwrap();
                let status_tx = ws_status_tx.clone().unwrap();
                let speed = config.replay_speed;
                tokio::spawn(async move {
                    let ctl = futures_orderbook::server_replay::ReplayController::new(
                        frame_tx, delta_tx, status_tx,
                    );
                    if let Some(store) = storage {
                        ctl.load_and_run(store, sid, speed).await;
                    } else {
                        warn!("No storage available for replay");
                    }
                });
            }
        }
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
    let mut ws_frame_interval = tokio::time::interval(Duration::from_millis(250));
    let mut ws_prev_frame: Option<futures_orderbook::analytics::heatmap::HeatmapFrame> = None;
    ws_frame_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                                                // Seed the analytics shadow book with the full post-reconcile
                                                // state, then announce readiness.
                                                let full = book_clone.read().await.snapshot();
                                                let _ = evt_tx.send(MarketEvent::OrderBookSnapshot {
                                                    symbol: symbol.clone(),
                                                    update_id: full.last_update_id,
                                                    bids: full.bids.iter().map(|l| (l.price, l.quantity)).collect(),
                                                    asks: full.asks.iter().map(|l| (l.price, l.quantity)).collect(),
                                                });
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
                                                event_time_ms: update.event_time,
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
                                                                let full = book_clone.read().await.snapshot();
                                                                let _ = evt_tx.send(MarketEvent::OrderBookSnapshot {
                                                                    symbol: symbol.clone(),
                                                                    update_id: full.last_update_id,
                                                                    bids: full.bids.iter().map(|l| (l.price, l.quantity)).collect(),
                                                                    asks: full.asks.iter().map(|l| (l.price, l.quantity)).collect(),
                                                                });
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
                if let Some(engine) = &analytics_engine {
                    let eng = engine.lock().unwrap();
                    output.push('\n');
                    output.push_str(&futures_orderbook::diagnostics::format_analytics_section(&eng));
                }
                print!("\x1B[2J\x1B[H{}", output);
                use std::io::Write;
                std::io::stdout().flush().unwrap_or(());
            }

            // Process internal events (analytics engine)
            ev = event_rx.recv() => {
                match ev {
                    Some(ev) => {
                        if let Some(engine) = &analytics_engine {
                            let output = {
                                let mut eng = engine.lock().unwrap();
                                eng.process_event(&ev)
                            };
                            if let Some(sink) = &analytics_sink {
                                let batch = analytics_batch_from_output(&output, analytics_session_id);
                                sink.submit(batch);
                            }
                        }
                    }
                    None => {
                        info!("Event channel closed");
                        break;
                    }
                }


            }

            // Periodic heatmap frame broadcast for WebSocket clients
            _ = ws_frame_interval.tick(), if ws_frame_tx.is_some() => {
                if let (Some(engine), Some(frame_tx), Some(status_tx)) = (
                    &analytics_engine, &ws_frame_tx, &ws_status_tx
                ) {
                    let eng = engine.lock().unwrap();
                    let heatmap = eng.heatmap();
                    let now_ms = eng.last_event_ts().unwrap_or(0);
                    // Use a wide visible range around best bid/ask
                    let (lo, hi) = {
                        let book = &eng.book;
                        let mid = book.mid_price_f64().unwrap_or(77300.0);
                        let mid_ticks = (mid * 100_000_000.0) as u64;
                        let range = 50 * 10_000_000; // 50 tick range
                        (mid_ticks.saturating_sub(range), mid_ticks.saturating_add(range))
                    };
                    let frame = futures_orderbook::analytics::heatmap::HeatmapFrame::from_heatmap(
                        heatmap, now_ms, lo, hi
                    );
                    let serde_frame: futures_orderbook::server::HeatmapFrameSerde = (&frame).into();

                    // Compute and broadcast delta if we have a previous frame
                    if let (Some(delta_tx), Some(prev)) = (&ws_delta_tx, &ws_prev_frame) {
                        let delta = futures_orderbook::analytics::heatmap::HeatmapDelta::compute(prev, &frame);
                        let serde_delta: futures_orderbook::server::HeatmapDeltaSerde = (&delta).into();
                        let _ = delta_tx.send(serde_delta);
                    }
                    ws_prev_frame = Some(frame);
                    drop(eng);
                    let _ = frame_tx.send(serde_frame);

                    // Also broadcast status
                    let eng = engine.lock().unwrap();
                    let book = &eng.book;
                    let status = futures_orderbook::server::StatusInfo {
                        connection: if analytics_enabled { "LIVE".into() } else { "DISCONNECTED".into() },
                        book_status: if book.is_ready() { "READY".into() } else { "BUFFERING".into() },
                        symbol: config.symbol.clone(),
                        exchange: "Binance USD\u{2119}-M Futures".into(),
                        best_bid: book.best_bid().map(|p| p as f64 / 100_000_000.0).unwrap_or(0.0),
                        best_ask: book.best_ask().map(|p| p as f64 / 100_000_000.0).unwrap_or(0.0),
                        mid: book.mid_price_f64().unwrap_or(0.0),
                        spread: book.spread_ticks().map(|s| s as f64 / 100_000_000.0).unwrap_or(0.0),
                        events_per_sec: 0,
                        trades_per_sec: 0,
                        heatmap_cells: eng.heatmap().cell_count(),
                        sequence_errors: 0,
                        queue_depth: 0,
                    };
                    drop(eng);
                    let _ = status_tx.send(status);
                }
            }
        }
    }

    // Shutdown
    {
        let mut s = sync.write().await;
        s.shutdown();
    }

    ws_handle.abort();
    trade_ws_handle.abort();

    // Finalize analytics: force a final snapshot and persist the session
    // volume-at-price profile (delta_by_price). Runs for live and replay.
    if let Some(engine) = &analytics_engine {
        let (final_snapshot, delta_rows) = {
            let mut eng = engine.lock().unwrap();
            info!("Analytics digest (live): {}", eng.digest().summarize());
            let hm_digest = eng.heatmap_digest();
            info!("Heatmap digest: {}", hm_digest.summarize());
            let snapshot = eng.force_snapshot();
            let mut rows = Vec::new();
            if let Some(ts) = eng.last_event_ts() {
                for (price, v) in eng.flow().volume_by_price() {
                    rows.push(DeltaByPriceRow {
                        session_id: analytics_session_id,
                        symbol: "BTCUSDT".to_string(),
                        ts_ms: futures_orderbook::storage::ms_to_datetime(ts),
                        price: *price,
                        buy_volume: v.buy_volume,
                        sell_volume: v.sell_volume,
                        total_volume: v.total_volume,
                        delta: v.delta,
                        trade_count: v.trade_count,
                        large_trade_count: v.large_trade_count,
                    });
                }
            }
            (snapshot, rows)
        };
        if let Some(sink) = &analytics_sink {
            let mut batch = AnalyticsBatch::default();
            if let Some(snap) = &final_snapshot {
                batch.snapshots.push(AnalyticsSnapshotRow::from_snapshot(
                    snap,
                    analytics_session_id,
                ));
            }
            batch.delta_by_price = delta_rows;
            sink.submit(batch);
        }
    }
    if let Some(handle) = analytics_sink_handle.take() {
        // Drop the sender first so the worker observes the channel close and
        // performs its final flush; otherwise the join would block forever.
        drop(analytics_sink);
        handle.join().await;
    }

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
