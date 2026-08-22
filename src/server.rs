//! WebSocket + static-file server for the Phase 6 Bookmap frontend.
//!
//! Architecture:
//! - Serves `frontend/dist/` as static files (production build).
//! - Exposes `/ws` WebSocket endpoint for live heatmap streaming.
//! - Uses `tokio::sync::broadcast` to fan-out HeatmapFrame snapshots
//!   and HeatmapDelta updates to all connected clients.
//!
//! The server does NOT compute analytics — it relays authoritative
//! state from the Rust `AnalyticsEngine` / `Heatmap`.

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};

use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::{debug, info, warn};

use crate::analytics::heatmap::{HeatmapCellSnapshot, HeatmapDelta, HeatmapFrame, HeatmapSummary};

// ---------------------------------------------------------------------------
// Server messages sent to the frontend
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMessage {
    /// Full heatmap frame snapshot — sent on initial connect and recovery.
    Snapshot { frame: HeatmapFrameSerde },
    /// Incremental delta update.
    Delta { delta: HeatmapDeltaSerde },
    /// Backend status info.
    Status { status: StatusInfo },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub connection: String,
    pub book_status: String,
    pub symbol: String,
    pub exchange: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
    pub spread: f64,
    pub events_per_sec: u64,
    pub trades_per_sec: u64,
    pub heatmap_cells: usize,
    pub sequence_errors: u64,
    pub queue_depth: usize,
}

// ---------------------------------------------------------------------------
// Client messages from the frontend
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Client requests a fresh full snapshot (e.g. after reconnect).
    RequestSnapshot,
    /// Load and start replaying a recorded session.
    ReplayLoad {
        session_id: String,
        speed: Option<f64>,
    },
    /// Resume replay playback.
    ReplayPlay,
    /// Pause replay playback.
    ReplayPause,
    /// Set replay playback speed.
    ReplaySpeed { speed: f64 },
    /// Stop replay.
    ReplayStop,
}

// ---------------------------------------------------------------------------
// JSON-friendly serialisation types (mirror Rust structs exactly)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapFrameSerde {
    pub timestamp: u64,
    pub visible_price_range: (u64, u64),
    pub time_range: (u64, u64),
    pub cells: Vec<HeatmapCellSnapshot>,
    pub summary: HeatmapSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapDeltaSerde {
    pub changed: Vec<(u64, HeatmapCellSnapshot)>,
    pub new: Vec<HeatmapCellSnapshot>,
    pub removed: Vec<u64>,
    pub summary_delta: SummaryDeltaSerde,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryDeltaSerde {
    pub total_executed_buy: u64,
    pub total_executed_sell: u64,
    pub total_delta: i128,
    pub total_trade_count: u64,
    pub total_liquidity_added: u64,
    pub total_liquidity_removed: u64,
    pub total_large_trade_volume: u64,
    pub total_replenishment_count: u64,
    pub total_absorption_candidate_count: u64,
    pub total_sweep_count: u64,
}

impl From<&HeatmapFrame> for HeatmapFrameSerde {
    fn from(f: &HeatmapFrame) -> Self {
        Self {
            timestamp: f.timestamp,
            visible_price_range: f.visible_price_range,
            time_range: f.time_range,
            cells: f.cells.clone(),
            summary: f.summary.clone(),
        }
    }
}

impl From<&HeatmapDelta> for HeatmapDeltaSerde {
    fn from(d: &HeatmapDelta) -> Self {
        Self {
            changed: d.changed.clone(),
            new: d.new.clone(),
            removed: d.removed.clone(),
            summary_delta: SummaryDeltaSerde {
                total_executed_buy: d.summary_delta.total_executed_buy,
                total_executed_sell: d.summary_delta.total_executed_sell,
                total_delta: d.summary_delta.total_delta,
                total_trade_count: d.summary_delta.total_trade_count,
                total_liquidity_added: d.summary_delta.total_liquidity_added,
                total_liquidity_removed: d.summary_delta.total_liquidity_removed,
                total_large_trade_volume: d.summary_delta.total_large_trade_volume,
                total_replenishment_count: d.summary_delta.total_replenishment_count,
                total_absorption_candidate_count: d.summary_delta.total_absorption_candidate_count,
                total_sweep_count: d.summary_delta.total_sweep_count,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Shared server state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ServerState {
    /// Broadcast sender for heatmap frame snapshots.
    pub frame_tx: broadcast::Sender<HeatmapFrameSerde>,
    /// Broadcast sender for heatmap deltas.
    pub delta_tx: broadcast::Sender<HeatmapDeltaSerde>,
    /// Broadcast sender for status updates.
    pub status_tx: broadcast::Sender<StatusInfo>,
    /// Path to the frontend dist directory.
    pub frontend_dir: PathBuf,
    /// Optional replay controller.
    pub replay: Option<std::sync::Arc<tokio::sync::RwLock<crate::server_replay::ReplayController>>>,
    /// Storage for loading replay sessions.
    pub storage: Option<std::sync::Arc<dyn crate::storage::Storage>>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start the HTTP + WebSocket server on the given address.
/// Returns a join handle for the server task.
pub fn start_server(
    addr: SocketAddr,
    frontend_dir: PathBuf,
    frame_rx: broadcast::Sender<HeatmapFrameSerde>,
    delta_rx: broadcast::Sender<HeatmapDeltaSerde>,
    status_rx: broadcast::Sender<StatusInfo>,
    replay: Option<std::sync::Arc<tokio::sync::RwLock<crate::server_replay::ReplayController>>>,
    storage: Option<std::sync::Arc<dyn crate::storage::Storage>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let state = ServerState {
        frame_tx: frame_rx,
        delta_tx: delta_rx,
        status_tx: status_rx,
        frontend_dir,
        replay,
        storage,
    };

    let cors = CorsLayer::permissive();

    let static_service = ServeDir::new(&state.frontend_dir)
        .append_index_html_on_directories(true)
        .fallback(ServeDir::new(&state.frontend_dir));

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(static_service)
        .layer(cors)
        .with_state(state);

    tokio::spawn(async move {
        info!("WebSocket server starting on {addr}");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .await
            .map_err(|e| anyhow::anyhow!("Server error: {e}"))
    })
}

// ---------------------------------------------------------------------------
// WebSocket handler
// ---------------------------------------------------------------------------

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<ServerState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: ServerState) {
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to broadcasts
    let mut frame_rx = state.frame_tx.subscribe();
    let mut delta_rx = state.delta_tx.subscribe();
    let mut status_rx = state.status_tx.subscribe();

    // Send initial snapshot (wait up to 5s for first frame)
    match tokio::time::timeout(std::time::Duration::from_secs(5), frame_rx.recv()).await {
        Ok(Ok(frame)) => {
            let msg = ServerMessage::Snapshot { frame };
            if let Ok(json) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json.into())).await.is_err() {
                    warn!("Client disconnected before initial snapshot");
                    return;
                }
            }
        }
        _ => {
            warn!("Timeout waiting for initial heatmap frame");
            // Send empty status
            let msg = ServerMessage::Status {
                status: StatusInfo {
                    connection: "CONNECTING".into(),
                    book_status: "BUFFERING".into(),
                    symbol: "BTCUSDT".into(),
                    exchange: "Binance USDⓈ-M Futures".into(),
                    best_bid: 0.0,
                    best_ask: 0.0,
                    mid: 0.0,
                    spread: 0.0,
                    events_per_sec: 0,
                    trades_per_sec: 0,
                    heatmap_cells: 0,
                    sequence_errors: 0,
                    queue_depth: 0,
                },
            };
            if let Ok(json) = serde_json::to_string(&msg) {
                let _ = sender.send(Message::Text(json.into())).await;
            }
        }
    }

    // Main loop: fan-out updates + receive client messages
    let mut pending_snapshots = 0u32;
    let max_pending = 8;

    loop {
        tokio::select! {
            // Receive from client
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                            match client_msg {
                        ClientMessage::RequestSnapshot => {
                            if let Ok(frame) = frame_rx.try_recv() {
                                let msg = ServerMessage::Snapshot { frame };
                                if let Ok(json) = serde_json::to_string(&msg) {
                                    if sender.send(Message::Text(json.into())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        ClientMessage::ReplayLoad { session_id, speed } => {
                            if let (Some(replay_ctl), Some(storage)) = (&state.replay, &state.storage) {
                                let sid = match uuid::Uuid::parse_str(&session_id) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        warn!("Invalid session ID: {e}");
                                        continue;
                                    }
                                };
                                let spd = speed.unwrap_or(1.0);
                                let frame_tx = state.frame_tx.clone();
                                let delta_tx = state.delta_tx.clone();
                                let status_tx = state.status_tx.clone();
                                let storage_clone = storage.clone();
                                let ctl_arc = replay_ctl.clone();
                                // Create new controller and replace
                                let new_ctl = crate::server_replay::ReplayController::new(
                                    frame_tx, delta_tx, status_tx
                                );
                                {
                                    let mut w = ctl_arc.write().await;
                                    *w = new_ctl;
                                }
                                // Spawn replay in background
                                let spawn_ctl = ctl_arc.clone();
                                tokio::spawn(async move {
                                    let ctl = spawn_ctl.read().await;
                                    ctl.load_and_run(storage_clone, sid, spd).await;
                                });
                            }
                        }
                        ClientMessage::ReplayPlay => {
                            if let Some(replay_ctl) = &state.replay {
                                let ctl = replay_ctl.read().await;
                                ctl.play().await;
                            }
                        }
                        ClientMessage::ReplayPause => {
                            if let Some(replay_ctl) = &state.replay {
                                let ctl = replay_ctl.read().await;
                                ctl.pause().await;
                            }
                        }
                        ClientMessage::ReplaySpeed { speed } => {
                            if let Some(replay_ctl) = &state.replay {
                                let ctl = replay_ctl.read().await;
                                ctl.set_speed(speed).await;
                            }
                        }
                        ClientMessage::ReplayStop => {
                            if let Some(replay_ctl) = &state.replay {
                                let ctl = replay_ctl.read().await;
                                ctl.stop().await;
                            }
                        }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            // Receive frame snapshot
            Ok(frame) = frame_rx.recv() => {
                pending_snapshots = pending_snapshots.saturating_add(1);
                if pending_snapshots <= max_pending {
                    let msg = ServerMessage::Snapshot { frame };
                    if let Ok(json) = serde_json::to_string(&msg) {
                        if sender.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
            // Receive delta
            Ok(delta) = delta_rx.recv() => {
                if pending_snapshots > 0 {
                    pending_snapshots = pending_snapshots.saturating_sub(1);
                }
                let msg = ServerMessage::Delta { delta };
                if let Ok(json) = serde_json::to_string(&msg) {
                    if sender.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
            // Receive status
            Ok(status) = status_rx.recv() => {
                let msg = ServerMessage::Status { status };
                if let Ok(json) = serde_json::to_string(&msg) {
                    if sender.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    debug!("WebSocket client disconnected");
}
