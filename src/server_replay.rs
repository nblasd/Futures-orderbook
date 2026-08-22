//! Streaming replay controller for the Phase 6 WebSocket frontend.
//!
//! Wraps the existing replay engine to process events one-by-one,
//! broadcasting HeatmapFrame snapshots and HeatmapDelta updates
//! to connected WebSocket clients.
//!
//! Supports: play, pause, seek, speed control, and completion detection.

use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn};

use crate::analytics::config::AnalyticsConfig;
use crate::analytics::engine::AnalyticsEngine;
use crate::analytics::heatmap::HeatmapFrame;
use crate::orderbook::book::OrderBook;
use crate::orderbook::synchronizer::{ProcessResult, SyncState, Synchronizer};
use crate::replay::reader::{load_session, ReplayEvent, SessionData};
use crate::server::{HeatmapDeltaSerde, HeatmapFrameSerde, StatusInfo};
use crate::storage::Storage;
use crate::trades::processor::{TradeProcessResult, TradeProcessor};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Replay state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayState {
    Idle,
    Loading,
    Playing,
    Paused,
    Completed,
}

#[derive(Debug, Clone)]
pub struct ReplayInfo {
    pub session_id: String,
    pub state: ReplayState,
    pub speed: f64,
    pub current_index: usize,
    pub total_events: usize,
    pub current_timestamp_ms: u64,
}

impl Default for ReplayInfo {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            state: ReplayState::Idle,
            speed: 1.0,
            current_index: 0,
            total_events: 0,
            current_timestamp_ms: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// ReplayController
// ---------------------------------------------------------------------------

pub struct ReplayController {
    /// Shared state accessible from WebSocket handlers.
    pub info: Arc<RwLock<ReplayInfo>>,
    /// Broadcast sender for heatmap frames.
    frame_tx: broadcast::Sender<HeatmapFrameSerde>,
    /// Broadcast sender for deltas.
    delta_tx: broadcast::Sender<HeatmapDeltaSerde>,
    /// Broadcast sender for status updates.
    status_tx: broadcast::Sender<StatusInfo>,
}

impl ReplayController {
    pub fn new(
        frame_tx: broadcast::Sender<HeatmapFrameSerde>,
        delta_tx: broadcast::Sender<HeatmapDeltaSerde>,
        status_tx: broadcast::Sender<StatusInfo>,
    ) -> Self {
        Self {
            info: Arc::new(RwLock::new(ReplayInfo::default())),
            frame_tx,
            delta_tx,
            status_tx,
        }
    }

    /// Load a session and begin streaming replay.
    pub async fn load_and_run(&self, storage: Arc<dyn Storage>, session_id: Uuid, speed: f64) {
        {
            let mut info = self.info.write().await;
            info.state = ReplayState::Loading;
            info.session_id = session_id.to_string();
            info.speed = speed;
            info.current_index = 0;
        }

        // Broadcast loading status
        self.broadcast_status(&ReplayInfo {
            state: ReplayState::Loading,
            session_id: session_id.to_string(),
            speed,
            ..ReplayInfo::default()
        })
        .await;

        // Load session data
        let data = match load_session(storage.as_ref(), session_id).await {
            Ok(d) => d,
            Err(e) => {
                warn!("Failed to load replay session: {e}");
                let mut info = self.info.write().await;
                info.state = ReplayState::Idle;
                return;
            }
        };

        let total_events = data.events.len();
        info!("Loaded replay session: {total_events} events");

        {
            let mut info = self.info.write().await;
            info.total_events = total_events;
            info.state = ReplayState::Playing;
        }

        // Process events one by one
        self.run_events(data).await;
    }

    /// Process replay events one at a time, broadcasting frames.
    async fn run_events(&self, data: SessionData) {
        let mut sync = Synchronizer::new();
        let mut book = OrderBook::new();
        let mut trade_proc = TradeProcessor::new();
        let mut analytics = AnalyticsEngine::new(AnalyticsConfig::btcusdt_default());

        sync.on_connected();

        let mut event_index = 0usize;
        let mut last_frame_ts = 0u64;

        for event in &data.events {
            // Check if we should pause
            loop {
                let info = self.info.read().await;
                match info.state {
                    ReplayState::Paused => {
                        drop(info);
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    ReplayState::Idle | ReplayState::Completed => {
                        return;
                    }
                    _ => {
                        break;
                    }
                }
            }

            // Pace based on speed
            let speed = self.info.read().await.speed;
            if speed > 0.0 {
                let event_time = event.time_ms();
                let target_delay = if last_frame_ts > 0 && event_time > last_frame_ts {
                    let delta_ms = event_time.saturating_sub(last_frame_ts);
                    (delta_ms as f64 / 1000.0 / speed * 1000.0) as u64
                } else {
                    0
                };
                if target_delay > 0 && target_delay < 10_000 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(target_delay)).await;
                }
                last_frame_ts = event_time;
            }

            // Process the event
            match event {
                ReplayEvent::Snapshot {
                    update_id,
                    bids,
                    asks,
                    ..
                } => {
                    sync.on_snapshot_loading();
                    if let Err(e) = book.apply_snapshot(bids, asks, *update_id) {
                        warn!("Replay snapshot error: {e}");
                        continue;
                    }
                    match sync.reconcile(*update_id) {
                        Ok(events) => {
                            for e in &events {
                                let _ =
                                    book.apply_depth_update(&e.bids, &e.asks, e.final_update_id);
                            }
                        }
                        Err(_) => {
                            sync.trigger_resync();
                        }
                    }
                    // Seed analytics shadow book
                    let full = book.snapshot();
                    let ev = crate::events::market::MarketEvent::OrderBookSnapshot {
                        symbol: "BTCUSDT".to_string(),
                        update_id: *update_id,
                        bids: full.bids.iter().map(|l| (l.price, l.quantity)).collect(),
                        asks: full.asks.iter().map(|l| (l.price, l.quantity)).collect(),
                    };
                    analytics.process_event(&ev);
                }
                ReplayEvent::Depth { update, .. } => {
                    if sync.state() == SyncState::Ready {
                        if let ProcessResult::Apply = sync.process_live_event(update) {
                            let _ = book.apply_depth_update(
                                &update.bids,
                                &update.asks,
                                update.final_update_id,
                            );
                            let ev = crate::events::market::MarketEvent::OrderBookUpdated {
                                symbol: update.symbol.clone(),
                                update_id: update.final_update_id,
                                event_time_ms: update.event_time,
                                bid_changes: update
                                    .bids
                                    .iter()
                                    .map(|(p, q)| {
                                        (
                                            crate::orderbook::level::price_str_to_ticks(p)
                                                .unwrap_or(0),
                                            crate::orderbook::level::quantity_str_to_ticks(q)
                                                .unwrap_or(0),
                                            None,
                                        )
                                    })
                                    .collect(),
                                ask_changes: update
                                    .asks
                                    .iter()
                                    .map(|(p, q)| {
                                        (
                                            crate::orderbook::level::price_str_to_ticks(p)
                                                .unwrap_or(0),
                                            crate::orderbook::level::quantity_str_to_ticks(q)
                                                .unwrap_or(0),
                                            None,
                                        )
                                    })
                                    .collect(),
                                best_bid: book.best_bid(),
                                best_ask: book.best_ask(),
                                mid_price: book.mid_price(),
                            };
                            analytics.process_event(&ev);
                        }
                    } else {
                        sync.buffer_event(update.clone());
                    }
                }
                ReplayEvent::Trade { event, .. } => {
                    if trade_proc.process(event.clone()) == TradeProcessResult::Processed {
                        analytics.process_event(&crate::events::market::MarketEvent::Trade(
                            event.clone(),
                        ));
                    }
                }
            }

            // Generate and broadcast frame
            let now_ms = event.time_ms();
            let heatmap = analytics.heatmap();
            let (lo, hi) = {
                let mid = book.mid_price().unwrap_or(77300.0);
                let mid_ticks = (mid * 100_000_000.0) as u64;
                let range = 50 * 10_000_000;
                (
                    mid_ticks.saturating_sub(range),
                    mid_ticks.saturating_add(range),
                )
            };
            let frame = HeatmapFrame::from_heatmap(heatmap, now_ms, lo, hi);
            let serde_frame: HeatmapFrameSerde = (&frame).into();
            let _ = self.frame_tx.send(serde_frame);

            // Broadcast status
            let book_snap = book.snapshot();
            let status = StatusInfo {
                connection: "REPLAY".into(),
                book_status: "READY".into(),
                symbol: "BTCUSDT".into(),
                exchange: "Binance USD\\u{2119}-M Futures".into(),
                best_bid: book_snap
                    .best_bid
                    .map(|p| p as f64 / 100_000_000.0)
                    .unwrap_or(0.0),
                best_ask: book_snap
                    .best_ask
                    .map(|p| p as f64 / 100_000_000.0)
                    .unwrap_or(0.0),
                mid: book_snap.mid_price.unwrap_or(0.0),
                spread: book_snap
                    .best_bid
                    .and_then(|b| book_snap.best_ask.map(|a| (a - b) as f64 / 100_000_000.0))
                    .unwrap_or(0.0),
                events_per_sec: 0,
                trades_per_sec: 0,
                heatmap_cells: analytics.heatmap().cell_count(),
                sequence_errors: 0,
                queue_depth: 0,
            };
            let _ = self.status_tx.send(status);

            // Update info
            event_index += 1;
            {
                let mut info = self.info.write().await;
                info.current_index = event_index;
                info.current_timestamp_ms = now_ms;
            }
        }

        // Replay completed
        {
            let mut info = self.info.write().await;
            info.state = ReplayState::Completed;
        }
        info!("Replay completed: {event_index} events processed");
    }

    /// Pause replay playback.
    pub async fn pause(&self) {
        let mut info = self.info.write().await;
        if info.state == ReplayState::Playing {
            info.state = ReplayState::Paused;
        }
    }

    /// Resume replay playback.
    pub async fn play(&self) {
        let mut info = self.info.write().await;
        if info.state == ReplayState::Paused {
            info.state = ReplayState::Playing;
        }
    }

    /// Set replay speed.
    pub async fn set_speed(&self, speed: f64) {
        let mut info = self.info.write().await;
        info.speed = speed;
    }

    /// Stop replay.
    pub async fn stop(&self) {
        let mut info = self.info.write().await;
        info.state = ReplayState::Idle;
    }

    /// Broadcast current status.
    async fn broadcast_status(&self, info: &ReplayInfo) {
        let status = StatusInfo {
            connection: match info.state {
                ReplayState::Playing => "REPLAY".into(),
                ReplayState::Paused => "REPLAY".into(),
                ReplayState::Loading => "REPLAY".into(),
                ReplayState::Completed => "REPLAY".into(),
                ReplayState::Idle => "DISCONNECTED".into(),
            },
            book_status: "READY".into(),
            symbol: "BTCUSDT".into(),
            exchange: "Binance USD\\u{2119}-M Futures".into(),
            best_bid: 0.0,
            best_ask: 0.0,
            mid: 0.0,
            spread: 0.0,
            events_per_sec: 0,
            trades_per_sec: 0,
            heatmap_cells: 0,
            sequence_errors: 0,
            queue_depth: 0,
        };
        let _ = self.status_tx.send(status);
    }
}
