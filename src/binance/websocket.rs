use anyhow::Result;
use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use super::types::DepthUpdate;
use crate::config::Config;

/// Timeout for the WebSocket connection handshake.
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum number of raw messages to log in debug mode before throttling.
const MAX_RAW_LOG_PER_SECOND: usize = 5;

/// Messages sent from the WebSocket client to the main engine.
#[derive(Debug)]
pub enum WsMessage {
    /// A depth update event from the WebSocket.
    DepthUpdate(DepthUpdate),
    /// The WebSocket connection has been established.
    Connected,
    /// The WebSocket connection has been closed.
    Disconnected,
    /// An error occurred.
    Error(String),
}

/// WebSocket client for Binance USDⓈ-M Futures depth streams.
pub struct WebSocketClient {
    config: Config,
}

impl WebSocketClient {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Connect to the WebSocket and stream depth updates.
    ///
    /// This function runs the WebSocket loop, sending messages through the
    /// provided channel. It handles reconnection with exponential backoff.
    pub async fn run(&self, tx: mpsc::UnboundedSender<WsMessage>) -> Result<()> {
        let url = self.config.depth_stream_url();
        let mut reconnect_delay = Duration::from_millis(self.config.reconnect_base_ms);
        let max_delay = Duration::from_millis(self.config.reconnect_max_ms);
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            info!("WS connecting to {} (attempt {})", url, attempt);
            let _ = tx.send(WsMessage::Error(format!("CONNECTING attempt {}", attempt)));

            // Connect with timeout
            match timeout(WS_CONNECT_TIMEOUT, connect_async(&url)).await {
                Err(_) => {
                    error!(
                        "WebSocket connection timed out after {:?}",
                        WS_CONNECT_TIMEOUT
                    );
                    let _ = tx.send(WsMessage::Error(format!(
                        "WebSocket connection timed out after {:?}",
                        WS_CONNECT_TIMEOUT
                    )));
                }
                Ok(Err(e)) => {
                    error!("WebSocket connection failed: {}", e);
                    let _ = tx.send(WsMessage::Error(format!(
                        "WebSocket connection failed: {}",
                        e
                    )));
                }
                Ok(Ok((ws_stream, response))) => {
                    // Inspect the HTTP upgrade response
                    let status = response.status();
                    info!("WS HTTP upgrade response: {}", status);

                    // If not a successful upgrade (101 Switching Protocols), retry
                    if status.as_u16() != 101 {
                        error!("WebSocket upgrade rejected with HTTP {}", status);
                        let _ = tx.send(WsMessage::Error(format!(
                            "WebSocket upgrade rejected: HTTP {}",
                            status
                        )));
                        // Backoff and retry
                        warn!("Reconnecting in {:?}...", reconnect_delay);
                        sleep(reconnect_delay).await;
                        reconnect_delay = (reconnect_delay * 2).min(max_delay);
                        continue;
                    }

                    // Successfully connected
                    info!("WS connected successfully");
                    reconnect_delay = Duration::from_millis(self.config.reconnect_base_ms);
                    attempt = 0;

                    let _ = tx.send(WsMessage::Connected);
                    info!("WS SUBSCRIBED to {}", self.config.depth_stream_url());

                    let (mut write, mut read) = ws_stream.split();
                    let mut raw_log_count: usize = 0;
                    let mut last_log_reset = tokio::time::Instant::now();

                    // Read messages
                    while let Some(msg) = read.next().await {
                        // Reset raw log counter every second
                        if last_log_reset.elapsed() > Duration::from_secs(1) {
                            raw_log_count = 0;
                            last_log_reset = tokio::time::Instant::now();
                        }

                        match msg {
                            Ok(Message::Text(text)) => {
                                if raw_log_count < MAX_RAW_LOG_PER_SECOND {
                                    debug!(
                                        "WS MSG_RECV len={}: {}",
                                        text.len(),
                                        &text[..text.len().min(200)]
                                    );
                                    raw_log_count += 1;
                                }

                                match serde_json::from_str::<DepthUpdate>(&text) {
                                    Ok(update) => {
                                        debug!(
                                            "Depth update: u={}, pu={}, U={}, {} bids, {} asks",
                                            update.final_update_id,
                                            update.previous_final_update_id,
                                            update.first_update_id,
                                            update.bids.len(),
                                            update.asks.len()
                                        );
                                        let _ = tx.send(WsMessage::DepthUpdate(update));
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to parse depth update: {} (first 200 chars: {})",
                                            e,
                                            &text[..text.len().min(200)]
                                        );
                                        let _ = tx
                                            .send(WsMessage::Error(format!("Parse error: {}", e)));
                                    }
                                }
                            }
                            Ok(Message::Pong(_)) => {
                                debug!("WS pong received");
                            }
                            Ok(Message::Close(frame)) => {
                                let reason = frame
                                    .map(|f| format!("{:?}", f))
                                    .unwrap_or_else(|| "no reason".to_string());
                                info!("WS closed by server: {}", reason);
                                let _ = tx.send(WsMessage::Disconnected);
                                break;
                            }
                            Ok(Message::Ping(data)) => {
                                let _ = write.send(Message::Pong(data)).await;
                            }
                            Err(e) => {
                                error!("WS error: {}", e);
                                let _ = tx.send(WsMessage::Error(format!("WS error: {}", e)));
                                break;
                            }
                            Ok(Message::Binary(_)) => {
                                // Binary frame - skip
                                debug!("WS binary frame received (ignored)");
                            }
                            Ok(Message::Frame(_)) => {
                                // Raw frame - skip
                            }
                        }
                    }
                    info!("WS read loop ended, sending Disconnected");
                    let _ = tx.send(WsMessage::Disconnected);
                }
            }

            // Exponential backoff before reconnecting
            warn!("WS reconnecting in {:?}...", reconnect_delay);
            sleep(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(max_delay);
        }
    }
}
