use anyhow::Result;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use super::trade_types::FuturesTrade;
use crate::config::Config;

/// Timeout for the WebSocket connection handshake.
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum number of raw trade messages to log per second before throttling.
const MAX_RAW_LOG_PER_SECOND: usize = 5;

/// Messages sent from the trade WebSocket client to the main engine.
#[derive(Debug)]
pub enum TradeWsMessage {
    /// A parsed trade event.
    Trade(FuturesTrade),
    /// The WebSocket connection has been established.
    Connected,
    /// The WebSocket connection has been closed.
    Disconnected,
    /// An error occurred.
    Error(String),
}

/// WebSocket client for Binance USDⓈ-M Futures trade streams.
pub struct TradeWebSocketClient {
    config: Config,
}

impl TradeWebSocketClient {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Connect to the trade WebSocket and stream trade events.
    ///
    /// Handles reconnection with exponential backoff, identical to the
    /// depth WebSocket client's pattern.
    pub async fn run(&self, tx: mpsc::UnboundedSender<TradeWsMessage>) -> Result<()> {
        let url = self.config.trade_stream_url();
        let mut reconnect_delay = Duration::from_millis(self.config.reconnect_base_ms);
        let max_delay = Duration::from_millis(self.config.reconnect_max_ms);
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            info!("TRADE WS connecting to {} (attempt {})", url, attempt);
            let _ = tx.send(TradeWsMessage::Error(format!(
                "CONNECTING attempt {}",
                attempt
            )));

            match timeout(WS_CONNECT_TIMEOUT, connect_async(&url)).await {
                Err(_) => {
                    error!(
                        "Trade WebSocket connection timed out after {:?}",
                        WS_CONNECT_TIMEOUT
                    );
                    let _ = tx.send(TradeWsMessage::Error(format!(
                        "Trade WebSocket timed out after {:?}",
                        WS_CONNECT_TIMEOUT
                    )));
                }
                Ok(Err(e)) => {
                    error!("Trade WebSocket connection failed: {}", e);
                    let _ = tx.send(TradeWsMessage::Error(format!(
                        "Trade WebSocket connection failed: {}",
                        e
                    )));
                }
                Ok(Ok((ws_stream, response))) => {
                    let status = response.status();
                    info!("TRADE WS HTTP upgrade response: {}", status);

                    if status.as_u16() != 101 {
                        error!("Trade WebSocket upgrade rejected with HTTP {}", status);
                        let _ = tx.send(TradeWsMessage::Error(format!(
                            "Trade WebSocket upgrade rejected: HTTP {}",
                            status
                        )));
                        warn!("Trade WS reconnecting in {:?}...", reconnect_delay);
                        sleep(reconnect_delay).await;
                        reconnect_delay = (reconnect_delay * 2).min(max_delay);
                        continue;
                    }

                    info!("TRADE WS connected successfully");
                    reconnect_delay = Duration::from_millis(self.config.reconnect_base_ms);
                    attempt = 0;

                    let _ = tx.send(TradeWsMessage::Connected);
                    info!("TRADE WS SUBSCRIBED to {}", url);

                    let (mut _write, mut read) = ws_stream.split();
                    let mut raw_log_count: usize = 0;
                    let mut last_log_reset = tokio::time::Instant::now();

                    while let Some(msg) = read.next().await {
                        if last_log_reset.elapsed() > Duration::from_secs(1) {
                            raw_log_count = 0;
                            last_log_reset = tokio::time::Instant::now();
                        }

                        match msg {
                            Ok(Message::Text(text)) => {
                                if raw_log_count < MAX_RAW_LOG_PER_SECOND {
                                    debug!(
                                        "TRADE WS MSG_RECV len={}: {}",
                                        text.len(),
                                        &text[..text.len().min(200)]
                                    );
                                    raw_log_count += 1;
                                }

                                match serde_json::from_str::<FuturesTrade>(&text) {
                                    Ok(trade) => {
                                        debug!(
                                            "Trade: id={}, price={}, qty={}, maker={}",
                                            trade.trade_id,
                                            trade.price,
                                            trade.quantity,
                                            trade.is_buyer_maker
                                        );
                                        let _ = tx.send(TradeWsMessage::Trade(trade));
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to parse trade: {} (first 200 chars: {})",
                                            e,
                                            &text[..text.len().min(200)]
                                        );
                                        let _ = tx.send(TradeWsMessage::Error(format!(
                                            "Trade parse error: {}",
                                            e
                                        )));
                                    }
                                }
                            }
                            Ok(Message::Pong(_)) => {
                                debug!("TRADE WS pong received");
                            }
                            Ok(Message::Close(frame)) => {
                                let reason = frame
                                    .map(|f| format!("{:?}", f))
                                    .unwrap_or_else(|| "no reason".to_string());
                                info!("TRADE WS closed by server: {}", reason);
                                let _ = tx.send(TradeWsMessage::Disconnected);
                                break;
                            }
                            Ok(Message::Ping(data)) => {
                                // pong is handled by tungstenite automatically in most cases,
                                // but we handle it explicitly for completeness
                                debug!("TRADE WS ping received (len={})", data.len());
                            }
                            Err(e) => {
                                error!("TRADE WS error: {}", e);
                                let _ = tx
                                    .send(TradeWsMessage::Error(format!("TRADE WS error: {}", e)));
                                break;
                            }
                            Ok(Message::Binary(_)) => {
                                debug!("TRADE WS binary frame received (ignored)");
                            }
                            Ok(Message::Frame(_)) => {}
                        }
                    }
                    info!("TRADE WS read loop ended, sending Disconnected");
                    let _ = tx.send(TradeWsMessage::Disconnected);
                }
            }

            warn!("TRADE WS reconnecting in {:?}...", reconnect_delay);
            sleep(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(max_delay);
        }
    }
}
