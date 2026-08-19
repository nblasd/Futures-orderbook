use anyhow::Context;
use tracing::{debug, error, info, warn};

use super::types::{DepthSnapshot, ExchangeInfo};
use crate::config::Config;

/// Timeout for REST API requests.
const REST_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// REST client for Binance USDⓈ-M Futures public endpoints.
pub struct RestClient {
    client: reqwest::Client,
    config: Config,
}

impl RestClient {
    pub fn new(config: Config) -> Self {
        let client = reqwest::Client::builder()
            .timeout(REST_REQUEST_TIMEOUT)
            .build()
            .expect("Failed to create HTTP client");
        Self { client, config }
    }

    /// Fetch the order book depth snapshot for the configured symbol.
    ///
    /// GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000
    ///
    /// Returns the snapshot containing lastUpdateId, bids, and asks.
    pub async fn fetch_depth_snapshot(&self) -> anyhow::Result<DepthSnapshot> {
        let url = self.config.depth_rest_url();
        info!("REST requesting snapshot from {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("HTTP request to Binance REST API failed")?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<could not read body>".to_string());
            error!(
                "REST snapshot failed: HTTP {} from {} — body: {}",
                status,
                url,
                &body[..body.len().min(500)]
            );
            anyhow::bail!(
                "REST snapshot failed: HTTP {} — {}",
                status,
                &body[..body.len().min(200)]
            );
        }

        let snapshot: DepthSnapshot = response
            .json()
            .await
            .context("Failed to deserialize REST snapshot JSON")?;

        info!(
            "REST snapshot received: lastUpdateId={}, {} bid levels, {} ask levels",
            snapshot.last_update_id,
            snapshot.bids.len(),
            snapshot.asks.len()
        );

        Ok(snapshot)
    }

    /// Fetch exchange information to determine symbol precision.
    ///
    /// GET /fapi/v1/exchangeInfo
    pub async fn fetch_exchange_info(&self) -> anyhow::Result<ExchangeInfo> {
        let url = self.config.exchange_info_url();
        debug!("Fetching exchange info from {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("HTTP request for exchangeInfo failed")?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<could not read body>".to_string());
            warn!(
                "REST exchangeInfo failed: HTTP {} — body: {}",
                status,
                &body[..body.len().min(300)]
            );
            anyhow::bail!("REST exchangeInfo failed: HTTP {}", status);
        }

        let info: ExchangeInfo = response
            .json()
            .await
            .context("Failed to deserialize exchangeInfo JSON")?;

        Ok(info)
    }

    /// Get symbol-specific information by symbol name.
    pub async fn get_symbol_info(&self, symbol: &str) -> anyhow::Result<super::types::SymbolInfo> {
        let info = self.fetch_exchange_info().await?;
        info.symbols
            .into_iter()
            .find(|s| s.symbol == symbol)
            .ok_or_else(|| crate::error::BinanceError::SymbolNotFound(symbol.to_string()).into())
    }
}
