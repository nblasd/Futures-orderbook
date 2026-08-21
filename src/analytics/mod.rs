//! Phase 4: deterministic order-flow & market-microstructure analytics.
//!
//! The engine in [`engine::AnalyticsEngine`] consumes the same [`MarketEvent`]
//! stream in live and replay, producing derived analytics events and
//! [`snapshot::MarketMicrostructureSnapshot`]s. All prices and quantities are
//! integer ticks (1e8 scale); no floating point for authoritative values.

pub mod absorption;
pub mod book;
pub mod clusters;
pub mod config;
pub mod engine;
pub mod events;
pub mod flow;
pub mod heatmap;
pub mod large_trades;
pub mod liquidity;
pub mod snapshot;
pub mod sweeps;
