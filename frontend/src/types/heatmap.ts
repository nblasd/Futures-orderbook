/**
 * Phase 5 HeatmapCell — integer-tick representation.
 * All price/quantity values are u64 ticks (1e8 scale) from the Rust backend.
 */
export interface HeatmapCell {
  price: number;
  resting_bid_liquidity: number;
  resting_ask_liquidity: number;
  liquidity_added: number;
  liquidity_removed: number;
  executed_buy_volume: number;
  executed_sell_volume: number;
  delta: number;
  trade_count: number;
  large_trade_volume: number;
  replenishment_count: number;
  absorption_candidate_count: number;
  sweep_count: number;
  pressure: number;
  timestamp_bucket: number | null;
}

/** Snapshot of a single cell for serialization/frame transfer. */
export interface HeatmapCellSnapshot {
  price_tick: number;
  resting_bid_liquidity: number;
  resting_ask_liquidity: number;
  liquidity_added: number;
  liquidity_removed: number;
  executed_buy_volume: number;
  executed_sell_volume: number;
  delta: number;
  trade_count: number;
  large_trade_volume: number;
  replenishment_count: number;
  absorption_candidate_count: number;
  sweep_count: number;
  pressure: number;
}

/** Summary metadata for a HeatmapFrame. */
export interface HeatmapSummary {
  total_price_levels: number;
  total_buckets: number;
  total_executed_buy: number;
  total_executed_sell: number;
  total_delta: number;
  total_trade_count: number;
  total_liquidity_added: number;
  total_liquidity_removed: number;
  total_large_trade_volume: number;
  total_replenishment_count: number;
  total_absorption_candidate_count: number;
  total_sweep_count: number;
}

/** Renderer-friendly snapshot of heatmap state. */
export interface HeatmapFrame {
  timestamp: number;
  visible_price_range: [number, number];
  time_range: [number, number];
  cells: HeatmapCellSnapshot[];
  summary: HeatmapSummary;
}

/** Incremental delta between frames. */
export interface HeatmapDelta {
  changed: [number, HeatmapCellSnapshot][];
  new: HeatmapCellSnapshot[];
  removed: number[];
  summary_delta: HeatmapSummaryDelta;
}

export interface HeatmapSummaryDelta {
  total_executed_buy: number;
  total_executed_sell: number;
  total_delta: number;
  total_trade_count: number;
  total_liquidity_added: number;
  total_liquidity_removed: number;
  total_large_trade_volume: number;
  total_replenishment_count: number;
  total_absorption_candidate_count: number;
  total_sweep_count: number;
}

/** Deterministic fingerprint for live/replay comparison. */
export interface HeatmapDigest {
  total_buckets: number;
  total_price_levels: number;
  total_executed_buy: number;
  total_executed_sell: number;
  total_delta: number;
  total_trade_count: number;
  total_liquidity_added: number;
  total_liquidity_removed: number;
  total_resting_bid: number;
  total_resting_ask: number;
  total_large_trade_volume: number;
  total_replenishment_count: number;
  total_absorption_candidate_count: number;
  total_sweep_count: number;
  total_pressure: number;
}

/** Visualization mode selector. */
export type HeatmapMode =
  | 'liquidity'
  | 'execution'
  | 'delta'
  | 'absorption'
  | 'sweeps'
  | 'pressure';

/** Connection status. */
export type ConnectionStatus = 'LIVE' | 'REPLAY' | 'DEMO' | 'DISCONNECTED';

/** Book readiness status. */
export type BookStatus = 'READY' | 'BUFFERING' | 'RESYNCING' | 'DISCONNECTED';

/** Market state snapshot for the header/status panels. */
export interface MarketState {
  connection: ConnectionStatus;
  bookStatus: BookStatus;
  symbol: string;
  exchange: string;
  bestBid: number;
  bestAsk: number;
  mid: number;
  spread: number;
  lastTradePrice: number;
  eventsPerSec: number;
  tradesPerSec: number;
  heatmapCells: number;
  sequenceErrors: number;
  queueDepth: number;
}

/** Trade event for overlay on heatmap. */
export interface TradeOverlay {
  price_tick: number;
  timestamp_ms: number;
  quantity_ticks: number;
  aggressor: 'BUY' | 'SELL';
  is_large: boolean;
}

/** Absorption marker for overlay. */
export interface AbsorptionMarker {
  price_tick: number;
  timestamp_ms: number;
  volume: number;
  direction: string;
  confidence: number;
}

/** Sweep marker for overlay. */
export interface SweepMarker {
  price_tick: number;
  timestamp_ms: number;
  volume: number;
  direction: string;
  price_range: [number, number];
  confidence: number;
}

/** Replenishment marker. */
export interface ReplenishmentMarker {
  price_tick: number;
  timestamp_ms: number;
  quantity: number;
  side: string;
}

/** Viewport state. */
export interface Viewport {
  /** Price range in ticks [lo, hi]. */
  priceRange: [number, number];
  /** Time range in ms [start, end]. */
  timeRange: [number, number];
  /** Whether auto-following current price. */
  follow: boolean;
  /** Whether playback is paused. */
  paused: boolean;
}

/** Tick scale constant — matches Rust TICK_SCALE = 100_000_000 (1e8). */
export const TICK_SCALE = 100_000_000;

/** Convert integer ticks to human-readable price. */
export function ticksToPrice(ticks: number): number {
  return ticks / TICK_SCALE;
}

/** Convert human-readable price to integer ticks. */
export function priceToTicks(price: number): number {
  return Math.round(price * TICK_SCALE);
}

/** Convert integer ticks to quantity (BTC). */
export function ticksToQuantity(ticks: number): number {
  return ticks / TICK_SCALE;
}
