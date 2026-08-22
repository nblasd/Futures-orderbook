/**
 * Application state store — manages heatmap, viewport, and UI state.
 * Uses a simple pub/sub pattern to avoid heavy state management libraries.
 */

import {
  HeatmapFrame,
  HeatmapMode,
  HeatmapDelta,
  MarketState,
  ConnectionStatus,
  TradeOverlay,
  AbsorptionMarker,
  SweepMarker,
  ReplenishmentMarker,
  Viewport,
} from '../types/heatmap';
import { MockEngine } from '../market/mock-engine';

type Listener = () => void;

export interface AppState {
  // Market data
  frame: HeatmapFrame | null;
  frames: HeatmapFrame[];
  currentFrameIndex: number;
  market: MarketState;
  trades: TradeOverlay[];
  absorptionMarkers: AbsorptionMarker[];
  sweepMarkers: SweepMarker[];
  replenishmentMarkers: ReplenishmentMarker[];

  // Viewport
  viewport: Viewport;

  // UI
  mode: HeatmapMode;
  connection: ConnectionStatus;
  staleData: boolean;
  lastUpdateTime: number;
  timeRange: number; // ms visible
  priceAggregation: number;
}

const DEFAULT_VIEWPORT: Viewport = {
  priceRange: [77_200 * 10_000_000, 77_400 * 10_000_000],
  timeRange: [Date.now() - 60_000, Date.now()],
  follow: true,
  paused: false,
};

const DEFAULT_MARKET: MarketState = {
  connection: 'DISCONNECTED',
  bookStatus: 'DISCONNECTED',
  symbol: 'BTCUSDT',
  exchange: 'Binance USDⓈ-M Futures',
  bestBid: 0,
  bestAsk: 0,
  mid: 0,
  spread: 0,
  lastTradePrice: 0,
  eventsPerSec: 0,
  tradesPerSec: 0,
  heatmapCells: 0,
  sequenceErrors: 0,
  queueDepth: 0,
};

export class Store {
  state: AppState;
  private listeners: Set<Listener> = new Set();
  private mockEngine: MockEngine;
  private mockInterval: ReturnType<typeof setInterval> | null = null;
  private staleTimeout: ReturnType<typeof setTimeout> | null = null;
  private frameBuffer: HeatmapFrame[] = [];
  private maxBufferFrames = 300; // 5 minutes at 1/sec

  constructor() {
    this.mockEngine = new MockEngine(42);
    this.state = {
      frame: null,
      frames: [],
      currentFrameIndex: 0,
      market: { ...DEFAULT_MARKET },
      trades: [],
      absorptionMarkers: [],
      sweepMarkers: [],
      replenishmentMarkers: [],
      viewport: { ...DEFAULT_VIEWPORT },
      mode: 'liquidity',
      connection: 'DEMO',
      staleData: false,
      lastUpdateTime: Date.now(),
      timeRange: 60_000,
      priceAggregation: 1,
    };
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private notify(): void {
    for (const listener of this.listeners) listener();
  }

  /** Start mock data generation. */
  startDemo(intervalMs: number = 500): void {
    this.stopDemo();
    this.state.connection = 'DEMO';
    this.state.market.connection = 'DEMO';

    this.mockEngine = new MockEngine(42);
    this.frameBuffer = [];

    const tick = () => {
      const now = Date.now();
      const frame = this.mockEngine.generateFrame(now);
      const delta =
        this.frameBuffer.length > 0
          ? this.mockEngine.generateDelta(this.frameBuffer[this.frameBuffer.length - 1], frame)
          : null;

      this.frameBuffer.push(frame);
      if (this.frameBuffer.length > this.maxBufferFrames) {
        this.frameBuffer.shift();
      }

      this.state.frame = frame;
      this.state.frames = this.frameBuffer;
      this.state.currentFrameIndex = this.frameBuffer.length - 1;
      this.state.trades = this.mockEngine.generateTrades(frame);
      this.state.absorptionMarkers = this.mockEngine.generateAbsorptionMarkers(frame);
      this.state.sweepMarkers = this.mockEngine.generateSweepMarkers(frame);
      this.state.replenishmentMarkers = this.mockEngine.generateReplenishmentMarkers(frame);
      this.state.market = this.mockEngine.getMarketState();
      this.state.market.connection = 'DEMO';
      this.state.lastUpdateTime = now;
      this.state.staleData = false;

      // Auto-follow: update viewport if following
      if (this.state.viewport.follow && !this.state.viewport.paused) {
        const priceLo = frame.visible_price_range[0];
        const priceHi = frame.visible_price_range[1];
        const padding = (priceHi - priceLo) * 0.3;
        this.state.viewport = {
          ...this.state.viewport,
          priceRange: [priceLo - padding, priceHi + padding],
          timeRange: [now - this.state.timeRange, now + 2000],
        };
      }

      this.notify();
    };

    // Initial frame
    tick();
    this.mockInterval = setInterval(tick, intervalMs);
  }

  stopDemo(): void {
    if (this.mockInterval) {
      clearInterval(this.mockInterval);
      this.mockInterval = null;
    }
  }

  setMode(mode: HeatmapMode): void {
    this.state.mode = mode;
    this.notify();
  }

  toggleFollow(): void {
    this.state.viewport = { ...this.state.viewport, follow: !this.state.viewport.follow };
    this.notify();
  }

  togglePause(): void {
    this.state.viewport = { ...this.state.viewport, paused: !this.state.viewport.paused };
    this.notify();
  }

  setTimeRange(ms: number): void {
    this.state.timeRange = ms;
    if (this.state.frame) {
      const now = this.state.frame.timestamp;
      this.state.viewport = { ...this.state.viewport, timeRange: [now - ms, now + 2000] };
    }
    this.notify();
  }

  setPriceAggregation(ticks: number): void {
    this.state.priceAggregation = ticks;
    this.notify();
  }

  setViewportPriceRange(lo: number, hi: number): void {
    this.state.viewport = { ...this.state.viewport, priceRange: [lo, hi], follow: false };
    this.notify();
  }

  zoomPrices(factor: number): void {
    const [lo, hi] = this.state.viewport.priceRange;
    const mid = (lo + hi) / 2;
    const halfRange = ((hi - lo) / 2) * factor;
    this.state.viewport = { ...this.state.viewport, priceRange: [mid - halfRange, mid + halfRange], follow: false };
    this.notify();
  }

  zoomTime(factor: number): void {
    const [start, end] = this.state.viewport.timeRange;
    const mid = (start + end) / 2;
    const halfRange = ((end - start) / 2) * factor;
    this.state.viewport = { ...this.state.viewport, timeRange: [mid - halfRange, mid + halfRange] };
    this.notify();
  }

  panTime(deltaMs: number): void {
    const [start, end] = this.state.viewport.timeRange;
    this.state.viewport = { ...this.state.viewport, timeRange: [start + deltaMs, end + deltaMs], follow: false };
    this.notify();
  }

  resetViewport(): void {
    this.state.viewport = { ...DEFAULT_VIEWPORT };
    this.notify();
  }

  /** Get the current frame for the given index (replay). */
  setFrameIndex(index: number): void {
    if (index >= 0 && index < this.frameBuffer.length) {
      this.state.currentFrameIndex = index;
      this.state.frame = this.frameBuffer[index];
      this.notify();
    }
  }

  // ---- Live data from TransportClient ----

  /** Receive a full heatmap frame from the backend. */
  receiveFrame(frame: HeatmapFrame): void {
    this.stopDemo(); // Stop mock if running
    this.state.connection = 'LIVE';
    this.state.frame = frame;
    this.state.frames.push(frame);
    if (this.state.frames.length > this.maxBufferFrames) {
      this.state.frames.shift();
    }
    this.state.currentFrameIndex = this.state.frames.length - 1;
    this.state.lastUpdateTime = Date.now();
    this.state.staleData = false;

    // Generate overlays from frame
    this.state.trades = this.generateTradesFromFrame(frame);
    this.state.absorptionMarkers = this.generateAbsorptionFromFrame(frame);
    this.state.sweepMarkers = this.generateSweepFromFrame(frame);
    this.state.replenishmentMarkers = this.generateReplenishFromFrame(frame);

    // Auto-follow
    if (this.state.viewport.follow && !this.state.viewport.paused) {
      const priceLo = frame.visible_price_range[0];
      const priceHi = frame.visible_price_range[1];
      const padding = (priceHi - priceLo) * 0.3;
      const now = frame.timestamp;
      this.state.viewport = {
        ...this.state.viewport,
        priceRange: [priceLo - padding, priceHi + padding],
        timeRange: [now - this.state.timeRange, now + 2000],
      };
    }

    this.notify();
  }

  /** Receive a delta update from the backend. */
  receiveDelta(delta: HeatmapDelta): void {
    if (!this.state.frame) return;

    // Apply delta to current frame
    const cellMap = new Map(this.state.frame.cells.map((c) => [c.price_tick, c]));
    for (const [price, cell] of delta.changed) {
      cellMap.set(price, cell);
    }
    for (const cell of delta.new) {
      cellMap.set(cell.price_tick, cell);
    }
    for (const price of delta.removed) {
      cellMap.delete(price);
    }

    const newCells = Array.from(cellMap.values()).sort(
      (a, b) => a.price_tick - b.price_tick
    );

    // Update summary with delta
    const s = this.state.frame.summary;
    const ds = delta.summary_delta;
    this.state.frame = {
      ...this.state.frame,
      cells: newCells,
      summary: {
        ...s,
        total_executed_buy: s.total_executed_buy + ds.total_executed_buy,
        total_executed_sell: s.total_executed_sell + ds.total_executed_sell,
        total_delta: s.total_delta + ds.total_delta,
        total_trade_count: s.total_trade_count + ds.total_trade_count,
        total_liquidity_added: s.total_liquidity_added + ds.total_liquidity_added,
        total_liquidity_removed: s.total_liquidity_removed + ds.total_liquidity_removed,
        total_large_trade_volume:
          s.total_large_trade_volume + ds.total_large_trade_volume,
        total_replenishment_count:
          s.total_replenishment_count + ds.total_replenishment_count,
        total_absorption_candidate_count:
          s.total_absorption_candidate_count +
          ds.total_absorption_candidate_count,
        total_sweep_count: s.total_sweep_count + ds.total_sweep_count,
      },
    };

    this.state.lastUpdateTime = Date.now();
    this.state.staleData = false;
    this.notify();
  }

  /** Receive status update from the backend. */
  receiveStatus(status: Partial<MarketState>): void {
    // Must create a new object reference so React detects the change
    this.state.market = { ...this.state.market, ...status };
    this.notify();
  }

  /** Set connection status from transport. */
  setConnection(status: ConnectionStatus): void {
    this.state.connection = status;
    // Must create a new object reference so React detects the change
    this.state.market = { ...this.state.market, connection: status };
    this.notify();
  }

  // ---- Overlay generators from frame data ----

  private generateTradesFromFrame(frame: HeatmapFrame) {
    const trades: TradeOverlay[] = [];
    for (const cell of frame.cells) {
      if (cell.trade_count > 0) {
        const isBuy = cell.executed_buy_volume > cell.executed_sell_volume;
        trades.push({
          price_tick: cell.price_tick,
          timestamp_ms: frame.timestamp,
          quantity_ticks:
            cell.executed_buy_volume + cell.executed_sell_volume,
          aggressor: isBuy ? 'BUY' : 'SELL',
          is_large: cell.large_trade_volume > 0,
        });
      }
    }
    return trades;
  }

  private generateAbsorptionFromFrame(frame: HeatmapFrame) {
    return frame.cells
      .filter((c) => c.absorption_candidate_count > 0)
      .map((c) => ({
        price_tick: c.price_tick,
        timestamp_ms: frame.timestamp,
        volume: c.executed_buy_volume + c.executed_sell_volume,
        direction: c.delta > 0 ? 'BUY' : 'SELL',
        confidence: 0.7,
      }));
  }

  private generateSweepFromFrame(frame: HeatmapFrame) {
    return frame.cells
      .filter((c) => c.sweep_count > 0)
      .map((c) => ({
        price_tick: c.price_tick,
        timestamp_ms: frame.timestamp,
        volume: c.executed_buy_volume + c.executed_sell_volume,
        direction: c.delta > 0 ? 'BUY' : 'SELL',
        price_range: [c.price_tick - 50_000_000, c.price_tick + 50_000_000] as [number, number],
        confidence: 0.7,
      }));
  }

  private generateReplenishFromFrame(frame: HeatmapFrame) {
    return frame.cells
      .filter((c) => c.replenishment_count > 0)
      .map((c) => ({
        price_tick: c.price_tick,
        timestamp_ms: frame.timestamp,
        quantity: c.resting_bid_liquidity + c.resting_ask_liquidity,
        side: c.resting_bid_liquidity > c.resting_ask_liquidity ? 'BID' : 'ASK',
      }));
  }
}

// Singleton
export const store = new Store();
