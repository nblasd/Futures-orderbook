/**
 * Deterministic mock data engine for Phase 6 frontend development.
 *
 * Generates realistic HeatmapFrame data with a seeded PRNG so visual
 * bugs can be reproduced. Clearly labeled as DEMO DATA.
 *
 * Units match the Rust backend exactly:
 *   - prices: integer ticks (1e8 scale, tick_size = 0.10 USDT)
 *   - quantities: integer ticks (1e8 scale, step_size = 0.001 BTC)
 *   - timestamps: milliseconds since epoch
 *   - delta: buy_volume - sell_volume (aggressor sign convention)
 */

import {
  HeatmapFrame,
  HeatmapCellSnapshot,
  HeatmapSummary,
  HeatmapSummaryDelta,
  HeatmapDelta,
  TradeOverlay,
  AbsorptionMarker,
  SweepMarker,
  ReplenishmentMarker,
  MarketState,
  TICK_SCALE,
} from '../types/heatmap';

// ---------- Seeded PRNG (mulberry32) ----------

function mulberry32(seed: number): () => number {
  let s = seed | 0;
  return () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---------- Constants ----------

const BTCUSDT_TICK_SIZE = 10_000_000; // 0.10 USDT in 1e8
const BTCUSDT_STEP_TICKS = 100_000_000; // 0.001 BTC in 1e8
const BASE_PRICE_TICKS = 77_300 * BTCUSDT_TICK_SIZE; // ~77300.00

// ---------- MockEngine ----------

export class MockEngine {
  private rng: () => number;
  private startTime: number;
  private currentPrice: number;
  private tradeIdCounter: number;
  private cumDelta: number;
  private cumCvd: number;
  private totalBuyVol: number;
  private totalSellVol: number;
  private tradeCount: number;
  private eventsPerSec: number;
  private tradesPerSec: number;
  private frameCount: number;

  constructor(seed: number = 42) {
    this.rng = mulberry32(seed);
    this.startTime = Date.now();
    this.currentPrice = BASE_PRICE_TICKS;
    this.tradeIdCounter = 1;
    this.cumDelta = 0;
    this.cumCvd = 0;
    this.totalBuyVol = 0;
    this.totalSellVol = 0;
    this.tradeCount = 0;
    this.eventsPerSec = 0;
    this.tradesPerSec = 0;
    this.frameCount = 0;
  }

  /** Generate a single mock HeatmapFrame at the given timestamp. */
  generateFrame(timestampMs: number, bucketMs: number = 1000): HeatmapFrame {
    this.frameCount++;
    const bucketStart = timestampMs - (timestampMs % bucketMs);

    // Simulate price movement: random walk around base price
    const drift = (this.rng() - 0.48) * BTCUSDT_TICK_SIZE * 3;
    const volatility = (this.rng() - 0.5) * BTCUSDT_TICK_SIZE * 8;
    this.currentPrice = Math.round(this.currentPrice + drift + volatility);
    // Keep price within a realistic range
    const minPrice = BASE_PRICE_TICKS - 200 * BTCUSDT_TICK_SIZE;
    const maxPrice = BASE_PRICE_TICKS + 200 * BTCUSDT_TICK_SIZE;
    this.currentPrice = Math.max(minPrice, Math.min(maxPrice, this.currentPrice));

    // Generate cells around the current price
    const cells: HeatmapCellSnapshot[] = [];
    const numLevels = 50 + Math.floor(this.rng() * 30);
    const halfRange = Math.floor(numLevels / 2);
    let totalBuy = 0;
    let totalSell = 0;
    let totalDelta = 0;
    let totalTrades = 0;
    let totalLiqAdded = 0;
    let totalLiqRemoved = 0;
    let totalLargeVol = 0;
    let totalReplenish = 0;
    let totalAbsorb = 0;
    let totalSweeps = 0;

    for (let i = -halfRange; i <= halfRange; i++) {
      const price = this.currentPrice + i * BTCUSDT_TICK_SIZE;
      if (price < minPrice || price > maxPrice) continue;

      // Liquidity: higher near current price, with noise
      const distFromMid = Math.abs(i);
      const liquidityFactor = Math.max(0, 1 - distFromMid / halfRange);
      const bidLiq = Math.round(
        (500 + this.rng() * 2000) * liquidityFactor * BTCUSDT_STEP_TICKS
      );
      const askLiq = Math.round(
        (500 + this.rng() * 2000) * liquidityFactor * BTCUSDT_STEP_TICKS
      );

      // Trading activity: concentrated near the current price
      const activityFactor = Math.max(0, 1 - distFromMid / 15);
      const buyVol = Math.round(
        this.rng() * 50 * activityFactor * BTCUSDT_STEP_TICKS
      );
      const sellVol = Math.round(
        this.rng() * 50 * activityFactor * BTCUSDT_STEP_TICKS
      );
      const delta = buyVol - sellVol;
      const trades = activityFactor > 0.3 ? Math.floor(this.rng() * 10) : 0;

      const liqAdded = Math.round(this.rng() * 5 * BTCUSDT_STEP_TICKS * liquidityFactor);
      const liqRemoved = Math.round(this.rng() * 4 * BTCUSDT_STEP_TICKS * liquidityFactor);
      const largeVol = distFromMid < 3 && this.rng() > 0.92
        ? Math.round((100 + this.rng() * 500) * BTCUSDT_STEP_TICKS)
        : 0;
      const replenish = activityFactor > 0.5 ? Math.floor(this.rng() * 3) : 0;
      const absorb = distFromMid < 2 && this.rng() > 0.95 ? 1 : 0;
      const sweep = distFromMid < 5 && this.rng() > 0.97 ? 1 : 0;

      totalBuy += buyVol;
      totalSell += sellVol;
      totalDelta += delta;
      totalTrades += trades;
      totalLiqAdded += liqAdded;
      totalLiqRemoved += liqRemoved;
      totalLargeVol += largeVol;
      totalReplenish += replenish;
      totalAbsorb += absorb;
      totalSweeps += sweep;

      cells.push({
        price_tick: price,
        resting_bid_liquidity: bidLiq,
        resting_ask_liquidity: askLiq,
        liquidity_added: liqAdded,
        liquidity_removed: liqRemoved,
        executed_buy_volume: buyVol,
        executed_sell_volume: sellVol,
        delta,
        trade_count: trades,
        large_trade_volume: largeVol,
        replenishment_count: replenish,
        absorption_candidate_count: absorb,
        sweep_count: sweep,
        pressure: delta,
      });

      // Accumulate session totals
      this.totalBuyVol += buyVol;
      this.totalSellVol += sellVol;
      this.cumDelta += delta;
      this.cumCvd += delta;
      this.tradeCount += trades;
    }

    const lo = cells.length > 0 ? cells[0].price_tick : this.currentPrice;
    const hi = cells.length > 0 ? cells[cells.length - 1].price_tick : this.currentPrice;

    const summary: HeatmapSummary = {
      total_price_levels: cells.length,
      total_buckets: 1,
      total_executed_buy: totalBuy,
      total_executed_sell: totalSell,
      total_delta: totalDelta,
      total_trade_count: totalTrades,
      total_liquidity_added: totalLiqAdded,
      total_liquidity_removed: totalLiqRemoved,
      total_large_trade_volume: totalLargeVol,
      total_replenishment_count: totalReplenish,
      total_absorption_candidate_count: totalAbsorb,
      total_sweep_count: totalSweeps,
    };

    this.eventsPerSec = 80 + Math.floor(this.rng() * 40);
    this.tradesPerSec = 5 + Math.floor(this.rng() * 15);

    return {
      timestamp: timestampMs,
      visible_price_range: [lo, hi],
      time_range: [bucketStart, bucketStart + bucketMs],
      cells,
      summary,
    };
  }

  /** Generate a delta from two consecutive frames. */
  generateDelta(
    previous: HeatmapFrame,
    current: HeatmapFrame
  ): HeatmapDelta {
    const prevMap = new Map(previous.cells.map((c) => [c.price_tick, c]));
    const currMap = new Map(current.cells.map((c) => [c.price_tick, c]));

    const changed: [number, HeatmapCellSnapshot][] = [];
    const newCells: HeatmapCellSnapshot[] = [];
    const removed: number[] = [];

    for (const [price, curr] of currMap) {
      const prev = prevMap.get(price);
      if (prev) {
        if (JSON.stringify(prev) !== JSON.stringify(curr)) {
          changed.push([price, curr]);
        }
      } else {
        newCells.push(curr);
      }
    }

    for (const price of prevMap.keys()) {
      if (!currMap.has(price)) {
        removed.push(price);
      }
    }

    const ps = previous.summary;
    const cs = current.summary;
    const summary_delta: HeatmapSummaryDelta = {
      total_executed_buy: cs.total_executed_buy - ps.total_executed_buy,
      total_executed_sell: cs.total_executed_sell - ps.total_executed_sell,
      total_delta: cs.total_delta - ps.total_delta,
      total_trade_count: cs.total_trade_count - ps.total_trade_count,
      total_liquidity_added: cs.total_liquidity_added - ps.total_liquidity_added,
      total_liquidity_removed: cs.total_liquidity_removed - ps.total_liquidity_removed,
      total_large_trade_volume: cs.total_large_trade_volume - ps.total_large_trade_volume,
      total_replenishment_count: cs.total_replenishment_count - ps.total_replenishment_count,
      total_absorption_candidate_count:
        cs.total_absorption_candidate_count - ps.total_absorption_candidate_count,
      total_sweep_count: cs.total_sweep_count - ps.total_sweep_count,
    };

    return { changed, new: newCells, removed, summary_delta };
  }

  /** Generate trade overlays from the current frame. */
  generateTrades(frame: HeatmapFrame): TradeOverlay[] {
    const trades: TradeOverlay[] = [];
    for (const cell of frame.cells) {
      if (cell.trade_count > 0) {
        for (let i = 0; i < Math.min(cell.trade_count, 3); i++) {
          const isBuy = cell.executed_buy_volume > cell.executed_sell_volume;
          trades.push({
            price_tick: cell.price_tick,
            timestamp_ms: frame.timestamp,
            quantity_ticks: Math.round(
              (cell.executed_buy_volume + cell.executed_sell_volume) /
                Math.max(1, cell.trade_count)
            ),
            aggressor: isBuy ? 'BUY' : 'SELL',
            is_large: cell.large_trade_volume > 0,
          });
        }
      }
    }
    return trades;
  }

  /** Generate absorption markers from the current frame. */
  generateAbsorptionMarkers(frame: HeatmapFrame): AbsorptionMarker[] {
    return frame.cells
      .filter((c) => c.absorption_candidate_count > 0)
      .map((c) => ({
        price_tick: c.price_tick,
        timestamp_ms: frame.timestamp,
        volume: c.executed_buy_volume + c.executed_sell_volume,
        direction: c.delta > 0 ? 'BUY' : 'SELL',
        confidence: 0.5 + this.rng() * 0.4,
      }));
  }

  /** Generate sweep markers from the current frame. */
  generateSweepMarkers(frame: HeatmapFrame): SweepMarker[] {
    return frame.cells
      .filter((c) => c.sweep_count > 0)
      .map((c) => ({
        price_tick: c.price_tick,
        timestamp_ms: frame.timestamp,
        volume: c.executed_buy_volume + c.executed_sell_volume,
        direction: c.delta > 0 ? 'BUY' : 'SELL',
        price_range: [c.price_tick - 5 * BTCUSDT_TICK_SIZE, c.price_tick + 5 * BTCUSDT_TICK_SIZE] as [number, number],
        confidence: 0.5 + this.rng() * 0.4,
      }));
  }

  /** Generate replenishment markers from the current frame. */
  generateReplenishmentMarkers(frame: HeatmapFrame): ReplenishmentMarker[] {
    return frame.cells
      .filter((c) => c.replenishment_count > 0)
      .map((c) => ({
        price_tick: c.price_tick,
        timestamp_ms: frame.timestamp,
        quantity: c.resting_bid_liquidity + c.resting_ask_liquidity,
        side: c.resting_bid_liquidity > c.resting_ask_liquidity ? 'BID' : 'ASK',
      }));
  }

  /** Get current market state. */
  getMarketState(): MarketState {
    return {
      connection: 'DEMO',
      bookStatus: 'READY',
      symbol: 'BTCUSDT',
      exchange: 'Binance USDⓈ-M Futures',
      bestBid: this.currentPrice / TICK_SCALE,
      bestAsk: (this.currentPrice + BTCUSDT_TICK_SIZE) / TICK_SCALE,
      mid: (this.currentPrice + BTCUSDT_TICK_SIZE / 2) / TICK_SCALE,
      spread: BTCUSDT_TICK_SIZE / TICK_SCALE,
      lastTradePrice: this.currentPrice / TICK_SCALE,
      eventsPerSec: this.eventsPerSec,
      tradesPerSec: this.tradesPerSec,
      heatmapCells: this.frameCount * 50,
      sequenceErrors: 0,
      queueDepth: 0,
    };
  }

  /** Create a deterministic stress-test frame with many cells. */
  static generateStressFrame(
    cellCount: number,
    timestampMs: number
  ): HeatmapFrame {
    const rng = mulberry32(12345);
    const cells: HeatmapCellSnapshot[] = [];
    const startPrice = BASE_PRICE_TICKS - Math.floor(cellCount / 2) * BTCUSDT_TICK_SIZE;

    for (let i = 0; i < cellCount; i++) {
      const price = startPrice + i * BTCUSDT_TICK_SIZE;
      cells.push({
        price_tick: price,
        resting_bid_liquidity: Math.round(rng() * 1000 * BTCUSDT_STEP_TICKS),
        resting_ask_liquidity: Math.round(rng() * 1000 * BTCUSDT_STEP_TICKS),
        liquidity_added: Math.round(rng() * 50 * BTCUSDT_STEP_TICKS),
        liquidity_removed: Math.round(rng() * 40 * BTCUSDT_STEP_TICKS),
        executed_buy_volume: Math.round(rng() * 100 * BTCUSDT_STEP_TICKS),
        executed_sell_volume: Math.round(rng() * 100 * BTCUSDT_STEP_TICKS),
        delta: Math.round((rng() - 0.5) * 200 * BTCUSDT_STEP_TICKS),
        trade_count: Math.floor(rng() * 20),
        large_trade_volume: rng() > 0.95 ? Math.round(rng() * 500 * BTCUSDT_STEP_TICKS) : 0,
        replenishment_count: Math.floor(rng() * 5),
        absorption_candidate_count: rng() > 0.98 ? 1 : 0,
        sweep_count: rng() > 0.99 ? 1 : 0,
        pressure: Math.round((rng() - 0.5) * 100 * BTCUSDT_STEP_TICKS),
      });
    }

    const lo = cells[0]?.price_tick ?? BASE_PRICE_TICKS;
    const hi = cells[cells.length - 1]?.price_tick ?? BASE_PRICE_TICKS;

    return {
      timestamp: timestampMs,
      visible_price_range: [lo, hi],
      time_range: [timestampMs - 1000, timestampMs],
      cells,
      summary: {
        total_price_levels: cells.length,
        total_buckets: 1,
        total_executed_buy: cells.reduce((s, c) => s + c.executed_buy_volume, 0),
        total_executed_sell: cells.reduce((s, c) => s + c.executed_sell_volume, 0),
        total_delta: cells.reduce((s, c) => s + c.delta, 0),
        total_trade_count: cells.reduce((s, c) => s + c.trade_count, 0),
        total_liquidity_added: cells.reduce((s, c) => s + c.liquidity_added, 0),
        total_liquidity_removed: cells.reduce((s, c) => s + c.liquidity_removed, 0),
        total_large_trade_volume: cells.reduce((s, c) => s + c.large_trade_volume, 0),
        total_replenishment_count: cells.reduce((s, c) => s + c.replenishment_count, 0),
        total_absorption_candidate_count: cells.reduce(
          (s, c) => s + c.absorption_candidate_count,
          0
        ),
        total_sweep_count: cells.reduce((s, c) => s + c.sweep_count, 0),
      },
    };
  }
}
