import { describe, it, expect } from 'vitest';
import { MockEngine } from '../market/mock-engine';

describe('MockEngine', () => {
  it('generates a valid HeatmapFrame', () => {
    const engine = new MockEngine(42);
    const frame = engine.generateFrame(Date.now());

    expect(frame.timestamp).toBeGreaterThan(0);
    expect(frame.cells.length).toBeGreaterThan(0);
    expect(frame.summary.total_price_levels).toBe(frame.cells.length);
    expect(frame.time_range[0]).toBeLessThan(frame.time_range[1]);
    expect(frame.visible_price_range[0]).toBeLessThanOrEqual(frame.visible_price_range[1]);
  });

  it('generates deterministic frames with same seed', () => {
    const engine1 = new MockEngine(42);
    const engine2 = new MockEngine(42);
    const ts = 1000000;

    const frame1 = engine1.generateFrame(ts);
    const frame2 = engine2.generateFrame(ts);

    expect(frame1.cells.length).toBe(frame2.cells.length);
    expect(frame1.summary.total_trade_count).toBe(frame2.summary.total_trade_count);
    expect(frame1.summary.total_delta).toBe(frame2.summary.total_delta);
  });

  it('generates different frames with different seeds', () => {
    const engine1 = new MockEngine(42);
    const engine2 = new MockEngine(99);
    const ts = 1000000;

    const frame1 = engine1.generateFrame(ts);
    const frame2 = engine2.generateFrame(ts);

    // At least the cell counts should differ with different seeds
    // (not guaranteed but extremely likely)
    expect(frame1.summary.total_executed_buy !== frame2.summary.total_executed_buy ||
      frame1.summary.total_executed_sell !== frame2.summary.total_executed_sell).toBe(true);
  });

  it('generates a valid delta', () => {
    const engine = new MockEngine(42);
    const frame1 = engine.generateFrame(1000);
    const frame2 = engine.generateFrame(2000);

    const delta = engine.generateDelta(frame1, frame2);
    expect(delta.changed.length + delta.new.length).toBeGreaterThan(0);
    expect(typeof delta.summary_delta.total_delta).toBe('number');
  });

  it('generates trades from frame', () => {
    const engine = new MockEngine(42);
    const frame = engine.generateFrame(1000);
    const trades = engine.generateTrades(frame);

    expect(trades.length).toBeGreaterThanOrEqual(0);
    for (const t of trades) {
      expect(['BUY', 'SELL']).toContain(t.aggressor);
      expect(t.quantity_ticks).toBeGreaterThanOrEqual(0);
    }
  });

  it('generates stress test frame', () => {
    const frame = MockEngine.generateStressFrame(100_000, 1000);
    expect(frame.cells.length).toBe(100_000);
    expect(frame.summary.total_price_levels).toBe(100_000);
  });

  it('generates market state', () => {
    const engine = new MockEngine(42);
    engine.generateFrame(1000);
    const state = engine.getMarketState();

    expect(state.connection).toBe('DEMO');
    expect(state.bookStatus).toBe('READY');
    expect(state.symbol).toBe('BTCUSDT');
    expect(state.bestBid).toBeGreaterThan(0);
    expect(state.bestAsk).toBeGreaterThan(state.bestBid);
  });
});
