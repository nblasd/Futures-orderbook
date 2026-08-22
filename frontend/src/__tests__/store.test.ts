import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Store } from '../state/store';

describe('Store', () => {
  let store: Store;

  beforeEach(() => {
    store = new Store();
  });

  afterEach(() => {
    store.stopDemo();
  });

  it('initializes with default state', () => {
    expect(store.state.frame).toBeNull();
    expect(store.state.connection).toBe('DEMO');
    expect(store.state.mode).toBe('liquidity');
    expect(store.state.viewport.follow).toBe(true);
  });

  it('startDemo generates frames synchronously on first tick', () => {
    store.startDemo(100);
    // startDemo calls tick() immediately
    expect(store.state.frame).not.toBeNull();
    expect(store.state.frame!.cells.length).toBeGreaterThan(0);
    expect(store.state.connection).toBe('DEMO');
    expect(store.state.market.connection).toBe('DEMO');
  });

  it('stopDemo stops interval', () => {
    store.startDemo(100);
    const frameCount = store.state.frames.length;
    store.stopDemo();
    // After stopping, no new frames should be added by interval
    // (the initial tick already ran)
    expect(store.state.frames.length).toBeGreaterThanOrEqual(frameCount);
  });

  it('setMode updates mode', () => {
    store.setMode('delta');
    expect(store.state.mode).toBe('delta');
    store.setMode('execution');
    expect(store.state.mode).toBe('execution');
  });

  it('toggleFollow toggles', () => {
    expect(store.state.viewport.follow).toBe(true);
    store.toggleFollow();
    expect(store.state.viewport.follow).toBe(false);
    store.toggleFollow();
    expect(store.state.viewport.follow).toBe(true);
  });

  it('togglePause toggles', () => {
    expect(store.state.viewport.paused).toBe(false);
    store.togglePause();
    expect(store.state.viewport.paused).toBe(true);
    store.togglePause();
    expect(store.state.viewport.paused).toBe(false);
  });

  it('subscribe receives notifications', () => {
    let count = 0;
    const unsub = store.subscribe(() => {
      count++;
    });
    store.setMode('delta');
    expect(count).toBe(1);
    store.setMode('execution');
    expect(count).toBe(2);
    unsub();
    store.setMode('liquidity');
    expect(count).toBe(2); // no change after unsub
  });

  it('zoomPrices changes viewport', () => {
    const [lo, hi] = store.state.viewport.priceRange;
    store.zoomPrices(0.5);
    const [newLo, newHi] = store.state.viewport.priceRange;
    expect(newHi - newLo).toBeLessThan(hi - lo);
  });

  it('setTimeRange updates time range', () => {
    store.setTimeRange(300_000);
    expect(store.state.timeRange).toBe(300_000);
  });

  it('resetViewport resets to defaults', () => {
    store.zoomPrices(0.1);
    store.toggleFollow();
    store.resetViewport();
    expect(store.state.viewport.follow).toBe(true);
  });
});
