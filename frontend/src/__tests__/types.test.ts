import { describe, it, expect } from 'vitest';
import { ticksToPrice, priceToTicks, ticksToQuantity, TICK_SCALE } from '../types/heatmap';

describe('Type conversions', () => {
  it('ticksToPrice converts correctly', () => {
    expect(ticksToPrice(TICK_SCALE)).toBe(1);
    expect(ticksToPrice(TICK_SCALE * 77300)).toBe(77300);
    expect(ticksToPrice(0)).toBe(0);
  });

  it('priceToTicks converts correctly', () => {
    expect(priceToTicks(1)).toBe(TICK_SCALE);
    expect(priceToTicks(77300)).toBe(TICK_SCALE * 77300);
    expect(priceToTicks(0)).toBe(0);
  });

  it('ticksToQuantity converts correctly', () => {
    expect(ticksToQuantity(TICK_SCALE)).toBe(1);
    expect(ticksToQuantity(500_000_000)).toBe(5);
  });

  it('round-trip price conversion is exact', () => {
    const price = 77300.1;
    const ticks = priceToTicks(price);
    const result = ticksToPrice(ticks);
    expect(result).toBeCloseTo(price, 6);
  });
});
