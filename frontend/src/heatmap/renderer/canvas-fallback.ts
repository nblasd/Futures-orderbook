/**
 * Canvas 2D fallback renderer — batched heatmap rendering.
 *
 * This renderer is used when WebGPU is unavailable.
 * All cells are rendered via a single Canvas 2D context using
 * batched draw calls. NO DOM elements are created per cell.
 *
 * Color mapping:
 *   liquidity mode: blue intensity gradient
 *   execution mode: green intensity gradient
 *   delta mode: red/green diverging (red = sell, green = buy)
 *   absorption mode: orange glow
 *   sweeps mode: purple markers
 *   pressure mode: blue/red diverging
 */

import type { HeatmapFrame, HeatmapCellSnapshot, HeatmapMode } from '../../types/heatmap';
import type { HeatmapRenderer } from './interface';

// ---------- Color palettes per mode ----------

const PALETTE = {
  liquidity: {
    empty: [15, 15, 25] as [number, number, number],
    max: [0, 100, 255] as [number, number, number],
  },
  execution: {
    empty: [15, 15, 25] as [number, number, number],
    max: [0, 200, 80] as [number, number, number],
  },
  delta: {
    empty: [15, 15, 25] as [number, number, number],
    positive: [0, 180, 60] as [number, number, number], // buy
    negative: [220, 40, 40] as [number, number, number], // sell
  },
  absorption: {
    empty: [15, 15, 25] as [number, number, number],
    max: [255, 160, 0] as [number, number, number],
  },
  sweeps: {
    empty: [15, 15, 25] as [number, number, number],
    max: [160, 60, 220] as [number, number, number],
  },
  pressure: {
    empty: [15, 15, 25] as [number, number, number],
    positive: [0, 150, 255] as [number, number, number],
    negative: [255, 80, 80] as [number, number, number],
  },
} as const;

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

function lerpColor(
  from: [number, number, number],
  to: [number, number, number],
  t: number
): string {
  const r = Math.round(lerp(from[0], to[0], t));
  const g = Math.round(lerp(from[1], to[1], t));
  const b = Math.round(lerp(from[2], to[2], t));
  return `rgb(${r},${g},${b})`;
}

type ColorTriple = [number, number, number];

function intensityColor(
  mode: HeatmapMode,
  cell: HeatmapCellSnapshot,
  maxVal: number
): string {
  const empty: ColorTriple = [15, 15, 25];

  switch (mode) {
    case 'liquidity': {
      const p = PALETTE.liquidity;
      const val = cell.resting_bid_liquidity + cell.resting_ask_liquidity;
      const t = Math.min(1, val / maxVal);
      return lerpColor(empty, p.max, t);
    }
    case 'execution': {
      const p = PALETTE.execution;
      const val = cell.executed_buy_volume + cell.executed_sell_volume;
      const t = Math.min(1, val / maxVal);
      return lerpColor(empty, p.max, t);
    }
    case 'delta': {
      const p = PALETTE.delta;
      const t = Math.min(1, Math.abs(cell.delta) / maxVal);
      return cell.delta >= 0
        ? lerpColor(empty, p.positive, t)
        : lerpColor(empty, p.negative, t);
    }
    case 'absorption': {
      const p = PALETTE.absorption;
      const t = Math.min(1, cell.absorption_candidate_count / Math.max(1, maxVal));
      return lerpColor(empty, p.max, t);
    }
    case 'sweeps': {
      const p = PALETTE.sweeps;
      const t = Math.min(1, cell.sweep_count / Math.max(1, maxVal));
      return lerpColor(empty, p.max, t);
    }
    case 'pressure': {
      const p = PALETTE.pressure;
      const t = Math.min(1, Math.abs(cell.pressure) / maxVal);
      return cell.pressure >= 0
        ? lerpColor(empty, p.positive, t)
        : lerpColor(empty, p.negative, t);
    }
  }
}

export class FallbackHeatmapRenderer implements HeatmapRenderer {
  readonly isGPU = false;

  private canvas: HTMLCanvasElement | null = null;
  private ctx: CanvasRenderingContext2D | null = null;
  private frame: HeatmapFrame | null = null;
  private mode: HeatmapMode = 'liquidity';
  private viewportPriceRange: [number, number] = [0, 1];
  private viewportTimeRange: [number, number] = [0, 1];
  private canvasWidth = 0;
  private canvasHeight = 0;

  async initialize(canvas: HTMLCanvasElement): Promise<void> {
    this.canvas = canvas;
    const ctx = canvas.getContext('2d');
    if (!ctx) throw new Error('Failed to get 2d context');
    this.ctx = ctx;
  }

  resize(width: number, height: number): void {
    this.canvasWidth = width;
    this.canvasHeight = height;
    if (this.canvas) {
      const dpr = window.devicePixelRatio || 1;
      this.canvas.width = width * dpr;
      this.canvas.height = height * dpr;
      this.canvas.style.width = `${width}px`;
      this.canvas.style.height = `${height}px`;
      this.ctx?.scale(dpr, dpr);
    }
  }

  setFrame(frame: HeatmapFrame): void {
    this.frame = frame;
  }

  applyDelta(delta: {
    changed: [number, HeatmapCellSnapshot][];
    new: HeatmapCellSnapshot[];
    removed: number[];
  }): void {
    if (!this.frame) return;
    // Apply to the in-memory frame
    const cellMap = new Map(this.frame.cells.map((c) => [c.price_tick, c]));
    for (const [price, cell] of delta.changed) {
      cellMap.set(price, cell);
    }
    for (const cell of delta.new) {
      cellMap.set(cell.price_tick, cell);
    }
    for (const price of delta.removed) {
      cellMap.delete(price);
    }
    this.frame = {
      ...this.frame,
      cells: Array.from(cellMap.values()).sort((a, b) => a.price_tick - b.price_tick),
    };
  }

  setViewport(
    priceRange: [number, number],
    timeRange: [number, number],
    canvasWidth: number,
    canvasHeight: number
  ): void {
    this.viewportPriceRange = priceRange;
    this.viewportTimeRange = timeRange;
    this.canvasWidth = canvasWidth;
    this.canvasHeight = canvasHeight;
  }

  setMode(mode: HeatmapMode): void {
    this.mode = mode;
  }

  render(): void {
    if (!this.ctx || !this.frame || !this.canvas) return;

    const ctx = this.ctx;
    const { canvasWidth: w, canvasHeight: h } = this;
    const cells = this.frame.cells;
    if (cells.length === 0 || w === 0 || h === 0) return;

    // Clear
    ctx.fillStyle = '#0f0f19';
    ctx.fillRect(0, 0, w, h);

    const [priceLo, priceHi] = this.viewportPriceRange;
    const priceRange = priceHi - priceLo;
    if (priceRange <= 0) return;

    // Compute max intensity value for normalization
    let maxVal = 1;
    for (const cell of cells) {
      switch (this.mode) {
        case 'liquidity':
          maxVal = Math.max(
            maxVal,
            cell.resting_bid_liquidity + cell.resting_ask_liquidity
          );
          break;
        case 'execution':
          maxVal = Math.max(
            maxVal,
            cell.executed_buy_volume + cell.executed_sell_volume
          );
          break;
        case 'delta':
          maxVal = Math.max(maxVal, Math.abs(cell.delta));
          break;
        case 'absorption':
          maxVal = Math.max(maxVal, cell.absorption_candidate_count);
          break;
        case 'sweeps':
          maxVal = Math.max(maxVal, cell.sweep_count);
          break;
        case 'pressure':
          maxVal = Math.max(maxVal, Math.abs(cell.pressure));
          break;
      }
    }

    // Render each visible cell as a filled rectangle
    const cellHeight = Math.max(2, h / Math.max(1, cells.length));
    const timeWidth = w; // each cell spans full visible time width for the bucket

    for (const cell of cells) {
      if (cell.price_tick < priceLo || cell.price_tick > priceHi) continue;

      // Map price to Y (top = high price)
      const normalizedPrice = (cell.price_tick - priceLo) / priceRange;
      const y = h - normalizedPrice * h - cellHeight;

      ctx.fillStyle = intensityColor(this.mode, cell, maxVal);
      ctx.fillRect(0, Math.max(0, y), timeWidth, Math.ceil(cellHeight) + 1);

      // Draw trade markers
      if (cell.trade_count > 0) {
        const midY = y + cellHeight / 2;
        const isBuy = cell.executed_buy_volume > cell.executed_sell_volume;
        const markerSize = Math.min(cellHeight * 0.6, 8);
        ctx.beginPath();
        ctx.arc(timeWidth / 2, midY, markerSize, 0, Math.PI * 2);
        ctx.fillStyle = isBuy ? 'rgba(0,255,100,0.7)' : 'rgba(255,60,60,0.7)';
        ctx.fill();
      }

      // Draw absorption markers
      if (cell.absorption_candidate_count > 0) {
        const midY = y + cellHeight / 2;
        ctx.beginPath();
        ctx.arc(timeWidth / 2, midY, Math.min(cellHeight * 0.8, 10), 0, Math.PI * 2);
        ctx.strokeStyle = '#ffa500';
        ctx.lineWidth = 2;
        ctx.stroke();
      }

      // Draw sweep markers
      if (cell.sweep_count > 0) {
        const midY = y + cellHeight / 2;
        const size = Math.min(cellHeight * 0.7, 9);
        ctx.beginPath();
        ctx.moveTo(timeWidth / 2, midY - size);
        ctx.lineTo(timeWidth / 2 + size, midY + size);
        ctx.lineTo(timeWidth / 2 - size, midY + size);
        ctx.closePath();
        ctx.strokeStyle = '#a03cdc';
        ctx.lineWidth = 2;
        ctx.stroke();
      }

      // Draw replenishment markers
      if (cell.replenishment_count > 0) {
        const midY = y + cellHeight / 2;
        ctx.fillStyle = 'rgba(255,200,0,0.5)';
        ctx.fillRect(timeWidth / 2 + 5, midY - 2, 6, 4);
      }
    }

    // Draw current price line
    const bestBid = this.frame.visible_price_range[0];
    const bestAsk = this.frame.visible_price_range[1];
    const midPrice = (bestBid + bestAsk) / 2;
    if (midPrice >= priceLo && midPrice <= priceHi) {
      const priceY = h - ((midPrice - priceLo) / priceRange) * h;
      ctx.strokeStyle = '#ffff00';
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(0, priceY);
      ctx.lineTo(w, priceY);
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  dispose(): void {
    this.canvas = null;
    this.ctx = null;
    this.frame = null;
  }
}
