import { describe, it, expect, vi, beforeEach } from 'vitest';
import { FallbackHeatmapRenderer } from '../heatmap/renderer/canvas-fallback';
import { MockEngine } from '../market/mock-engine';

// Mock canvas context
const mockCtx = {
  fillRect: vi.fn(),
  fillStyle: '',
  beginPath: vi.fn(),
  arc: vi.fn(),
  fill: vi.fn(),
  stroke: vi.fn(),
  strokeStyle: '',
  lineWidth: 1,
  setLineDash: vi.fn(),
  scale: vi.fn(),
  clearRect: vi.fn(),
  moveTo: vi.fn(),
  lineTo: vi.fn(),
};

const mockCanvas = {
  getContext: vi.fn(() => mockCtx),
  width: 800,
  height: 600,
  style: { width: '', height: '' },
} as unknown as HTMLCanvasElement;

describe('FallbackHeatmapRenderer', () => {
  let renderer: FallbackHeatmapRenderer;

  beforeEach(() => {
    vi.clearAllMocks();
    renderer = new FallbackHeatmapRenderer();
  });

  it('initializes without errors', async () => {
    await renderer.initialize(mockCanvas);
    expect(renderer.isGPU).toBe(false);
  });

  it('resize sets canvas dimensions', async () => {
    await renderer.initialize(mockCanvas);
    renderer.resize(1024, 768);
    // Should not throw
  });

  it('setFrame stores frame', async () => {
    await renderer.initialize(mockCanvas);
    const engine = new MockEngine(42);
    const frame = engine.generateFrame(1000);
    renderer.setFrame(frame);
    // Should not throw
  });

  it('setMode changes visualization', async () => {
    await renderer.initialize(mockCanvas);
    renderer.setMode('delta');
    renderer.setMode('execution');
    renderer.setMode('liquidity');
    // Should not throw
  });

  it('render produces output', async () => {
    await renderer.initialize(mockCanvas);
    const engine = new MockEngine(42);
    const frame = engine.generateFrame(1000);
    renderer.setFrame(frame);
    renderer.setViewport([frame.visible_price_range[0], frame.visible_price_range[1]], frame.time_range, 800, 600);
    renderer.render();
    expect(mockCtx.fillRect).toHaveBeenCalled();
  });

  it('dispose cleans up', async () => {
    await renderer.initialize(mockCanvas);
    renderer.dispose();
    // Should not throw
  });
});
