/**
 * Renderer interface — the contract between the heatmap view model
 * and the underlying rendering implementation.
 *
 * Two implementations: WebGPUHeatmapRenderer and FallbackHeatmapRenderer.
 * The React layer never touches GPU APIs directly.
 */

import type { HeatmapFrame, HeatmapMode, HeatmapCellSnapshot } from '../../types/heatmap';

export interface HeatmapRenderer {
  /** Initialize the renderer, attaching to the given canvas. */
  initialize(canvas: HTMLCanvasElement): Promise<void>;

  /** Resize the internal buffers/canvas to match CSS dimensions. */
  resize(width: number, height: number): void;

  /** Upload a full frame, replacing the current state. */
  setFrame(frame: HeatmapFrame): void;

  /** Apply an incremental delta to the current frame. */
  applyDelta(delta: {
    changed: [number, HeatmapCellSnapshot][];
    new: HeatmapCellSnapshot[];
    removed: number[];
  }): void;

  /**
   * Set the viewport.
   * priceRange: [lo, hi] in ticks
   * timeRange: [startMs, endMs]
   */
  setViewport(
    priceRange: [number, number],
    timeRange: [number, number],
    canvasWidth: number,
    canvasHeight: number
  ): void;

  /** Set the visualization mode. */
  setMode(mode: HeatmapMode): void;

  /** Render one frame. */
  render(): void;

  /** Release GPU/canvas resources. */
  dispose(): void;

  /** Whether this renderer uses GPU acceleration. */
  readonly isGPU: boolean;
}
